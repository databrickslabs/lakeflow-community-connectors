"""Offline tests for value encoding, options and authentication (no MongoDB needed)."""

import json
import pickle
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from bson import Binary, Decimal128, Int64, ObjectId
from bson.timestamp import Timestamp

from databricks.labs.community_connector.sources.mongodb import mongodb_auth
from databricks.labs.community_connector.sources.mongodb.mongodb_auth import (
    AwsKeys,
    MongoClientFactory,
    UcServiceCredentialProvider,
)
from databricks.labs.community_connector.sources.mongodb.mongodb_schemas import table_schema
from databricks.labs.community_connector.sources.mongodb.mongodb_utils import (
    decode_id,
    dumps,
    encode_id,
    timestamp_to_datetime,
)

MULTIBYTE = "日本語テキスト・ｶﾀｶﾅ・한국어・中文・👨‍👩‍👧‍👦・🍣・é (e\u0301)"


class TestEncodeId:
    def test_objectid_is_hex(self):
        oid = ObjectId()
        assert encode_id(oid) == str(oid)
        assert decode_id(encode_id(oid)) == oid

    @pytest.mark.parametrize(
        "value",
        ["abc", MULTIBYTE, "", 1, Int64(1), 1.5, Decimal128("1.10"), True, None,
         {"a": 1, "b": "日本"}, [1, "二"], Binary(b"\x00\xff", 4),
         datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=timezone.utc)],
    )
    def test_round_trip(self, value):
        assert decode_id(encode_id(value)) == value

    def test_no_collisions_between_types(self):
        # MongoDB treats numerically equal _id values (1, Int64(1), 1.0) as the same
        # key, so they never coexist; strings and ObjectIds with the same text can.
        oid = ObjectId("65a1b2c3d4e5f60718293a4b")
        values = [oid, str(oid), 1, "1", {"a": 1}, '{"a":1}', [1], "[1]"]
        encoded = [encode_id(v) for v in values]
        assert len(set(encoded)) == len(values), encoded

    def test_multibyte_id_is_literal_utf8(self):
        encoded = encode_id(MULTIBYTE)
        assert MULTIBYTE in encoded  # not \\u escaped
        assert "\\u" not in encoded


class TestDumps:
    def test_multibyte_text_and_field_names_kept(self):
        doc = {"名前": MULTIBYTE, "絵文字🍣": ["🍣", "寿司"], "ネスト": {"キー": "値"}}
        text = dumps(doc)
        assert "\\u" not in text
        assert json.loads(text) == doc

    def test_bson_types_are_extended_json(self):
        doc = {"d": datetime(2024, 1, 1, tzinfo=timezone.utc), "n": Int64(2**40),
               "dec": Decimal128(Decimal("12.345")), "oid": ObjectId("65a1b2c3d4e5f60718293a4b")}
        parsed = json.loads(dumps(doc))
        assert parsed["d"] == {"$date": "2024-01-01T00:00:00Z"}
        assert parsed["n"] == 2**40
        assert parsed["dec"] == {"$numberDecimal": "12.345"}
        assert parsed["oid"] == {"$oid": "65a1b2c3d4e5f60718293a4b"}

    def test_variant_parse_keeps_multibyte(self):
        from pyspark.sql.types import VariantVal  # pylint: disable=import-outside-toplevel

        doc = {"名前": MULTIBYTE}
        assert VariantVal.parseJson(dumps(doc)).toPython() == doc


def test_timestamp_to_datetime():
    assert timestamp_to_datetime(Timestamp(1700000000, 3)) == datetime.fromtimestamp(
        1700000000, tz=timezone.utc
    )
    assert timestamp_to_datetime(None) is None


def test_schema_document_format():
    assert table_schema("variant")["document"].dataType.typeName() == "variant"
    assert table_schema("string")["document"].dataType.typeName() == "string"
    assert table_schema()["_id"].nullable is False


class _Resp:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class _Session:
    def __init__(self, expires_in_s=3000):
        self.calls = []
        self.expires_in_s = expires_in_s

    def post(self, url, **kwargs):
        self.calls.append(url)
        if url.endswith("/oidc/v1/token"):
            return _Resp(200, {"access_token": "tok", "expires_in": 3600})
        return _Resp(200, {
            "aws_temp_credentials": {"access_key_id": f"ASIA{len(self.calls)}",
                                     "secret_access_key": "s", "session_token": "t,with=chars"},
            "expiration_time": int((time.time() + self.expires_in_s) * 1000),
        })


class TestAwsServiceCredential:
    OPTIONS = {
        "connection_uri": "mongodb+srv://cluster0.example.mongodb.net/",
        "auth_mode": "aws_service_credential",
        "databricks_host": "dbc-1.cloud.databricks.com",
        "databricks_client_id": "app-id",
        "databricks_client_secret": "secret",
        "aws_service_credential": "atlas_role",
    }

    def test_missing_options_fail_early(self):
        with pytest.raises(ValueError, match="databricks_client_id"):
            MongoClientFactory({**self.OPTIONS, "databricks_client_id": ""})
        with pytest.raises(ValueError, match="aws_service_credential"):
            MongoClientFactory({**self.OPTIONS, "aws_service_credential": ""})

    def test_kwargs_use_temporary_keys(self, monkeypatch):
        session = _Session()
        monkeypatch.setattr(mongodb_auth.requests, "Session", lambda: session)
        kwargs = MongoClientFactory(dict(self.OPTIONS)).build_kwargs()
        assert kwargs["authMechanism"] == "MONGODB-AWS"
        assert kwargs["authSource"] == "$external"
        assert kwargs["username"].startswith("ASIA")
        # The session token goes as a dict so commas or '=' cannot break parsing.
        assert kwargs["authMechanismProperties"] == {"AWS_SESSION_TOKEN": "t,with=chars"}
        assert session.calls[0] == "https://dbc-1.cloud.databricks.com/oidc/v1/token"
        assert session.calls[1].endswith("/api/2.1/unity-catalog/temporary-service-credentials")

    def test_refresh_is_based_on_reported_expiry(self, monkeypatch):
        # Keys that expire in 5 minutes are inside the default 10 minute margin.
        session = _Session(expires_in_s=300)
        monkeypatch.setattr(mongodb_auth.requests, "Session", lambda: session)
        factory = MongoClientFactory(dict(self.OPTIONS))
        first = factory.build_kwargs()["username"]
        assert factory.credentials_expire_soon()
        second = factory.build_kwargs()["username"]
        assert first != second

    def test_entitlement_hint(self, monkeypatch):
        class Denied(_Session):
            def post(self, url, **kwargs):
                if url.endswith("/oidc/v1/token"):
                    return _Resp(200, {"access_token": "tok", "expires_in": 3600})
                return _Resp(403, {"message": "This API is disabled for users without the "
                                              "workspace-access entitlements"})

        monkeypatch.setattr(mongodb_auth.requests, "Session", Denied)
        with pytest.raises(RuntimeError, match="workspace-access"):
            MongoClientFactory(dict(self.OPTIONS)).build_kwargs()

    def test_factory_pickles_without_keys(self, monkeypatch):
        session = _Session()
        monkeypatch.setattr(mongodb_auth.requests, "Session", lambda: session)
        factory = MongoClientFactory(dict(self.OPTIONS))
        factory.build_kwargs()
        clone = pickle.loads(pickle.dumps(factory))
        assert clone._keys is None  # pylint: disable=protected-access
        assert clone.credentials_expire_soon()


def test_aws_keys_margin():
    keys = AwsKeys("a", "b", "c", expires_at=1000)
    assert not keys.needs_refresh(100, now=800)
    assert keys.needs_refresh(100, now=900)


def test_x509_writes_private_pem(tmp_path):
    factory = MongoClientFactory({"connection_uri": "mongodb://h/", "auth_mode": "x509",
                                  "tls_certificate_key_pem": "-----BEGIN-----\n鍵\n"})
    kwargs = factory.build_kwargs()
    path = kwargs["tlsCertificateKeyFile"]
    with open(path, encoding="utf-8") as handle:
        assert handle.read() == "-----BEGIN-----\n鍵\n"
    import os  # pylint: disable=import-outside-toplevel
    assert oct(os.stat(path).st_mode & 0o777) == "0o600"
    assert kwargs["authMechanism"] == "MONGODB-X509"


def test_uc_provider_requires_https():
    provider = UcServiceCredentialProvider("http://x.cloud.databricks.com", "a", "b", "c")
    assert provider.host == "https://x.cloud.databricks.com"


class TestDeleteLookback:
    """The delete flow's look-back finds the oldest readable start instead of jumping to now."""

    def test_bisects_to_oplog_start(self, monkeypatch):
        import pymongo.errors  # pylint: disable=import-outside-toplevel
        from databricks.labs.community_connector.sources.mongodb import mongodb  # pylint: disable=import-outside-toplevel

        oplog_start = Timestamp(10_000 - 2_700, 10)  # 45 minutes, from increment 10
        monkeypatch.setattr(mongodb.MongodbLakeflowConnect, "_now",
                            staticmethod(lambda: Timestamp(10_000, 0)))

        class _Stream:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def try_next(self):
                return None

        class _Coll:
            def watch(self, start_at_operation_time=None, **_):
                if start_at_operation_time < oplog_start:
                    raise pymongo.errors.OperationFailure("lost", code=286)
                return _Stream()

        conn = mongodb.MongodbLakeflowConnect({"connection_uri": "mongodb://h/", "database": "d"})
        monkeypatch.setattr(conn, "_collection", lambda cfg: _Coll())
        cfg = mongodb._TableConfig(conn, "c", {})  # pylint: disable=protected-access
        start = conn._offset(cfg, "stream", None, start_at=10_000 - 3_600, lookback=True)  # pylint: disable=protected-access
        nxt = conn._oldest_available_start(cfg, start, "delete")  # pylint: disable=protected-access
        assert (nxt["start_at"], nxt["start_at_inc"]) == (oplog_start.time, oplog_start.inc)
        assert "lookback" not in nxt

        oplog_start = Timestamp(10_000 - 30, 0)
        with pytest.raises(RuntimeError, match="does not reach back"):
            conn._oldest_available_start(cfg, start, "delete")  # pylint: disable=protected-access

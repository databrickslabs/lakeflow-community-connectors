"""Client construction and authentication for the MongoDB connector.

Three ways to authenticate, chosen with the ``auth_mode`` connection parameter:

``uri`` (default)
    Everything is in ``connection_uri``: SCRAM user/password, ``authMechanism``
    and any other driver option. Works on every compute type.

``x509``
    The client certificate (PEM, certificate + private key) is passed in
    ``tls_certificate_key_pem``. It is written to a private temporary file
    because the driver only accepts a file path.

``aws_service_credential``
    ``MONGODB-AWS`` with temporary AWS keys obtained from a Unity Catalog
    service credential. On standard access mode and serverless compute user
    code cannot reach the EC2 instance metadata service, so the driver's
    default credential chain finds no keys there, and
    ``getServiceCredentialsProvider`` only works inside UDFs. This mode calls
    the Unity Catalog REST API ``temporary-service-credentials`` with a service
    principal's OAuth token instead, and refreshes the keys before they expire.

On classic dedicated compute with an instance profile, put
``authMechanism=MONGODB-AWS`` in ``connection_uri`` and keep ``auth_mode=uri``:
PyMongo then reads the keys from the instance metadata service itself.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from typing import Any, Optional

import requests

AUTH_MODES = ("uri", "x509", "aws_service_credential")
HTTP_TIMEOUT_SECONDS = 30
# Refresh AWS keys this long before they expire. Unity Catalog can hand back
# keys that were issued earlier, so the remaining lifetime is often well under
# an hour; the refresh decision must be based on the reported expiry time.
DEFAULT_REFRESH_MARGIN_SECONDS = 600
# Temporary keys from Unity Catalog last at most one hour, and an issued key
# can be returned again with part of its lifetime used (about 46 minutes left
# was observed), so a margin above 30 minutes would refresh on every batch.
MIN_REFRESH_MARGIN_SECONDS = 60
MAX_REFRESH_MARGIN_SECONDS = 1800
# OAuth scope of the token used for temporary-service-credentials.
OAUTH_SCOPE = "unity-catalog"
# The client secret is sent to databricks_host, so it must be a Databricks
# workspace on AWS (MONGODB-AWS needs an AWS IAM role service credential).
WORKSPACE_DOMAINS = (".cloud.databricks.com", ".cloud.databricks.us", ".cloud.databricks.mil")


class AwsKeys:  # pylint: disable=too-few-public-methods
    """Temporary AWS keys and their expiry (epoch seconds)."""

    def __init__(self, access_key_id: str, secret_access_key: str, session_token: str,
                 expires_at: float):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.expires_at = expires_at

    def needs_refresh(self, margin_seconds: int, now: Optional[float] = None) -> bool:
        return (now if now is not None else time.time()) >= self.expires_at - margin_seconds


def _auth_mode_from_options(options: dict[str, str]) -> str:
    """The auth method whose required parameters are given (as the connector spec says)."""
    if options.get("aws_service_credential"):
        return "aws_service_credential"
    if options.get("tls_certificate_key_pem"):
        return "x509"
    return "uri"


def workspace_url(host: str) -> str:
    """``https://<host>`` for a Databricks workspace on AWS; reject anything else.

    The service principal's client secret is sent to this host, so a typo or a
    look-alike domain must not receive it.
    """
    from urllib.parse import urlsplit  # pylint: disable=import-outside-toplevel

    raw = host.strip()
    url = urlsplit(raw if "://" in raw else "https://" + raw)
    name = (url.hostname or "").lower()
    if url.scheme not in ("https", "http") or not name.endswith(WORKSPACE_DOMAINS) \
            or url.path.strip("/") or url.query or url.port not in (None, 443):
        raise ValueError(
            f"'databricks_host' must be the URL of a Databricks workspace on AWS (for example "
            f"https://dbc-1234abcd-5678.cloud.databricks.com); got {host!r}. The service "
            "principal's client secret is sent to this host."
        )
    return f"https://{name}"


def refresh_margin(value: Optional[str]) -> int:
    if value in (None, ""):
        return DEFAULT_REFRESH_MARGIN_SECONDS
    try:
        margin = int(value)
    except (TypeError, ValueError):
        margin = -1
    if not MIN_REFRESH_MARGIN_SECONDS <= margin <= MAX_REFRESH_MARGIN_SECONDS:
        raise ValueError(
            f"'aws_refresh_margin_seconds' must be an integer from {MIN_REFRESH_MARGIN_SECONDS} "
            f"to {MAX_REFRESH_MARGIN_SECONDS} (temporary keys last at most one hour); got {value!r}"
        )
    return margin


class UcServiceCredentialProvider:
    """Fetch temporary AWS keys for a Unity Catalog service credential.

    Authenticates to the workspace as a service principal with OAuth
    machine-to-machine (client credentials). The service principal needs the
    ``workspace-access`` entitlement and ``ACCESS`` on the service credential.
    """

    def __init__(self, host: str, client_id: str, client_secret: str, credential_name: str,
                 session: Optional[requests.Session] = None):
        if not host:
            raise ValueError("auth_mode=aws_service_credential requires 'databricks_host'")
        if not client_id or not client_secret:
            raise ValueError(
                "auth_mode=aws_service_credential requires 'databricks_client_id' and "
                "'databricks_client_secret' (a service principal OAuth secret)"
            )
        if not credential_name:
            raise ValueError("auth_mode=aws_service_credential requires 'aws_service_credential'")
        self.host = workspace_url(host)
        self.client_id = client_id
        self.client_secret = client_secret
        self.credential_name = credential_name
        self._session = session or requests.Session()
        self._token: Optional[str] = None
        self._token_expires_at = 0.0

    def _oauth_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        resp = self._session.post(
            f"{self.host}/oidc/v1/token",
            # Only the Unity Catalog APIs: the token cannot call other APIs
            # even if the service principal is allowed to.
            data={"grant_type": "client_credentials", "scope": OAUTH_SCOPE},
            auth=(self.client_id, self.client_secret),
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"OAuth token request to {self.host} failed with HTTP {resp.status_code}: "
                f"{resp.text[:300]}"
            )
        body = resp.json()
        self._token = body["access_token"]
        self._token_expires_at = time.time() + int(body.get("expires_in", 3600))
        return self._token

    def fetch(self) -> AwsKeys:
        resp = self._session.post(
            f"{self.host}/api/2.1/unity-catalog/temporary-service-credentials",
            json={"credential_name": self.credential_name},
            headers={"Authorization": f"Bearer {self._oauth_token()}"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            hint = ""
            if resp.status_code == 403 and "entitlement" in resp.text:
                hint = " (grant the service principal the 'workspace-access' entitlement)"
            raise RuntimeError(
                f"temporary-service-credentials for '{self.credential_name}' failed with HTTP "
                f"{resp.status_code}: {resp.text[:300]}{hint}"
            )
        body = resp.json()
        aws = body.get("aws_temp_credentials")
        if not aws:
            raise RuntimeError(
                f"service credential '{self.credential_name}' did not return AWS credentials; "
                "it must be an AWS IAM role service credential"
            )
        return AwsKeys(
            aws["access_key_id"],
            aws["secret_access_key"],
            aws["session_token"],
            int(body["expiration_time"]) / 1000,
        )


def _write_private_file(content: str, suffix: str) -> str:
    fd, path = tempfile.mkstemp(prefix="lakeflow_mongodb_", suffix=suffix)
    try:
        os.write(fd, content.encode("utf-8"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return path


class MongoClientFactory:
    """Build ``MongoClient`` instances and rebuild them when keys expire.

    The factory is created on the driver from the connector options and is
    pickled to Python workers, so it never holds a live client or socket.
    """

    def __init__(self, options: dict[str, str], client_kwargs: Optional[dict] = None):
        self.uri = options.get("connection_uri") or ""
        if not self.uri:
            raise ValueError("MongoDB connector requires the 'connection_uri' connection parameter")
        self.auth_mode = options.get("auth_mode") or _auth_mode_from_options(options)
        if self.auth_mode not in AUTH_MODES:
            raise ValueError(
                f"'auth_mode' must be one of {', '.join(AUTH_MODES)}; got {self.auth_mode!r}"
            )
        self.options = {k: v for k, v in options.items() if v is not None}
        self.client_kwargs = dict(client_kwargs or {})
        self.refresh_margin = refresh_margin(options.get("aws_refresh_margin_seconds"))
        self._lock = threading.Lock()
        self._provider: Optional[UcServiceCredentialProvider] = None
        self._keys: Optional[AwsKeys] = None
        self._files: dict[str, str] = {}
        if self.auth_mode == "x509" and not options.get("tls_certificate_key_pem"):
            raise ValueError("auth_mode=x509 requires 'tls_certificate_key_pem'")
        if self.auth_mode == "aws_service_credential":
            # Validate eagerly so configuration errors surface at pipeline start.
            self._make_provider()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_lock"] = None
        state["_provider"] = None
        state["_keys"] = None
        state["_files"] = {}
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def _make_provider(self) -> UcServiceCredentialProvider:
        if self._provider is None:
            self._provider = UcServiceCredentialProvider(
                self.options.get("databricks_host", ""),
                self.options.get("databricks_client_id", ""),
                self.options.get("databricks_client_secret", ""),
                self.options.get("aws_service_credential", ""),
            )
        return self._provider

    def _file_option(self, key: str, suffix: str) -> Optional[str]:
        """Write a PEM option to a private temp file and return its path.

        PyMongo takes certificates only as file paths. It reads them into the
        SSL context when ``MongoClient`` is constructed, so :meth:`create`
        deletes the files right after that.
        """
        pem = self.options.get(key)
        if not pem:
            return None
        path = self._files.get(key)
        if not path or not os.path.exists(path):
            path = _write_private_file(pem, suffix)
            self._files[key] = path
        return path

    def _remove_files(self) -> None:
        for path in self._files.values():
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        self._files.clear()

    def credentials_expire_soon(self) -> bool:
        """True when a client built earlier must be replaced (new AWS keys)."""
        if self.auth_mode != "aws_service_credential":
            return False
        with self._lock:
            return self._keys is None or self._keys.needs_refresh(self.refresh_margin)

    def build_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "appname": "lakeflow-community-connector-mongodb",
            # Fail fast with a clear error instead of hanging the micro-batch.
            "serverSelectionTimeoutMS": 30000,
            # Keep the first undecodable byte sequence from silently turning
            # into replacement characters: strict is the default, ``replace``
            # is opt-in through the ``unicode_decode_error_handler`` option.
            "unicode_decode_error_handler": self.options.get(
                "unicode_decode_error_handler"
            ) or "strict",
            "tz_aware": True,
        }
        kwargs.update(self.client_kwargs)
        ca = self._file_option("tls_ca_pem", ".pem")
        if ca:
            kwargs["tlsCAFile"] = ca
        if self.auth_mode == "x509":
            kwargs["tlsCertificateKeyFile"] = self._file_option("tls_certificate_key_pem", ".pem")
            kwargs["authMechanism"] = "MONGODB-X509"
            kwargs["tls"] = True
        elif self.auth_mode == "aws_service_credential":
            with self._lock:
                if self._keys is None or self._keys.needs_refresh(self.refresh_margin):
                    self._keys = self._make_provider().fetch()
                keys = self._keys
            kwargs["authMechanism"] = "MONGODB-AWS"
            kwargs["authSource"] = "$external"
            kwargs["username"] = keys.access_key_id
            kwargs["password"] = keys.secret_access_key
            # Passed as a dict, not in the URI: the token can contain characters
            # that are not safe inside authMechanismProperties.
            kwargs["authMechanismProperties"] = {"AWS_SESSION_TOKEN": keys.session_token}
        return kwargs

    def create(self):
        import pymongo  # pylint: disable=import-outside-toplevel

        kwargs = self.build_kwargs()
        if self._files:
            # PyMongo resolves mongodb+srv:// when the client first connects and
            # then reads the certificate files again. connect=True (overriding
            # connect=false in the URI) does that inside the constructor, so the
            # files can be deleted right after it.
            kwargs["connect"] = True
        try:
            return pymongo.MongoClient(self.uri, **kwargs)
        finally:
            # The certificates are in the client's SSL context now.
            self._remove_files()

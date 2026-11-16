"""The merged single-file source must load in a Python worker without PyMongo.

Managed ingestion pipelines create the data source in a Python worker that
has only the packages of the serverless base environment, not the pipeline's
dependencies: unpickling the source there failed with ``No module named
'bson'`` (seen on a COMMUNITY connection pipeline created with the CLI). The
merged source is pickled by value, so every ``bson`` / ``pymongo`` name that
its classes reach through module globals is imported when it is unpickled.
This test pickles the registered class and loads it in a subprocess where
``bson`` and ``pymongo`` cannot be imported, then builds the source and its
schema.
"""

import importlib.util
import pathlib
import subprocess
import sys

from pyspark import cloudpickle

SOURCE = (pathlib.Path(__file__).parents[4] / "src/databricks/labs/community_connector/sources"
          / "mongodb/_generated_mongodb_python_source.py")

_CHILD = r"""
import sys
for name in ("bson", "pymongo", "gridfs", "pymongo_auth_aws"):
    sys.modules[name] = None  # any import of the driver fails
from pyspark import cloudpickle
cls = cloudpickle.loads(sys.stdin.buffer.read())
source = cls({"tableName": "orders", "connection_uri": "mongodb://h/", "database": "db"})
print("FIELDS", ",".join(f.name for f in source.schema().fields))
"""


def _registered_class():
    spec = importlib.util.spec_from_file_location("_merged_mongodb_for_test", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    registered = []

    class _Spark:  # pylint: disable=too-few-public-methods
        class dataSource:  # pylint: disable=invalid-name,too-few-public-methods
            @staticmethod
            def register(cls):
                registered.append(cls)

    module.register_lakeflow_source(_Spark)
    return registered[0]


def test_merged_source_loads_without_pymongo():
    payload = cloudpickle.dumps(_registered_class())
    result = subprocess.run([sys.executable, "-c", _CHILD], input=payload,
                            capture_output=True, timeout=120, check=False)
    out = result.stdout.decode() + result.stderr.decode()
    assert result.returncode == 0, out[-2000:]
    assert "FIELDS _id,document,_cdc_operation" in out

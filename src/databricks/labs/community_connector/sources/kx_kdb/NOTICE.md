# KX KDB+ HDB connector third-party notices

This file lists the dependencies used by the KX KDB+ HDB connector and their
license families. It is intended for open-source attribution and IP review.
The exact transitive versions resolved by `pip` can vary by Python version and
platform.

## Distribution model

The connector source and wheel do not bundle or redistribute PyKX, `q.so`,
KDB-X, the KX installer, an offline KX bundle, or a KX license. Customers
provide and license those components separately.

The optional online bootstrap downloads the installer directly from KX using
the customer's KX bearer token and license. The installer is held only in a
process-local temporary directory for that bootstrap attempt. It is not
persisted, uploaded, cached for another run, or re-served to another customer
or workspace. The connector contains no shared or Databricks-held KX
credential or license.

## Declared dependencies

| Dependency | Connector constraint | License / terms | Project |
| --- | --- | --- | --- |
| pandas | `>=2.0.0,<3.0` | BSD-3-Clause | <https://pandas.pydata.org/> |
| PyKX | `==4.0.0b5` | Dual licensed: Apache-2.0 Python components and KX Software License terms for `q.so`/KDB-X | <https://code.kx.com/pykx/4.0/license.html> |
| lakeflow-community-connectors | `>=0.1.0,<1.0` | Part of this repository; governed by the repository `LICENSE` | <https://github.com/databrickslabs/lakeflow-community-connectors> |

PyKX/KDB-X is not treated as an open-source-only dependency. For this
connector on Databricks, customers must provide a commercial KX license whose
terms expressly permit third-party-cloud deployment. Personal and Community
licenses are not supported for this deployment.

## Mandatory open-source transitive dependencies

The following packages are introduced by pandas, PyKX, or the
lakeflow-community-connectors framework under their standard mandatory
dependency sets:

| Dependency | License family |
| --- | --- |
| annotated-types | MIT |
| certifi | MPL-2.0 |
| cffi | MIT |
| charset-normalizer | MIT |
| cryptography | Apache-2.0 OR BSD-3-Clause |
| dill | BSD-3-Clause |
| idna | BSD-3-Clause |
| numpy | BSD-3-Clause |
| pydantic | MIT |
| pydantic-core | MIT |
| pycparser | BSD-3-Clause |
| PyJWT | MIT |
| python-dateutil | Apache-2.0 OR BSD-3-Clause |
| pytz | MIT |
| PyYAML | MIT |
| requests | Apache-2.0 |
| six | MIT |
| toml | MIT |
| typing-extensions | PSF-2.0 |
| typing-inspection | MIT |
| tzdata | Apache-2.0 |
| urllib3 | MIT |

PySpark and PyArrow can be supplied by the Databricks runtime. They are not
declared as connector runtime dependencies or bundled in the connector wheel;
both are Apache-2.0 projects.

## Copyleft review

No GPL, LGPL, or AGPL dependency was identified among the connector's declared
dependencies or the mandatory transitive dependencies listed above.

`certifi` uses MPL-2.0, a file-level weak-copyleft license. It is listed
explicitly for IP counsel review and is not bundled into the connector wheel.

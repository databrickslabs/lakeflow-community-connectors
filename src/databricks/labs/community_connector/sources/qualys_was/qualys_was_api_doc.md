# Qualys WAS API Notes (Phase-1)

Primary APIs referenced:
- Search Web Apps: `POST /qps/rest/3.0/search/was/webapp`
- Search Findings: `POST /qps/rest/3.0/search/was/finding`
- Search Scans: `POST /qps/rest/3.0/search/was/wasscan`
- Get Scan Details: `GET /qps/rest/3.0/get/was/wasscan/{id}`

Notes:
- WAS QPS APIs are XML-oriented.
- This phase-1 spike uses deterministic simulator fixtures and source-native fields.

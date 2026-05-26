# Qualys VMDR API Notes (Phase-1)

Primary VMDR APIs referenced:
- Host List: `GET/POST /api/2.0/fo/asset/host/?action=list`
- Host Detection List: `GET/POST /api/2.0/fo/asset/host/vm/detection/?action=list`
- KnowledgeBase Download: `GET/POST /api/2.0/fo/knowledge_base/vuln/?action=list`
- VM Scan List: `GET/POST /api/2.0/fo/scan/?action=list`
- VM Scan Summary: `GET/POST /api/2.0/fo/scan/vm/summary/?action=list`
- Asset Group List: `GET/POST /api/2.0/fo/asset/group/?action=list`

Notes:
- VMDR FO APIs are XML-oriented and frequently paginate using truncation/warning URL patterns.
- This phase-1 spike implements deterministic simulator fixtures and source-native fields only.

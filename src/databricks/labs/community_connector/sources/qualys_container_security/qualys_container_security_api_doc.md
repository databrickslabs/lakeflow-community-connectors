# Qualys Container Security API Notes (Phase-1)

Primary APIs referenced:
- Containers: `GET /csapi/v1.3/containers`
- Images: `GET /csapi/v1.3/images`
- Image Vulnerabilities: `GET /csapi/v1.3/images/{imageSha}/vuln`
- Registries: `GET /csapi/v1.3/registry`
- Sensors: `GET /csapi/v1.3/sensors`

Notes:
- APIs are JSON and use Bearer auth.
- Container/image list endpoints are paginated via `pageNumber` and `pageSize`.

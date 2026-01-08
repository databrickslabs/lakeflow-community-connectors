# Particle.io REST API - GET Methods Summary

## Overview

The Particle Device Cloud API is a RESTful API that enables communication between computers, mobile devices, and Particle IoT devices (such as Argon, Boron, and Photon 2).

**Base URL:** `https://api.particle.io`

**Protocol:** All requests require TLSv1.2 or later.

**Authentication:** Include access token in Authorization header:
```bash
curl -H "Authorization: Bearer YOUR_ACCESS_TOKEN" https://api.particle.io/v1/devices
```

---

## Devices

### List Devices

```
GET /v1/devices
```

List all devices the authenticated user has access to.

```bash
curl https://api.particle.io/v1/devices \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
[
  {
    "id": "53ff6f0650723",
    "name": "my_device",
    "online": true,
    "platform_id": 6,
    "functions": ["led"],
    "variables": {"temperature": "int32"},
    "system_firmware_version": "1.5.0"
  }
]
```

---

### Get Device Information

```
GET /v1/devices/:deviceId
```

Get detailed information about a specific device.

```bash
curl https://api.particle.io/v1/devices/DEVICE_ID \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response includes:** `id`, `name`, `owner`, `last_ip_address`, `last_heard`, `online`, `platform_id`, `cellular`, `functions`, `variables`, `status`, `serial_number`, `system_firmware_version`

---

### List Devices in a Product

```
GET /v1/products/:productIdOrSlug/devices
```

List all devices that are part of a product. Results are paginated (default 25 per page).

**Query Parameters:**
- `deviceId` - Filter by device ID (partial match)
- `deviceName` - Filter by device name (partial match)
- `groups` - Filter by group names (comma-separated)
- `serialNumber` - Filter by serial number (partial match)
- `sortAttr` - Sort by: `deviceName`, `deviceId`, `firmwareVersion`, `lastConnection`
- `sortDir` - `asc` or `desc`
- `page` - Page number
- `perPage` - Records per page

```bash
curl "https://api.particle.io/v1/products/my-product/devices?groups=beta" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Get Product Device Information

```
GET /v1/products/:productIdOrSlug/devices/:deviceId
```

Get information about a specific device within a product.

```bash
curl https://api.particle.io/v1/products/my-product/devices/DEVICE_ID \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Look Up Device by Serial Number

```
GET /v1/serial_numbers/:serial_number
```

Return device ID and SIM ICCID for a device by its serial number. Rate limited to 50 requests/hour.

```bash
curl https://api.particle.io/v1/serial_numbers/E26AAA111111111 \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "ok": true,
  "deviceID": "0123456789abcdef01234567",
  "iccid": "8934076500002589174"
}
```

---

## Variables

### Get a Variable Value

```
GET /v1/devices/:deviceId/:varName
```

Request the current value of a variable exposed by the device.

```bash
curl https://api.particle.io/v1/devices/DEVICE_ID/temperature \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Query Parameters:**
- `format=raw` - Return only the value without metadata

**Response:**
```json
{
  "name": "temperature",
  "result": 25,
  "coreInfo": {
    "name": "my_device",
    "deviceID": "DEVICE_ID",
    "connected": true,
    "last_handshake_at": "2024-01-15T12:00:00.000Z"
  }
}
```

---

### Get Product Device Variable

```
GET /v1/products/:productIdOrSlug/devices/:deviceId/:varName
```

Get a variable value from a device within a product.

```bash
curl https://api.particle.io/v1/products/my-product/devices/DEVICE_ID/temperature \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Events

### Get a Stream of Events

```
GET /v1/events/:eventPrefix
```

Open a Server-Sent Events (SSE) stream for all events matching the prefix filter.

```bash
curl https://api.particle.io/v1/events/temp \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response (SSE format):**
```
event: temperature
data: {"data":"25.34","ttl":"60","published_at":"2024-01-15T12:00:00.000Z","coreid":"DEVICE_ID"}
```

---

### Get a Stream of Your Device Events

```
GET /v1/devices/events/:eventPrefix
```

Stream events from all your devices. The eventPrefix is optional.

```bash
curl https://api.particle.io/v1/devices/events \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Get Events for a Specific Device

```
GET /v1/devices/:deviceId/events/:eventPrefix
```

Stream events from a specific device.

```bash
curl https://api.particle.io/v1/devices/DEVICE_ID/events/temperature \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Remote Diagnostics

### Get Last Known Device Vitals

```
GET /v1/diagnostics/:deviceId/last
```

Returns the most recent device vitals payload.

```bash
curl https://api.particle.io/v1/diagnostics/DEVICE_ID/last \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "diagnostics": {
    "updated_at": "2024-01-15T12:00:00.000Z",
    "deviceID": "DEVICE_ID",
    "payload": {
      "device": {
        "network": { "signal": { "strength": 66.66 } },
        "cloud": { "connection": { "status": "connected" } },
        "system": { "uptime": 2567461, "memory": { "used": 36016, "total": 113664 } }
      }
    }
  }
}
```

---

### Get All Historical Device Vitals

```
GET /v1/diagnostics/:deviceId
```

Returns all stored device vitals (expires after 1 month).

**Query Parameters:**
- `start_date` - Oldest diagnostic to return (ISO8601 format)
- `end_date` - Newest diagnostic to return (ISO8601 format)

**Headers:**
- `Accept: text/csv` - Return results as CSV

```bash
curl "https://api.particle.io/v1/diagnostics/DEVICE_ID?start_date=2024-01-01T00:00:00Z&end_date=2024-01-15T00:00:00Z" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Get Cellular Network Status

```
GET /v1/sims/:iccid/status
```

Check if a device/SIM has an active data session. This is an async request - poll until `meta.state` is `complete`.

```bash
curl https://api.particle.io/v1/sims/ICCID/status \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "ok": true,
  "meta": {
    "state": "complete",
    "task_id": "1234abcd"
  },
  "sim_status": {
    "connected": true,
    "gsm_connection": true,
    "data_connection": true
  }
}
```

---

## SIM Cards

### List SIM Cards

```
GET /v1/sims
```

List all SIM cards owned by the user.

```bash
curl https://api.particle.io/v1/sims \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### List Product SIM Cards

```
GET /v1/products/:productIdOrSlug/sims
```

List SIM cards in a product. Paginated (default 25 per page).

**Query Parameters:**
- `iccid` - Filter by ICCID (partial match)
- `deviceId` - Filter by associated device ID
- `deviceName` - Filter by associated device name
- `page` - Page number
- `perPage` - Records per page

```bash
curl https://api.particle.io/v1/products/my-product/sims \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Get SIM Information

```
GET /v1/sims/:iccid
```

Get details for a specific SIM card.

```bash
curl https://api.particle.io/v1/sims/ICCID \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response includes:** `_id`, `status`, `base_country_code`, `carrier`, `first_activated_on`, `last_device_id`, `last_device_name`

---

### Get SIM Data Usage

```
GET /v1/sims/:iccid/data_usage
```

Get data usage for the current billing period, broken out by day.

```bash
curl https://api.particle.io/v1/sims/ICCID/data_usage \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "iccid": "8934076500002589174",
  "usage_by_day": [
    { "date": "2024-01-14", "mbs_used": 0.98, "mbs_used_cumulative": 0.98 },
    { "date": "2024-01-15", "mbs_used": 0.50, "mbs_used_cumulative": 1.48 }
  ]
}
```

---

### Get Fleet Data Usage

```
GET /v1/products/:productIdOrSlug/sims/data_usage
```

Get fleet-wide SIM data usage for a product.

```bash
curl https://api.particle.io/v1/products/my-product/sims/data_usage \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "total_mbs_used": 200.00,
  "total_active_sim_cards": 2000,
  "usage_by_day": [
    { "date": "2024-01-14", "mbs_used": 100.00, "mbs_used_cumulative": 100.00 }
  ]
}
```

---

## User

### Get User Information

```
GET /v1/user
```

Get the current authenticated user's information.

```bash
curl https://api.particle.io/v1/user \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "username": "user@example.com",
  "subscription_ids": [],
  "account_info": {
    "first_name": "John",
    "last_name": "Doe",
    "business_account": false
  },
  "mfa": { "enabled": false },
  "wifi_device_count": 5,
  "cellular_device_count": 2
}
```

---

## Access Tokens

### Get Current Access Token Information

```
GET /v1/access_tokens/current
```

Get information about the currently used token.

```bash
curl https://api.particle.io/v1/access_tokens/current \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "expires_at": "2024-04-15T12:00:00.000Z",
  "client": "particle",
  "scopes": [],
  "orgs": []
}
```

---

## OAuth Clients

### List OAuth Clients

```
GET /v1/clients
```

List all OAuth clients for the authenticated user.

```bash
curl https://api.particle.io/v1/clients \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

**Response:**
```json
{
  "ok": true,
  "clients": [
    { "name": "MyApp", "type": "installed", "id": "myapp-2146" }
  ]
}
```

---

### List Product OAuth Clients

```
GET /v1/products/:productIdOrSlug/clients
```

List OAuth clients associated with a product.

```bash
curl https://api.particle.io/v1/products/my-product/clients \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Search

### Search Organization

```
GET /v1/orgs/:orgIdOrSlug/search
```

Search an organization for devices, products, and SIMs. Returns max 10 results per type.

**Query Parameters:**
- `text` (required) - Search text
- `type` - Filter by type: `device`, `product`, `sim`

```bash
curl "https://api.particle.io/v1/orgs/my-org/search?text=alpha" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Search Sandbox

```
GET /v1/user/search
```

Search your personal sandbox for devices, products, and SIMs.

```bash
curl "https://api.particle.io/v1/user/search?text=alpha" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Products

### List Products

```
GET /v1/products
```

List all products accessible to the user.

```bash
curl https://api.particle.io/v1/products \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### Get Product Details

```
GET /v1/products/:productIdOrSlug
```

Get details for a specific product.

```bash
curl https://api.particle.io/v1/products/my-product \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

### List Product Team Members

```
GET /v1/products/:productIdOrSlug/team
```

List team members (including API users) for a product.

```bash
curl https://api.particle.io/v1/products/my-product/team \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Rate Limits

| Scope | Limit |
|-------|-------|
| General API calls | 10,000 requests / 5 minutes per IP |
| SSE connections | 100 simultaneous connections per IP |
| Serial number lookup | 50 requests / hour per user |

---

## Response Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad Request - Invalid parameters |
| 401 | Unauthorized - Invalid/expired token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Device offline or doesn't exist |
| 408 | Timeout - Device didn't respond |
| 429 | Too Many Requests - Rate limited |
| 500 | Server Error |

---

## Additional Resources

- **Full API Reference:** https://docs.particle.io/reference/cloud-apis/api/
- **Getting Started:** https://docs.particle.io/getting-started/cloud/cloud-api/

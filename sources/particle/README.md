# Particle.io Connector

This connector provides data ingestion from the [Particle Device Cloud](https://docs.particle.io/reference/cloud-apis/api/) REST API into Databricks using Lakeflow.

## Overview

Particle.io is an IoT platform that provides tools for building, connecting, and managing IoT devices. This connector enables you to ingest data about your devices, products, SIM cards, diagnostics, and more.

## Authentication

The connector supports two authentication methods:

### Method 1: Direct Access Token (Recommended)
Use a pre-generated Particle access token.

**Required Options:**
- `access_token`: Your Particle API access token

### Method 2: Username/Password (Automatic)
Provide your Particle account credentials and the connector will automatically generate an access token.

**Required Options:**
- `username`: Your Particle account email address
- `password`: Your Particle account password

**Optional Options (both methods):**
- `base_url`: Override the Particle API base URL (default: `https://api.particle.io`)
- `token_cache_file`: File path to cache generated tokens (default: None)

### Getting Your Credentials

**For Method 1 (Direct Token):**
1. Visit: https://console.particle.io/
2. Go to: Settings → Access Tokens
3. Create a new token
4. Use the token directly in config

**For Method 2 (Username/Password):**
- Use your Particle account email and password
- The connector will handle OAuth token generation automatically

## Supported Tables

| Table | Description | Ingestion Type | Required Table Options |
|-------|-------------|----------------|------------------------|
| `devices` | All devices owned by the authenticated user | Snapshot | None |
| `products` | All products accessible to the user | Snapshot | None |
| `sims` | All SIM cards owned by the user | Snapshot | None |
| `user` | Authenticated user information | Snapshot | None |
| `oauth_clients` | OAuth clients for the authenticated user | Snapshot | None |
| `product_devices` | Devices within a specific product | Snapshot | `product_id_or_slug` |
| `product_sims` | SIM cards within a specific product | Snapshot | `product_id_or_slug` |
| `diagnostics` | Historical device vitals/diagnostics | Append | `device_id` |
| `sim_data_usage` | SIM card data usage for current billing period | Snapshot | `iccid` |
| `fleet_data_usage` | Fleet-wide SIM data usage for a product | Snapshot | `product_id_or_slug` |

## Table Options

### product_devices

| Option | Required | Description |
|--------|----------|-------------|
| `product_id_or_slug` | Yes | Product ID or slug to list devices for |
| `groups` | No | Filter by group names (comma-separated) |
| `device_name` | No | Filter by device name (partial match) |
| `device_id` | No | Filter by device ID (partial match) |
| `serial_number` | No | Filter by serial number (partial match) |
| `sort_attr` | No | Sort by: `deviceName`, `deviceId`, `firmwareVersion`, `lastConnection` |
| `sort_dir` | No | Sort direction: `asc` or `desc` |
| `per_page` | No | Records per page (default: 100, max: 100) |

### product_sims

| Option | Required | Description |
|--------|----------|-------------|
| `product_id_or_slug` | Yes | Product ID or slug to list SIMs for |
| `iccid` | No | Filter by ICCID (partial match) |
| `device_id` | No | Filter by associated device ID |
| `device_name` | No | Filter by associated device name |
| `per_page` | No | Records per page (default: 100, max: 100) |

### diagnostics

| Option | Required | Description |
|--------|----------|-------------|
| `device_id` | Yes | Device ID to get diagnostics for |
| `start_date` | No | Oldest diagnostic to return (ISO8601 format) |
| `end_date` | No | Newest diagnostic to return (ISO8601 format) |

### sim_data_usage

| Option | Required | Description |
|--------|----------|-------------|
| `iccid` | Yes | SIM ICCID to get data usage for |

### fleet_data_usage

| Option | Required | Description |
|--------|----------|-------------|
| `product_id_or_slug` | Yes | Product ID or slug to get fleet data usage for |

## Configuration

### Connection Configuration (dev_config.json)

```json
{
    "access_token": "YOUR_PARTICLE_ACCESS_TOKEN"
}
```

### Table Configuration (dev_table_config.json)

```json
{
    "product_devices": {
        "product_id_or_slug": "my-product"
    },
    "product_sims": {
        "product_id_or_slug": "my-product"
    },
    "diagnostics": {
        "device_id": "0123456789abcdef01234567"
    },
    "sim_data_usage": {
        "iccid": "8934076500002589174"
    },
    "fleet_data_usage": {
        "product_id_or_slug": "my-product"
    }
}
```

## Running Tests

```bash
# Update the config files with your credentials
# sources/particle/configs/dev_config.json
# sources/particle/configs/dev_table_config.json

# Run tests
pytest sources/particle/test/test_particle_lakeflow_connect.py -v
```

## Rate Limits

The Particle API has the following rate limits:

| Scope | Limit |
|-------|-------|
| General API calls | 10,000 requests / 5 minutes per IP |
| SSE connections | 100 simultaneous connections per IP |
| Serial number lookup | 50 requests / hour per user |

## Additional Resources

- [Particle Cloud API Reference](https://docs.particle.io/reference/cloud-apis/api/)
- [Getting Started with Particle Cloud API](https://docs.particle.io/getting-started/cloud/cloud-api/)

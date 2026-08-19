# Lakeflow Xquik Community Connector

Use this connector to ingest public X (Twitter) data through the Xquik REST API
into Databricks. It supports Twitter advanced search results, public profiles,
profile timelines, and regional trends.

## Prerequisites

- An Xquik account with available credits.
- An API key created through the [Xquik authentication guide](https://docs.xquik.com/api-reference/authentication).
- Network access to `https://xquik.com`.
- A Databricks workspace that supports Lakeflow community connectors.

Xquik is an independent third-party service. Not affiliated with X Corp.
"Twitter" and "X" are trademarks of X Corp.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `api_key` | string | Yes | Xquik API key. The connector sends it only in the `x-api-key` header. |

The connector uses table-specific options. Set the connection's required
`externalOptionsAllowList` to this exact value:

`count,include_replies,limit,max_pages,page_size,q,query_type,since_date,since_time,until_date,until_time,usernames,woeids`

### Obtain an API Key

1. Sign in to the Xquik dashboard.
2. Open API Keys.
3. Create a key and copy it once.
4. Store the key in the Unity Catalog connection as `api_key`.

Do not place the key in pipeline code, table options, logs, or source control.
The connector rejects redirects instead of forwarding the key to another origin.

### Create a Unity Catalog Connection

1. Open the Lakeflow Community Connector flow from the Add Data page.
2. Select the Xquik connector or create a connection for this source.
3. Enter `api_key`.
4. Set `externalOptionsAllowList` to the exact value above.

You can also create the connection through the standard Unity Catalog API.

## Supported Objects

All objects use snapshot ingestion. Each run drains cursor pages up to
`max_pages`. Date options bound the source query but do not create a Lakeflow
incremental offset.

| Object | Description | Primary key |
|---|---|---|
| `tweets_search` | Tweets matching one search query | `id`, `search_query` |
| `user_profiles` | Profiles for configured usernames or numeric user IDs | `id` |
| `user_tweets` | Timeline tweets for configured usernames or user IDs | `id`, `source_username` |
| `trends` | Ranked trends for configured WOEIDs | `woeid`, `name` |

The connector adds lineage fields that protect keys across configured slices:
`search_query`, `configured_username`, `source_username`, and `woeid`.

## Table Configurations

### Source & Destination

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Exact source object name |
| `destination_catalog` | No | Target catalog |
| `destination_schema` | No | Target schema |
| `destination_table` | No | Target table name |

### Common `table_configuration` Options

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` or `SCD_TYPE_2` for snapshot tables |
| `primary_keys` | No | Override the connector primary key |
| `sequence_by` | No | Sequence column for SCD Type 2 |
| `cluster_by` | No | Destination Liquid Clustering columns |

### Source-Specific Options

| Object | Option | Required | Description |
|---|---|---|---|
| `tweets_search` | `q` | Yes | Search query, hashtag, operators, Tweet ID, or status URL |
| `tweets_search` | `query_type` | No | `Latest` or `Top`; default `Latest` |
| `tweets_search` | `limit` | No | Requested result limit from 1 to 200; default 100 |
| `tweets_search` | `since_time` | No | ISO 8601 lower time bound |
| `tweets_search` | `until_time` | No | ISO 8601 upper time bound |
| `user_profiles` | `usernames` | Yes | Comma-separated usernames without `@`, or numeric user IDs |
| `user_tweets` | `usernames` | Yes | Comma-separated usernames without `@`, or numeric user IDs |
| `user_tweets` | `page_size` | No | Page size from 1 to 100; default 20 |
| `user_tweets` | `include_replies` | No | `true` or `false`; default `false` |
| `user_tweets` | `since_date` | No | `YYYY-MM-DD` lower date bound |
| `user_tweets` | `until_date` | No | `YYYY-MM-DD` upper date bound |
| `trends` | `woeids` | Yes | Comma-separated numeric WOEIDs; use `1` worldwide |
| `trends` | `count` | No | Trends per WOEID from 1 to 50; default 30 |
| All paginated objects | `max_pages` | No | Maximum cursor pages from 1 to 1,000; default 100 |

Supported trend regions include worldwide, United States, United Kingdom,
Turkey, Spain, Germany, France, Japan, India, Brazil, Canada, and Mexico. See
the [trends guide](https://docs.xquik.com/guides/trends) for WOEIDs.

## Data Type Mapping

| API type | Databricks type | Notes |
|---|---|---|
| string | string | Tweet and user IDs remain strings |
| integer | long | Engagement and profile counts |
| boolean | boolean | Verification and tweet markers |
| object | struct | Author fields remain nested |
| array of objects | array of struct | Media fields remain nested |
| ISO 8601 string | string | Cast to timestamp downstream if needed |

## How to Run

### Step 1: Add the Connector

Use the Lakeflow Community Connector UI to reference this repository and the
`xquik` source directory.

### Step 2: Configure the Pipeline

```json
{
  "pipeline_spec": {
    "connection_name": "xquik_connection",
    "object": [
      {
        "table": {
          "source_table": "tweets_search",
          "table_configuration": {
            "q": "\"open source\" lang:en",
            "query_type": "Latest",
            "since_time": "2026-08-01T00:00:00Z",
            "limit": "100",
            "max_pages": "5"
          }
        }
      },
      {
        "table": {
          "source_table": "user_profiles",
          "table_configuration": {"usernames": "databricks,apache_spark"}
        }
      },
      {
        "table": {
          "source_table": "user_tweets",
          "table_configuration": {
            "usernames": "databricks",
            "include_replies": "false",
            "page_size": "20",
            "max_pages": "5"
          }
        }
      },
      {
        "table": {
          "source_table": "trends",
          "table_configuration": {"woeids": "1,23424977", "count": "20"}
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline through your normal Lakeflow workflow. Scheduled snapshot
runs reread the configured source slice.

#### Best Practices

- Start with one table, narrow date bounds, and `max_pages: "1"`.
- Preserve the default primary keys for idempotent snapshot merges.
- Use specific queries. Broad search consumes more credits and driver memory.
- Update `since_time` or `since_date` in scheduled jobs to bound repeated reads.
- Respect X data rights, privacy duties, and platform restrictions.

#### Troubleshooting

- `401 unauthenticated`: Replace an invalid or revoked API key.
- `402 insufficient_credits`: Add credits or reduce the requested slice.
- `424` or `5xx`: The connector retries transient dependency failures.
- `429 rate_limit_exceeded`: The connector honors `Retry-After` and retries.
- Empty search results: Check query syntax, time bounds, and credit balance.
- Invalid usernames: Remove `@` and use letters, digits, or underscores.
- Large snapshots: Reduce `limit`, `max_pages`, username count, or date range.

## References

- [REST API overview](https://docs.xquik.com/api-reference/overview)
- [Search tweets](https://docs.xquik.com/api-reference/x/search-tweets)
- [Profile lookup](https://docs.xquik.com/api-reference/x/twitter-profile-lookup)
- [User tweets](https://docs.xquik.com/api-reference/x/user-tweets)
- [Regional trends](https://docs.xquik.com/api-reference/x/trends)
- [Authentication](https://docs.xquik.com/api-reference/authentication)
- [Privacy policy](https://xquik.com/en/privacy)

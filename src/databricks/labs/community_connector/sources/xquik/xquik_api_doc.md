# Xquik API Documentation

## Authorization

The connector uses one authentication method: an Xquik account API key.

- Connection parameter: `api_key`
- Request header: `x-api-key: <api_key>`
- REST base URL: `https://xquik.com/api/v1`
- Transport: HTTPS only

Create and revoke keys through the Xquik dashboard. Treat each key as a
secret. The connector never sends the key in a URL or query parameter.
It rejects redirects instead of forwarding the key to another origin.

```bash
curl "https://xquik.com/api/v1/x/trends?woeid=1&count=5" \
  -H "x-api-key: xq_YOUR_KEY_HERE"
```

## Object List

The object list is static. Xquik does not expose a schema-discovery endpoint.

| Object | API path | Required table options | Purpose |
|---|---|---|---|
| `tweets_search` | `GET /x/tweets/search` | `q` | Tweets matching an X or Twitter advanced search query |
| `user_profiles` | `GET /x/users/{username}` | `usernames` | Public profiles for usernames or numeric user IDs |
| `user_tweets` | `GET /x/users/{username}/tweets` | `usernames` | Public profile timelines |
| `trends` | `GET /x/trends` | `woeids` | Ranked regional trends |

## Object Schema

Schemas are static and follow the Xquik OpenAPI 3.1 document. The connector
keeps nested `author` and `media` structures instead of flattening them.

### `tweets_search`

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | Yes | Tweet ID |
| `text` | string | Yes | Tweet text |
| `createdAt` | string | No | ISO 8601 creation time |
| `url` | string | No | Tweet permalink |
| `lang` | string | No | Language code |
| `retweetCount` | integer | Yes | Repost count |
| `replyCount` | integer | Yes | Reply count |
| `likeCount` | integer | Yes | Like count |
| `quoteCount` | integer | Yes | Quote count |
| `viewCount` | integer | Yes | View count |
| `bookmarkCount` | integer | Yes | Bookmark count |
| `isReply` | boolean | No | Reply marker |
| `isQuoteStatus` | boolean | No | Quote marker |
| `conversationId` | string | No | Root conversation ID |
| `author` | object | No | Nested `id`, `username`, and `name` |
| `media` | array | No | Nested `type`, `mediaUrl`, `width`, and `height` |
| `search_query` | string | Yes | Connector-added query lineage |

### `user_profiles`

| Field | Type | Required | Notes |
|---|---|---|---|
| `id` | string | Yes | X user ID |
| `username` | string | Yes | Username without `@` |
| `name` | string | Yes | Display name |
| `description` | string | No | Public bio |
| `businessAccountAffiliatesCount` | integer | No | Business-affiliate count |
| `creatorSubscriptionsCount` | integer | No | Creator-subscription count |
| `favouritesCount` | integer | No | Public likes count |
| `followers` | integer | No | Follower count |
| `following` | integer | No | Following count |
| `verified` | boolean | No | Verification marker |
| `isVerified` | boolean | No | Current verification marker |
| `isBlueVerified` | boolean | No | Blue verification marker |
| `hasGraduatedAccess` | boolean | No | Graduated-access marker |
| `hasHiddenSubscriptionsOnProfile` | boolean | No | Hidden-subscriptions marker |
| `highlightsInfo` | object | No | Highlight capability and selected post ID |
| `identityVerification` | object | No | Identity status and verification timestamp |
| `isProfileTranslatable` | boolean | No | Profile translation availability |
| `profilePicture` | string | No | Profile image URL |
| `coverPicture` | string | No | Cover image URL |
| `profileBannerUrl` | string | No | Profile banner URL |
| `profileDescriptionLanguage` | string | No | Detected profile language |
| `profileImageShape` | string | No | Profile image presentation shape |
| `profileInterstitialType` | string | No | Profile interstitial type |
| `profileSortEnabled` | boolean | No | Profile sorting marker |
| `profileTranslatorType` | string | No | Profile translator type |
| `profile_bio` | object | No | Structured description and URL entities |
| `location` | string | No | Public profile location |
| `createdAt` | string | No | ISO 8601 account creation time |
| `mediaCount` | integer | No | Public media-post count |
| `parodyCommentaryFanLabel` | string | No | Account category label |
| `possiblySensitive` | boolean | No | Sensitive-profile marker |
| `statusesCount` | integer | No | Public post count |
| `protected` | boolean | No | Protected-account marker |
| `superFollowEligible` | boolean | No | Subscription eligibility marker |
| `url` | string | No | Public profile URL |
| `verifiedType` | string | No | Verification category |
| `configured_username` | string | Yes | Connector-added request lineage |

### `user_tweets`

This object uses the tweet fields above. It replaces `search_query` with the
connector-added `source_username` field.

### `trends`

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | Yes | Trend name or hashtag |
| `woeid` | integer | Yes | Connector-added regional lineage |
| `description` | string | No | Trend context |
| `query` | string | No | Encoded search query |
| `rank` | integer | No | Regional position |
| `tweetVolume` | integer | No | Reported post volume |
| `url` | string | No | Search URL |

## Get Object Primary Keys

Primary keys are static.

| Object | Primary key |
|---|---|
| `tweets_search` | `id`, `search_query` |
| `user_profiles` | `id` |
| `user_tweets` | `id`, `source_username` |
| `trends` | `woeid`, `name` |

## Object Ingestion Type

All 4 objects use `snapshot` ingestion. Each `read_table` call drains cursor
pages up to `max_pages`, then returns no incremental offset. Snapshot mode is
intentional because search queries, username sets, date bounds, and WOEID sets
are table configuration. A changed configuration defines a changed source
slice. Xquik does not expose a delete feed for these public reads.

Use `since_time` and `until_time` to bound `tweets_search`. Use `since_date`
and `until_date` to bound `user_tweets`. These are source filters, not Lakeflow
incremental offsets.

## Read API for Data Retrieval

### Search Tweets

`GET /x/tweets/search`

| Parameter | Required | Description |
|---|---|---|
| `q` | Yes | Search text, hashtag, username operator, or advanced query |
| `queryType` | No | `Latest` or `Top`; default `Latest` |
| `limit` | No | Requested result limit; connector range 1 to 200 |
| `sinceTime` | No | ISO 8601 lower time bound |
| `untilTime` | No | ISO 8601 upper time bound |
| `cursor` | No | Opaque `next_cursor` from the prior page |

```bash
curl -G "https://xquik.com/api/v1/x/tweets/search" \
  --data-urlencode 'q="open source" lang:en' \
  --data-urlencode 'queryType=Latest' \
  --data-urlencode 'limit=100' \
  -H "x-api-key: xq_YOUR_KEY_HERE"
```

The response contains `tweets`, `has_next_page`, and `next_cursor`.

### User Profiles

`GET /x/users/{username}` accepts a username without `@` or a numeric user ID.
The API returns one profile object. The connector fans out over the
comma-separated `usernames` option and adds `configured_username`.

### User Tweets

`GET /x/users/{username}/tweets`

| Parameter | Required | Description |
|---|---|---|
| `pageSize` | No | Results per page; connector range 1 to 100 |
| `includeReplies` | No | `true` or `false` |
| `sinceDate` | No | `YYYY-MM-DD` lower date bound |
| `untilDate` | No | `YYYY-MM-DD` upper date bound |
| `cursor` | No | Opaque `next_cursor` from the prior page |

The response uses the same tweet page wrapper as search. The connector adds
`source_username` to each record.

### Trends

`GET /x/trends` accepts a supported numeric `woeid` and `count` from 1 to 50.
The response contains `trends`, `count`, and `woeid`. The connector fans out
over the comma-separated `woeids` option and preserves the WOEID on every row.

### Pagination, Errors, and Limits

Follow `next_cursor` only while `has_next_page` is true. Cursors are opaque.
The connector stops at `max_pages` to bound Spark driver work and API cost.

Read requests use a 60-second timeout. The connector retries network errors,
408, 409, 424, 429, and 5xx responses up to 5 attempts. It honors
`Retry-After` up to 60 seconds and otherwise uses exponential backoff.
Authentication, validation, and billing errors are not retried.

Xquik applies read rate limits per account. Current public documentation lists
300 GET requests per second and requires clients to honor `Retry-After`.
Metered reads consume credits. Requested limits are upper bounds when the
remaining balance is insufficient for a full page.

## Field Type Mapping

| Xquik / JSON type | Spark type | Notes |
|---|---|---|
| string | `StringType` | IDs remain strings to avoid overflow |
| integer | `LongType` | Counts and dimensions |
| boolean | `BooleanType` | Verification and tweet markers |
| object | `StructType` | Explicit nested author fields |
| array of objects | `ArrayType(StructType)` | Explicit media fields |
| ISO 8601 date-time string | `StringType` | Cast to timestamp downstream if needed |

## Sources and References

| Source type | URL | Accessed (UTC) | Confidence | Confirmed |
|---|---|---|---|---|
| Official OpenAPI | https://docs.xquik.com/openapi.yaml | 2026-08-18 | Highest | Paths, parameters, schemas, errors |
| Official API overview | https://docs.xquik.com/api-reference/overview | 2026-08-18 | High | Base URL, auth, pagination, retries |
| Official search docs | https://docs.xquik.com/api-reference/x/search-tweets | 2026-08-18 | High | Search filters and tweet response |
| Official profile docs | https://docs.xquik.com/api-reference/x/twitter-profile-lookup | 2026-08-18 | High | Username lookup and profile fields |
| Official timeline docs | https://docs.xquik.com/api-reference/x/user-tweets | 2026-08-18 | High | Timeline filters and cursor pages |
| Official trends docs | https://docs.xquik.com/api-reference/x/trends | 2026-08-18 | High | WOEIDs, count, and trend fields |
| Airbyte implementation | https://github.com/airbytehq/airbyte/pull/84628 | 2026-08-18 | Medium | Independent stream mapping and pagination |

The official OpenAPI contract takes precedence over connector examples if they
conflict. No unresolved documentation gaps remain for the 4 supported objects.

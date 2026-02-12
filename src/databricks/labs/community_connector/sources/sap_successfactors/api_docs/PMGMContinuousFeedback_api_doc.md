# Continuous Feedback API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

**Note:** These APIs are only available with the latest version of Continuous Performance Management (CPM).

## Object List
The following entities/objects are available in this API:

1. **FeedbackRequests** - Feedback request records
2. **Feedback** - Feedback records
3. **UserCapabilities** - User capability information for continuous feedback operations
4. **Recipient** - Feedback recipient information (nested entity)
5. **DefaultQuestions** - Default questions for feedback (function return type)
6. **PendingFeedbackRequests** - Pending feedback requests (function return type)

## Object Schema

### FeedbackRequests

| Field | Type | Required | Filterable | Insertable | Description |
|-------|------|----------|------------|------------|-------------|
| recordId | string (max 255) | Yes | No | No | Record ID of the feedback request (Primary Key) |
| requestId | int64 | No | Yes | No | Feedback request ID |
| topic | string (max 500) | No | No | Yes | Topic of the feedback request |
| question1 | string (max 4000) | No | No | Yes | First question in the feedback request |
| question2 | string (max 250) | No | No | Yes | Second question in the feedback request (nullable) |
| question3 | string (max 250) | No | No | Yes | Third question in the feedback request (nullable) |
| requestText | string (max 4000) | No | Yes | Yes | Message of feedback request (legacy) |
| requestDate | date-time | No | Yes | No | Date the feedback request was created |
| createdAt | date-time | No | No | No | MDF Audit Field - creation timestamp |
| lastModifiedAt | date-time | No | No | No | MDF Audit Field - last modification timestamp |
| createdBy | string (max 255) | No | No | No | User who created the feedback request |
| lastModifiedBy | string (max 255) | No | No | No | User who last modified the feedback request |
| statusId | string (max 250) | No | No | No | Feedback request status ID |
| statusName | string | No | No | No | Feedback request status name |
| declined | boolean | No | No | No | Whether the request has been declined (nullable) |
| requesterAssignmentUUID | string (max 100) | No | Yes | Yes | User assignment UUID of the requester |
| recipientAssignmentUUID | string (max 100) | No | Yes | Yes | User assignment UUID of the recipient |
| subjectUserAssignmentUUID | string (max 100) | No | Yes | Yes | User assignment UUID of the subject |
| requesterDisplayName | string | No | No | No | Display name of the requester |
| requesterDisplayTitle | string | No | No | No | Display title of the requester |
| recipientDisplayName | string | No | No | No | Display name of the recipient |
| recipientDisplayTitle | string | No | No | No | Display title of the recipient |
| subjectDisplayName | string | No | No | No | Display name of the subject |
| subjectDisplayTitle | string | No | No | No | Display title of the subject |
| update_mc | boolean | No | No | No | Whether the request can be updated |
| delete_mc | boolean | No | No | No | Whether the request can be deleted |
| question1_FC | uint8 | No | No | No | Field control for question1 |
| question2_FC | uint8 | No | No | No | Field control for question2 |
| question3_FC | uint8 | No | No | No | Field control for question3 |
| topic_FC | uint8 | No | No | No | Field control for topic |
| declined_FC | uint8 | No | No | No | Field control for declined |
| feedbackResponse_FC | uint8 | No | No | No | Field control for feedbackResponse navigation |
| feedbackResponse | Feedback | No | No | No | Navigation to feedback response |

### Feedback

| Field | Type | Required | Filterable | Description |
|-------|------|----------|------------|-------------|
| recordId | string (max 255) | Yes | Yes | Record ID of the feedback (Primary Key, nullable) |
| feedbackId | int64 | No | Yes | Feedback ID (nullable) |
| topic | string (max 500) | No | Yes | Topic of the feedback (nullable) |
| question1 | string (max 4000) | No | Yes | First question (nullable) |
| question2 | string (max 250) | No | Yes | Second question (nullable) |
| question3 | string (max 250) | No | Yes | Third question (nullable) |
| answer1 | string (max 4000) | No | Yes | Response to first question (nullable) |
| answer2 | string (max 4000) | No | Yes | Response to second question (nullable) |
| answer3 | string (max 4000) | No | Yes | Response to third question (nullable) |
| feedbackMessage | string (max 4000) | No | Yes | Legacy feedback message (nullable) |
| feedbackRequestRecordId | int64 | No | Yes | ID of related feedback request (nullable) |
| dateReceived | date-time | No | Yes | Date feedback was received (nullable) |
| dateModified | date-time | No | Yes | Date feedback was modified (nullable) |
| createdAt | date-time | No | Yes | MDF Audit Field - creation timestamp (nullable) |
| lastModifiedAt | date-time | No | Yes | MDF Audit Field - last modification timestamp (nullable) |
| createdBy | string (max 255) | No | Yes | User who created the feedback (nullable) |
| lastModifiedBy | string (max 255) | No | Yes | User who last modified the feedback (nullable) |
| visibleToManager | boolean | No | Yes | Whether visible to manager (nullable) |
| senderUserAssignmentUUID | string (max 100) | No | Yes | User assignment UUID of sender (nullable) |
| subjectUserAssignmentUUID | string (max 100) | No | Yes | User assignment UUID of subject (nullable) |
| senderDisplayName | string | No | Yes | Display name of sender (nullable) |
| senderDisplayTitle | string | No | Yes | Display title of sender (nullable) |
| subjectDisplayName | string | No | Yes | Display name of subject (nullable) |
| subjectDisplayTitle | string | No | Yes | Display title of subject (nullable) |
| update_mc | boolean | No | Yes | Whether feedback can be updated (nullable) |
| delete_mc | boolean | No | Yes | Whether feedback can be deleted (nullable) |
| question1_FC | uint8 | No | Yes | Field control for question1 (nullable) |
| question2_FC | uint8 | No | Yes | Field control for question2 (nullable) |
| question3_FC | uint8 | No | Yes | Field control for question3 (nullable) |
| answer1_FC | uint8 | No | Yes | Field control for answer1 (nullable) |
| answer2_FC | uint8 | No | Yes | Field control for answer2 (nullable) |
| answer3_FC | uint8 | No | Yes | Field control for answer3 (nullable) |
| topic_FC | uint8 | No | Yes | Field control for topic (nullable) |
| visibleToManager_FC | uint8 | No | Yes | Field control for visibleToManager (nullable) |
| feedbackRequest | FeedbackRequests | No | Yes | Navigation to feedback request (nullable) |
| recipients | array[Recipient] | No | Yes | Navigation to recipients |

### UserCapabilities

| Field | Type | Required | Filterable | Description |
|-------|------|----------|------------|-------------|
| targetUserAssignmentUUID | string | Yes | Yes | User assignment UUID of target user (Primary Key, nullable) |
| canGiveFeedback | boolean | No | Yes | Whether logged-on user can give feedback (nullable) |
| canGiveFeedbackToTargetUser | boolean | No | Yes | Whether logged-on user can give feedback to target (nullable) |
| canRequestFeedbackFromTargetUser | boolean | No | Yes | Whether logged-on user can request feedback from target (nullable) |
| canRequestFeedbackAboutTargetUser | boolean | No | Yes | Whether logged-on user can request feedback about target (nullable) |

### Recipient

| Field | Type | Required | Filterable | Description |
|-------|------|----------|------------|-------------|
| recordId | string (max 255) | Yes | Yes | Unique identifier for recipient (Primary Key, nullable) |
| externalCode | int64 | No | Yes | Alternate identifier (nullable) |
| feedbackRecipientAssignmentUUID | string (max 100) | No | Yes | User assignment UUID of recipient (nullable) |
| parent | string (max 38) | No | Yes | Parent feedback ID (nullable) |
| parentRecordId | string (max 38) | No | Yes | Parent feedback record ID (nullable) |

### DefaultQuestions

| Field | Type | Filterable | Description |
|-------|------|------------|-------------|
| question1 | string | Yes | Default text for first question (nullable) |
| question2 | string | Yes | Default text for second question (nullable) |
| question3 | string | Yes | Default text for third question (nullable) |

## Get Object Primary Keys

| Entity | Primary Key |
|--------|-------------|
| FeedbackRequests | `recordId` |
| Feedback | `recordId` |
| UserCapabilities | `targetUserAssignmentUUID` |
| Recipient | `recordId` |

## Object's Ingestion Type

| Entity | Ingestion Type | Rationale |
|--------|---------------|-----------|
| FeedbackRequests | `cdc` | Supports `$filter` and `$orderby`; has `lastModifiedAt` field |
| Feedback | `cdc` | Supports `$filter` and `$orderby`; has `lastModifiedAt` and `dateModified` fields |
| UserCapabilities | `snapshot` | Read-only capability check, no incremental support |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odatav4/talent/continuousfeedback/v1
```

### Feedback Requests

#### Get All Feedback Requests

**Endpoint:** `GET /feedbackRequests`

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items |
| $skip | integer | Skip the first n items |
| $search | string | Search items by phrases |
| $filter | string | Filter items by property values |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values |
| $select | array | Select properties to return |
| $expand | array | Expand related entities |

**Example Request:**
```http
GET https://{api-server}/odatav4/talent/continuousfeedback/v1/feedbackRequests?$top=50&$orderby=lastModifiedAt desc
Authorization: Basic base64(username@companyId:password)
```

#### Get Feedback Request by Record ID

**Endpoint:** `GET /feedbackRequests('{recordId}')`

**Example Request:**
```http
GET https://{api-server}/odatav4/talent/continuousfeedback/v1/feedbackRequests('7F513CAE66C94D248EC51E57852026C8')?$expand=feedbackResponse
Authorization: Basic base64(username@companyId:password)
```

#### Get Default Questions for Feedback Requests

**Endpoint:** `GET /feedbackRequests/getDefaultQuestions()`

**Example Response:**
```json
{
  "@odata.context": "...",
  "question1": "What went well?",
  "question2": "What could I improve on?",
  "question3": "Any additional comments?"
}
```

#### Get Pending Feedback Requests

**Endpoint:** `GET /feedbackRequests/getMyPendingfeedbackRequests()`

### Feedback

#### Get All Feedback Records

**Endpoint:** `GET /feedback`

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items |
| $skip | integer | Skip the first n items |
| $search | string | Search items by phrases |
| $filter | string | Filter items by property values |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values |
| $select | array | Select properties to return |
| $expand | array | Expand related entities |

**Example Request:**
```http
GET https://{api-server}/odatav4/talent/continuousfeedback/v1/feedback?$top=50&$orderby=dateModified desc
Authorization: Basic base64(username@companyId:password)
```

#### Get Feedback by Record ID

**Endpoint:** `GET /feedback('{recordId}')`

**Example Request:**
```http
GET https://{api-server}/odatav4/talent/continuousfeedback/v1/feedback('CBB104D87A8F4D5AA76D6C85154CCDB0')?$expand=recipients
Authorization: Basic base64(username@companyId:password)
```

#### Get Default Questions for Feedback

**Endpoint:** `GET /feedback/getDefaultQuestions()`

### User Capabilities

#### Get User Capabilities

**Endpoint:** `GET /userCapabilities('{targetUserAssignmentUUID}')`

**Example Request:**
```http
GET https://{api-server}/odatav4/talent/continuousfeedback/v1/userCapabilities('D1BADA3B5AD04CA9ABA29DBC7C83A455')
Authorization: Basic base64(username@companyId:password)
```

### Pagination Approach
- Use `$top` to limit records per request (recommended: 50)
- Use `$skip` to offset for subsequent pages
- Use `$count=true` to get total count

**Example Pagination:**
```http
# First page
GET /feedbackRequests?$top=50&$skip=0&$count=true

# Second page
GET /feedbackRequests?$top=50&$skip=50
```

### Incremental Data Retrieval (CDC)

**For FeedbackRequests:**
```http
GET /feedbackRequests?$filter=lastModifiedAt gt 2024-01-01T00:00:00Z&$orderby=lastModifiedAt asc
```

**For Feedback:**
```http
GET /feedback?$filter=dateModified gt 2024-01-01T00:00:00Z&$orderby=dateModified asc
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |
| date-time (format) | TimestampType |
| integer / int32 | IntegerType |
| int64 | LongType |
| uint8 | IntegerType |
| array | ArrayType |

## Sources and References
- SAP SuccessFactors API Spec: `PMGMContinuousFeedback.json`
- SAP Help Portal: [Continuous Performance Feedback API](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/9f5f060351034d98990213d077dab38a/9984ba036eb04bc7ae72d43cb7dce0d8.html)

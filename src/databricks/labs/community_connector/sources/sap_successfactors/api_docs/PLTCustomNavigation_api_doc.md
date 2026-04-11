# Custom Navigation API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List
The following entities are available in this API:

| Entity Name | Description |
|------------|-------------|
| CustomNav | Custom navigation items added to the Home page |

## Object Schema

### CustomNav
| Field Name | Type | Nullable | Description |
|-----------|------|----------|-------------|
| title | string | No | Title of the navigation item (Primary Key) |
| altText | string | Yes | Alternative text for the navigation item |
| alwaysShow | boolean | Yes | Flag indicating if the item should always be shown |
| isModule | boolean | Yes | Flag indicating if this is a module |
| isSelected | boolean | Yes | Flag indicating if the item is currently selected |
| label | string | Yes | Label text for the navigation item |
| newWindow | boolean | Yes | Flag indicating if the link should open in a new window |
| url | string | Yes | URL for the navigation item |

## Get Object Primary Keys

| Entity | Primary Key Field(s) |
|--------|---------------------|
| CustomNav | title |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| CustomNav | `snapshot` | No timestamp fields (createdDateTime/lastModifiedDateTime) available for incremental filtering |

## Read API for Data Retrieval

### Base URL
```
https://{api-server}/odata/v2
```

### Get All CustomNav Entities
```http
GET /CustomNav
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| $top | integer | Show only the first n items (default: 20) |
| $skip | integer | Skip the first n items |
| $filter | string | Filter items by property values |
| $search | string | Search items by search phrases |
| $count | boolean | Include count of items |
| $orderby | array | Order items by property values |
| $select | array | Select properties to be returned |

**Available $select/$orderby Fields:**
- altText
- alwaysShow
- isModule
- isSelected
- label
- newWindow
- title
- url

### Get Single CustomNav Entity by Key
```http
GET /CustomNav('{title}')
```

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| title | string | Yes | Primary key: title |

### Example Requests

**List all navigation items:**
```http
GET https://{api-server}/odata/v2/CustomNav?$top=100
```

**Get specific navigation item:**
```http
GET https://{api-server}/odata/v2/CustomNav('Home')
```

**Filter navigation items:**
```http
GET https://{api-server}/odata/v2/CustomNav?$filter=alwaysShow eq true
```

**Select specific fields:**
```http
GET https://{api-server}/odata/v2/CustomNav?$select=title,url,label
```

### Pagination
Use `$top` and `$skip` for pagination:
```http
GET /CustomNav?$top=20&$skip=0   # First page
GET /CustomNav?$top=20&$skip=20  # Second page
```

### Response Format
```json
{
  "d": {
    "results": [
      {
        "title": "Custom Link",
        "altText": "Custom Link Alt",
        "alwaysShow": true,
        "isModule": false,
        "isSelected": false,
        "label": "Custom",
        "newWindow": true,
        "url": "https://example.com"
      }
    ]
  }
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| string | StringType |
| boolean | BooleanType |

## Sources and References
- SAP SuccessFactors API Spec: PLTCustomNavigation.json
- SAP Help Portal: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/03e1fc3791684367a6a76a614a2916de.html

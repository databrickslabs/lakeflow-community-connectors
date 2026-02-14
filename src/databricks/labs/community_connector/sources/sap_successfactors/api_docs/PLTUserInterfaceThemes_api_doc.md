# User Interface Themes API Documentation

## Authorization
- Method: Basic Authentication
- Header: `Authorization: Basic base64(username:password)`
- Username format: `username@companyId`

## Object List

The following entities/objects are available in this API:

1. **ThemeTemplate** - Theme Template
2. **ThemeInfo** - Theme Info
3. **ThemeConfig** - Theme Config

## Object Schema

### ThemeTemplate
| Field | Type | Description |
|-------|------|-------------|
| id | String | Primary key - Theme template identifier |
| bodyStyleClass | String (nullable) | CSS body style class |
| footer | String (nullable) | Footer HTML/content |
| header | String (nullable) | Header HTML/content |
| langDir | String (nullable) | Language direction (ltr/rtl) |
| locale | String (nullable) | Locale setting |
| scripts | String (nullable) | JavaScript scripts |
| styles | String (nullable) | CSS styles |
| template | String (nullable) | Template content |
| ui5BaseThemeRootUrl | String (nullable) | UI5 base theme root URL |

### ThemeInfo
| Field | Type | Description |
|-------|------|-------------|
| id | String | Primary key - Theme info identifier |
| fingerprints | ThemeFingerprintsBean | Theme fingerprints (complex type) |
| lastModifiedDate | String (int64, nullable) | Last modification timestamp |
| locale | String (nullable) | Locale setting |
| modules | String (nullable) | Module information |
| ui5Theme | String (nullable) | UI5 theme name |
| urls | ThemeUrlsBean | Theme URLs (complex type) |

### ThemeConfig
| Field | Type | Description |
|-------|------|-------------|
| id | String | Primary key - Theme config identifier |
| themeConfiguration | GlobalThemeConfiguration | Full theme configuration (complex type) |

### Complex Types

#### ThemeFingerprintsBean
| Field | Type | Description |
|-------|------|-------------|
| config | String (nullable) | Configuration fingerprint |
| css | String (nullable) | CSS fingerprint |
| ui5BaseThemeRoot | String (nullable) | UI5 base theme root fingerprint |

#### ThemeUrlsBean
| Field | Type | Description |
|-------|------|-------------|
| baseUrl | String (nullable) | Base URL |
| configUrl | String (nullable) | Configuration URL |
| cssUrl | String (nullable) | CSS URL |
| ui5BaseThemeRootUrl | String (nullable) | UI5 base theme root URL |

#### GlobalThemeConfiguration
Contains comprehensive theme settings including:
- **accentColorBase** - Accent color configuration
- **background** - Background configuration (ThemeBackgroundConfig)
- **content** - Content area configuration (ThemeContentConfig)
- **diagram** - Diagram/chart configuration (ThemeDiagramConfig)
- **external** - External assets configuration (ThemeExternalConfig)
- **footer** - Footer configuration (ThemeFooterConfig)
- **headerBackground** - Header background configuration
- **highlight** - Highlight configuration (ThemeHighlightConfig)
- **login** - Login page configuration (ThemeLoginConfig)
- **logo** - Logo configuration (ThemeLogoConfig)
- **menu** - Menu configuration (ThemeMenuConfig)
- **navigation** - Navigation configuration (ThemeNavigationConfig)
- **placemat** - Placemat configuration (ThemePlacematConfig)
- **portlet** - Portlet configuration (ThemePortletConfig)
- **primaryButton/secondaryButton** - Button configurations (ThemeButtonConfig)
- **table** - Table configuration (ThemeTableConfig)
- **tile** - Tile configuration (ThemeTileConfig)
- **landingPage** - Landing page configuration
- **Various boolean flags** - useContainerShadowColor, useDownloadableFonts, useModernStylesInLegacy, useTextShadow

## Get Object Primary Keys

| Entity | Primary Key Field(s) | Type |
|--------|---------------------|------|
| ThemeTemplate | id | String |
| ThemeInfo | id | String |
| ThemeConfig | id | String |

## Object's Ingestion Type

| Entity | Ingestion Type | Reason |
|--------|---------------|--------|
| ThemeTemplate | `snapshot` | No standard lastModifiedDateTime field for filtering |
| ThemeInfo | `cdc` | Has `lastModifiedDate` field (int64 timestamp) that can be used for incremental filtering |
| ThemeConfig | `snapshot` | No lastModifiedDateTime field available for incremental filtering |

**Note:** ThemeInfo's `lastModifiedDate` is stored as an int64 (Unix timestamp in milliseconds), not a standard OData DateTime. This may require special handling for CDC operations.

## Read API for Data Retrieval

### Base URL Pattern
```
https://{api-server}/odata/v2/{EntitySet}
```

### Supported Query Parameters
| Parameter | Description |
|-----------|-------------|
| $select | Select specific properties to return |
| $filter | Filter items by property values |
| $top | Limit number of items returned (default: 20) |
| $skip | Skip first n items for pagination |
| $orderby | Order items by property values |
| $count | Include count of items |
| $search | Search items by search phrases |

### Available $orderby Fields

**ThemeTemplate:**
- bodyStyleClass, footer, header, id, langDir, locale, scripts, styles, template, ui5BaseThemeRootUrl

**ThemeInfo:**
- fingerprints, id, lastModifiedDate, locale, modules, ui5Theme, urls

**ThemeConfig:**
- id, themeConfiguration

### Example Requests

**Get all Theme Templates with pagination:**
```http
GET https://{api-server}/odata/v2/ThemeTemplate?$top=100&$skip=0
```

**Get Theme Info ordered by last modified date (CDC):**
```http
GET https://{api-server}/odata/v2/ThemeInfo?$orderby=lastModifiedDate desc
```

**Get ThemeInfo modified after a specific timestamp (CDC):**
```http
GET https://{api-server}/odata/v2/ThemeInfo?$filter=lastModifiedDate gt 1704067200000&$orderby=lastModifiedDate asc
```

**Get a specific theme template by ID:**
```http
GET https://{api-server}/odata/v2/ThemeTemplate('default-theme')
```

**Get theme info with selected fields:**
```http
GET https://{api-server}/odata/v2/ThemeInfo?$select=id,locale,ui5Theme,lastModifiedDate
```

**Get all theme configurations:**
```http
GET https://{api-server}/odata/v2/ThemeConfig
```

**Get a specific theme configuration by ID:**
```http
GET https://{api-server}/odata/v2/ThemeConfig('company-theme')
```

**Get theme templates for a specific locale:**
```http
GET https://{api-server}/odata/v2/ThemeTemplate?$filter=locale eq 'en_US'
```

### Pagination Approach
- Use `$top` and `$skip` for offset-based pagination
- Default page size is 20 items
- Increment `$skip` by page size for subsequent requests
- For CDC with ThemeInfo: filter by `lastModifiedDate` (Unix timestamp in milliseconds)

### Response Structure

**ThemeTemplate:**
```json
{
  "d": {
    "results": [
      {
        "id": "default-theme",
        "bodyStyleClass": "sap-theme-default",
        "locale": "en_US",
        "langDir": "ltr",
        "styles": "<link rel='stylesheet' href='...'/>",
        ...
      }
    ]
  }
}
```

**ThemeInfo:**
```json
{
  "d": {
    "results": [
      {
        "id": "company-theme",
        "lastModifiedDate": "1704067200000",
        "locale": "en_US",
        "ui5Theme": "sap_belize",
        "fingerprints": {
          "config": "abc123",
          "css": "def456"
        },
        "urls": {
          "baseUrl": "https://...",
          "cssUrl": "https://..."
        }
      }
    ]
  }
}
```

**ThemeConfig:**
```json
{
  "d": {
    "results": [
      {
        "id": "company-theme",
        "themeConfiguration": {
          "accentColorBase": { "value": "#0854a0" },
          "background": {
            "baseColor": { "value": "#f7f7f7" },
            "lighting": "light"
          },
          "primaryButton": {
            "bgColor": { "value": "#0854a0" },
            "textColor": { "value": "#ffffff" }
          },
          ...
        }
      }
    ]
  }
}
```

## Field Type Mapping

| API Type | Spark Type |
|----------|------------|
| Edm.String | StringType |
| Edm.Int32 | IntegerType |
| Edm.Int64 | LongType |
| Edm.Boolean | BooleanType |
| Edm.DateTime | TimestampType |
| Edm.DateTimeOffset | TimestampType |
| Edm.Decimal | DecimalType |
| Edm.Double | DoubleType |

**Note:** Complex types (ThemeFingerprintsBean, ThemeUrlsBean, GlobalThemeConfiguration, etc.) should be mapped to StringType (JSON serialized) or StructType in Spark.

## Sources and References
- SAP SuccessFactors API Spec: PLTUserInterfaceThemes.json
- SAP Help Portal: [User Interface Themes on SAP Help Portal](https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/d8c5d8e76fb840a3a7d159fcf21ac467.html)

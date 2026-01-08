Klaviyo is a marketing automation platform for email and SMS, and its API lets you retrieve a wide range of marketing data. Here's what you can download:

## Data You Can Retrieve

| Category | What You Can Get |
|----------|------------------|
| **Profiles** | Customer/subscriber data, custom properties, email addresses, phone numbers, consent status |
| **Lists & Segments** | List/segment definitions, member profiles |
| **Events** | Customer actions (purchases, email opens, clicks, page views, custom events) |
| **Metrics** | All tracked event types in your account |
| **Campaigns** | Campaign details, performance data, recipient info |
| **Flows** | Automated flow definitions, messages, performance |
| **Templates** | Email template content and metadata |
| **Catalogs** | Product catalog data |
| **Forms** | Signup form data and performance |
| **Tags** | Organizational tags and tagged items |

## Authentication

Klaviyo provides 3 methods of authentication: private key authentication and OAuth for server-side APIs, and public key authentication for client-side APIs.

```
Authorization: Klaviyo-API-Key your-private-api-key
```

## Rate Limits

All new API endpoints are rate limited on a per-account basis using a fixed-window algorithm with burst (short) and steady (long) windows. Exceeding limits returns HTTP 429 errors.

## Key Considerations

- Use cursor-based pagination for large exports
- API keys require specific scopes (e.g., `profiles:read`, `campaigns:read`)
- Historical event data is available without retention limits
- The API uses JSON:API format with relationships between objects

## Documentation

**https://developers.klaviyo.com**

From there you can access:

- **API Reference**: https://developers.klaviyo.com/en/reference/api_overview
- **Guides & Tutorials**: https://developers.klaviyo.com/en/docs
- **OpenAPI Spec**: Available for import into Postman or other tools
- **Postman Collection**: https://www.postman.com/klaviyo

They also have a help center with additional guidance at https://help.klaviyo.com.
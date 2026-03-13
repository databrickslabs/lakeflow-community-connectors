# Terna connector test configuration

Tests use real credentials and hit the Terna Public API.

1. Copy `dev_config.json` and fill in your Terna API credentials:
   - **client_id**: OAuth 2.0 Client Credentials application key (required).
   - **client_secret**: OAuth 2.0 Client Credentials secret (required).
   - **base_url**: (optional) Default is `https://api.terna.it`.
   - **x_api_key**: (optional) For Physical Foreign Flow if that endpoint uses x-api-key.

2. Obtain credentials from [Terna Open Data](https://opendata.terna.it/) (register for API access).

3. `dev_table_config.json` defines date ranges per table (format `dd/mm/yyyy`). **date_from** is required; **date_to** is optional and defaults to current execution time. Ranges longer than 60 days are split into multiple API calls (60 days per request). API allows up to 5 years of history.

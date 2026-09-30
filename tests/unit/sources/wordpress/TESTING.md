# Testing the WordPress connector

Two test postures:

- **Simulate mode (default)** — offline, no credentials, no network. Runs against
  the checked-in spec + corpus under `source_simulator/specs/wordpress/`. This is
  what CI runs.
- **Live / record mode** — runs the connector against a real WordPress site to
  validate that the simulator spec still matches reality (drift detection).

This guide shows how to stand up a throwaway WordPress site, enable REST API
access, point the connector at it, and run both modes. Two ways to get a site:

| | TasteWP (Option A) | Local Docker (Option B) |
|---|---|---|
| Cost | free (temp site, expires ~2 days) | free |
| URL | real public **HTTPS** | `http://localhost:8080` |
| App Passwords | works (HTTPS) | works via `WP_ENVIRONMENT_TYPE=local` over HTTP |
| Best for | a quick public datapoint | offline / repeatable local runs |

---

## Run simulate mode (no setup)

```bash
pytest tests/unit/sources/wordpress/ -q
# -> 20 passed, 1 skipped
```

Everything below is only needed for **live / record mode**.

---

## Option A — TasteWP (free public HTTPS site)

### 1. Create the site

Open <https://tastewp.com/new/>. It provisions a WordPress site in a few seconds,
drops you into `wp-admin`, and shows an admin **username + password**. Note the
site URL, e.g. `https://loosecomb.s3-tastewp.com`. (No login needed; the site
lives ~2 days. Sign in first for ~7 days.)

### 2. Make permalinks "pretty"

`wp-admin` → **Settings → Permalinks → "Post name" → Save**. Without this the REST
API 301-redirects to a trailing slash on every call (harmless — `requests`
follows it — but cleaner to avoid).

### 3. Get an Application Password

> The admin **login password is NOT an Application Password**. Using it for REST
> Basic Auth returns `401 rest_not_logged_in`.

Normally: `wp-admin` → **Users → Profile** → **Application Passwords** → name it
`lakeflow-connector` → **Add New** → copy the 6-group secret
(`abcd EFGH ijkl MNOP qrst UVWX`).

**If that section is missing** (TasteWP builds sometimes hide the profile UI even
though the feature is on), use the core authorization flow instead — while signed
in to `wp-admin`:

```
https://loosecomb.s3-tastewp.com/wp-admin/authorize-application.php?app_name=lakeflow-connector
```

Click **Yes, I approve** → copy the password shown. Confirm the feature is enabled
first if unsure:

```bash
curl -s https://loosecomb.s3-tastewp.com/wp-json/ | python3 -c \
  "import sys,json; print('application-passwords' in json.load(sys.stdin).get('authentication',{}))"
# -> True
```

### 4. Verify API access

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  --user "admin:abcd EFGH ijkl MNOP qrst UVWX" \
  https://loosecomb.s3-tastewp.com/wp-json/wp/v2/users/me
# -> 200   (401 means wrong/again-a-login password)
```

### 5. (Optional) seed the empty tables

A fresh TasteWP site has a default post/page/comment/category but **no media or
tags**, so those two connector tables read empty. Seed them via the REST API:

```bash
AUTH="admin:abcd EFGH ijkl MNOP qrst UVWX"
BASE="https://loosecomb.s3-tastewp.com/wp-json/wp/v2"

curl -s -X POST --user "$AUTH" -H "Content-Type: application/json" \
  -d '{"name":"lakeflow","slug":"lakeflow"}' "$BASE/tags"

printf 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==' \
  | base64 -d > /tmp/pixel.png
curl -s -X POST --user "$AUTH" \
  -H "Content-Disposition: attachment; filename=pixel.png" -H "Content-Type: image/png" \
  --data-binary @/tmp/pixel.png "$BASE/media"
```

Now jump to [Point the connector at the site](#point-the-connector-at-the-site).

---

## Option B — Local Docker (free, offline)

### 1. `docker-compose.yml`

```yaml
services:
  db:
    image: mariadb:11
    environment:
      MARIADB_ROOT_PASSWORD: rootpw
      MARIADB_DATABASE: wordpress
      MARIADB_USER: wordpress
      MARIADB_PASSWORD: wordpress
    volumes: [db_data:/var/lib/mysql]
    healthcheck:
      test: ["CMD", "healthcheck.sh", "--connect", "--innodb_initialized"]
      interval: 5s
      timeout: 5s
      retries: 30

  wordpress:
    image: wordpress:latest
    depends_on:
      db: { condition: service_healthy }
    ports: ["8080:80"]
    environment:
      WORDPRESS_DB_HOST: db:3306
      WORDPRESS_DB_USER: wordpress
      WORDPRESS_DB_PASSWORD: wordpress
      WORDPRESS_DB_NAME: wordpress
      # THE key line: makes Application Passwords available over plain HTTP.
      WORDPRESS_CONFIG_EXTRA: |
        define('WP_ENVIRONMENT_TYPE', 'local');
    volumes: [wp_data:/var/www/html]

  # One-shot WP-CLI runner. user 33:33 matches the Debian wordpress image's
  # www-data uid so WP-CLI can write wp-content/uploads. (The cli image is
  # Alpine/uid 82 by default, which otherwise can't write the volume files.)
  wpcli:
    image: wordpress:cli
    user: "33:33"
    profiles: ["tools"]
    depends_on: [wordpress]
    environment:
      WORDPRESS_DB_HOST: db:3306
      WORDPRESS_DB_USER: wordpress
      WORDPRESS_DB_PASSWORD: wordpress
      WORDPRESS_DB_NAME: wordpress
    volumes: [wp_data:/var/www/html]

volumes:
  db_data:
  wp_data:
```

### 2. Bring it up and install WordPress

```bash
docker-compose up -d db wordpress
# wait until the front page responds (files copied + DB reachable):
until [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/)" != "000" ]; do sleep 1; done

wp() { docker-compose run --rm -T wpcli wp "$@"; }
wp core install --url=http://localhost:8080 --title="Lakeflow WP Test" \
  --admin_user=admin --admin_password=admin_pw_123 --admin_email=admin@example.com --skip-email
wp rewrite structure '/%postname%/' --hard && wp rewrite flush --hard
```

### 3. Seed every table + mint an Application Password

```bash
# uploads dir must exist and be writable (uid 33) for media import:
docker-compose run --rm -T -u root wpcli \
  sh -c 'mkdir -p /var/www/html/wp-content/uploads && chown -R 33:33 /var/www/html/wp-content/uploads'

wp term create category News --slug=news
wp term create post_tag lakeflow --slug=lakeflow
P1=$(wp post create --post_type=post --post_status=publish --post_title="First Post" --post_content="hi" --porcelain)
wp post create --post_type=page --post_status=publish --post_title="About" --post_content="about" --porcelain
wp comment create --comment_post_ID="$P1" --comment_content="Nice" --comment_author="Reader" \
  --comment_author_email="r@example.com" --comment_approved=1 --porcelain

printf 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==' \
  | base64 -d > /tmp/pixel.png
docker cp /tmp/pixel.png "$(docker-compose ps -q wordpress)":/tmp/pixel.png
wp media import /tmp/pixel.png --title="Test Pixel" --porcelain

# Application Password (WP_ENVIRONMENT_TYPE=local lets this work over HTTP):
wp user application-password create admin lakeflow-connector --porcelain
# -> prints a 24-char password, e.g. Dhaqkr3B7aRjMFr3ToCEgOlt
```

Tear down with `docker-compose down -v`.

---

## Point the connector at the site

Write a config JSON anywhere (the path is passed via env var). Keys map directly
to the connector's connection options.

TasteWP:
```json
{
  "base_url": "https://loosecomb.s3-tastewp.com",
  "username": "admin",
  "application_password": "abcd EFGH ijkl MNOP qrst UVWX"
}
```

Local Docker:
```json
{
  "base_url": "http://localhost:8080",
  "username": "admin",
  "application_password": "Dhaqkr3B7aRjMFr3ToCEgOlt"
}
```

---

## Run live / record mode

Live mode hits the real site, records a cassette, and **diffs each live response
against the spec + corpus** (drift detection). It is read-only on the committed
corpus — it never overwrites it.

```bash
CONNECTOR_TEST_MODE=live \
CONNECTOR_TEST_CONFIG_PATH=/absolute/path/to/config.json \
  pytest tests/unit/sources/wordpress/ -q
# -> 18 passed, 3 skipped
```

> Use an **absolute** path — `~` is not expanded. `CONNECTOR_TEST_MODE=record` is
> an accepted alias for `live`.

Artifacts are written next to the test (all gitignored):

```
tests/unit/sources/wordpress/cassettes/TestWordPressConnector.json
                                        .json.coverage.json
                                        .json.validation.json
```

### Interpreting the validation report

```bash
python3 -c "import json; d=json.load(open(
  'tests/unit/sources/wordpress/cassettes/TestWordPressConnector.json.validation.json'));
print(json.dumps(d['summary']))
[print(e['spec_path'].split('/')[-1], e.get('issues')) for e in d['endpoints']]"
```

Expected drift against a stock WordPress site is **benign** — live returns fields
the connector intentionally does not model:

- `_links` (HATEOAS) and `meta` (free-form) — omitted by design on every endpoint.
- `class_list`, and on media `featured_media` / `filename` / `filesize` — not modeled.
- `taxonomies`: `live=dict spec=list` — the connector handles both shapes
  (`_read_dict_shaped`).

None of these are defects: no field the connector **does** model is missing or
mistyped. A *real* drift signal would be a modeled field absent from live, a type
mismatch, or a `no_spec` endpoint — investigate those.

### Optional: quick standalone read (no pytest)

To sanity-check connectivity without the suite, instantiate the connector and read
each table directly:

```bash
PYTHONPATH=src python3 - <<'PY'
import itertools, json
from databricks.labs.community_connector.sources.wordpress.wordpress import WordPressLakeflowConnect
conn = WordPressLakeflowConnect(json.load(open("/absolute/path/to/config.json")))
for t in conn.list_tables():
    if conn.is_partitioned(t):
        parts = conn.get_partitions(t, {}, end_offset=conn.latest_offset(t, {}))
        rows = list(itertools.islice(
            itertools.chain.from_iterable(conn.read_partition(t, p, {}) for p in parts), 3))
    else:
        recs, _ = conn.read_table(t, {}, {})
        rows = list(itertools.islice(recs, 3))
    print(f"{t:<12} {len(rows)} row(s)")
PY
```

---

## Gotchas

| Symptom | Cause / fix |
|---|---|
| `401 rest_not_logged_in` | Used the login password, not an Application Password. |
| Application Passwords section missing in profile | Use `authorize-application.php?app_name=...` (Option A step 3). |
| `401/403` on a remote site | Application Passwords require **HTTPS** (or `WP_ENVIRONMENT_TYPE=local` for localhost). |
| REST calls 301-redirect | Set pretty permalinks (`wp rewrite structure '/%postname%/'`). |
| `media import` "Unable to create directory ... uploads" | uid mismatch — pin the cli service to `user: "33:33"` and `chown -R 33:33` the uploads dir. |
| `CONNECTOR_TEST_CONFIG_PATH points to a non-existent file` | Pass an absolute path; `~` is not expanded. |

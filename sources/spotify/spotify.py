import requests
import base64
import time
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
    MapType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Spotify connector with connection-level options.

        Expected options:
            - client_id: Spotify application client ID
            - client_secret: Spotify application client secret
            - refresh_token: OAuth refresh token for user data access
            - base_url (optional): Override for Spotify API base URL. Defaults to https://api.spotify.com/v1
        """
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        refresh_token = options.get("refresh_token")

        if not client_id or not client_secret:
            raise ValueError(
                "Spotify connector requires 'client_id' and 'client_secret' in options"
            )

        if not refresh_token:
            raise ValueError(
                "Spotify connector requires 'refresh_token' in options for user data access"
            )

        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.base_url = options.get("base_url", "https://api.spotify.com/v1").rstrip(
            "/"
        )
        self.token_url = "https://accounts.spotify.com/api/token"

        # Access token will be fetched on demand
        self._access_token: str | None = None
        self._token_expires_at: float = 0

        # Configure a session for API requests
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        """
        current_time = time.time()

        # If we have a valid token that's not about to expire (with 60s buffer), use it
        if self._access_token and current_time < (self._token_expires_at - 60):
            return self._access_token

        # Refresh the token
        auth_header = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        response = requests.post(
            self.token_url,
            headers={
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=30,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to refresh Spotify access token: {response.status_code} {response.text}"
            )

        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self._token_expires_at = current_time + expires_in

        return self._access_token

    def _make_request(
        self, url: str, params: dict | None = None, retry_count: int = 3
    ) -> dict:
        """
        Make an authenticated request to the Spotify API with retry logic for rate limiting.
        """
        for attempt in range(retry_count):
            token = self._get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            response = self._session.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                # Rate limited - check Retry-After header
                retry_after = int(response.headers.get("Retry-After", 5))
                if attempt < retry_count - 1:
                    time.sleep(retry_after)
                    continue

            if response.status_code == 401:
                # Token might be invalid, force refresh on next attempt
                self._access_token = None
                self._token_expires_at = 0
                if attempt < retry_count - 1:
                    continue

            raise RuntimeError(
                f"Spotify API error: {response.status_code} {response.text}"
            )

        raise RuntimeError("Spotify API request failed after retries")

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return [
            "tracks",
            "albums",
            "playlists",
            "artists",
            "top_tracks",
            "top_artists",
            "recently_played",
            "user_profile",
        ]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the Spotify Web API documentation.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        # Common nested struct schemas
        external_urls_struct = StructType(
            [
                StructField("spotify", StringType(), True),
            ]
        )

        image_struct = StructType(
            [
                StructField("url", StringType(), True),
                StructField("height", LongType(), True),
                StructField("width", LongType(), True),
            ]
        )

        followers_struct = StructType(
            [
                StructField("href", StringType(), True),
                StructField("total", LongType(), True),
            ]
        )

        # Simplified artist struct (for nested use in tracks/albums)
        simple_artist_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("uri", StringType(), True),
                StructField("href", StringType(), True),
                StructField("external_urls", external_urls_struct, True),
            ]
        )

        # Full artist struct (for artists table)
        artist_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("uri", StringType(), True),
                StructField("href", StringType(), True),
                StructField("popularity", LongType(), True),
                StructField("genres", ArrayType(StringType(), True), True),
                StructField("images", ArrayType(image_struct, True), True),
                StructField("followers", followers_struct, True),
                StructField("external_urls", external_urls_struct, True),
            ]
        )

        # Album struct (simplified for track context)
        simple_album_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("uri", StringType(), True),
                StructField("href", StringType(), True),
                StructField("album_type", StringType(), True),
                StructField("total_tracks", LongType(), True),
                StructField("release_date", StringType(), True),
                StructField("release_date_precision", StringType(), True),
                StructField("images", ArrayType(image_struct, True), True),
                StructField("artists", ArrayType(simple_artist_struct, True), True),
                StructField("external_urls", external_urls_struct, True),
            ]
        )

        # External IDs struct
        external_ids_struct = StructType(
            [
                StructField("isrc", StringType(), True),
                StructField("ean", StringType(), True),
                StructField("upc", StringType(), True),
            ]
        )

        # Track struct
        track_struct = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("uri", StringType(), True),
                StructField("href", StringType(), True),
                StructField("disc_number", LongType(), True),
                StructField("track_number", LongType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("explicit", BooleanType(), True),
                StructField("is_local", BooleanType(), True),
                StructField("popularity", LongType(), True),
                StructField("preview_url", StringType(), True),
                StructField("external_urls", external_urls_struct, True),
                StructField("external_ids", external_ids_struct, True),
                StructField("album", simple_album_struct, True),
                StructField("artists", ArrayType(simple_artist_struct, True), True),
            ]
        )

        if table_name == "tracks":
            return StructType(
                [
                    StructField("added_at", StringType(), True),
                    StructField("track", track_struct, True),
                ]
            )

        if table_name == "albums":
            # Full album struct with additional fields
            copyright_struct = StructType(
                [
                    StructField("text", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            )

            full_album_struct = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("album_type", StringType(), True),
                    StructField("total_tracks", LongType(), True),
                    StructField("release_date", StringType(), True),
                    StructField("release_date_precision", StringType(), True),
                    StructField("label", StringType(), True),
                    StructField("popularity", LongType(), True),
                    StructField("genres", ArrayType(StringType(), True), True),
                    StructField("images", ArrayType(image_struct, True), True),
                    StructField("artists", ArrayType(simple_artist_struct, True), True),
                    StructField("copyrights", ArrayType(copyright_struct, True), True),
                    StructField("external_ids", external_ids_struct, True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

            return StructType(
                [
                    StructField("added_at", StringType(), True),
                    StructField("album", full_album_struct, True),
                ]
            )

        if table_name == "playlists":
            owner_struct = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("display_name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

            tracks_ref_struct = StructType(
                [
                    StructField("href", StringType(), True),
                    StructField("total", LongType(), True),
                ]
            )

            return StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("public", BooleanType(), True),
                    StructField("collaborative", BooleanType(), True),
                    StructField("snapshot_id", StringType(), True),
                    StructField("owner", owner_struct, True),
                    StructField("images", ArrayType(image_struct, True), True),
                    StructField("tracks", tracks_ref_struct, True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

        if table_name == "artists":
            return artist_struct

        if table_name == "top_tracks":
            # Same as track struct with added time_range
            return StructType(
                [
                    StructField("time_range", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("disc_number", LongType(), True),
                    StructField("track_number", LongType(), True),
                    StructField("duration_ms", LongType(), True),
                    StructField("explicit", BooleanType(), True),
                    StructField("is_local", BooleanType(), True),
                    StructField("popularity", LongType(), True),
                    StructField("preview_url", StringType(), True),
                    StructField("external_urls", external_urls_struct, True),
                    StructField("external_ids", external_ids_struct, True),
                    StructField("album", simple_album_struct, True),
                    StructField("artists", ArrayType(simple_artist_struct, True), True),
                ]
            )

        if table_name == "top_artists":
            # Same as artist struct with added time_range
            return StructType(
                [
                    StructField("time_range", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("popularity", LongType(), True),
                    StructField("genres", ArrayType(StringType(), True), True),
                    StructField("images", ArrayType(image_struct, True), True),
                    StructField("followers", followers_struct, True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

        if table_name == "recently_played":
            context_struct = StructType(
                [
                    StructField("type", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

            return StructType(
                [
                    StructField("played_at", StringType(), True),
                    StructField("track", track_struct, True),
                    StructField("context", context_struct, True),
                ]
            )

        if table_name == "user_profile":
            explicit_content_struct = StructType(
                [
                    StructField("filter_enabled", BooleanType(), True),
                    StructField("filter_locked", BooleanType(), True),
                ]
            )

            return StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("display_name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("href", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("product", StringType(), True),
                    StructField("explicit_content", explicit_content_struct, True),
                    StructField("followers", followers_struct, True),
                    StructField("images", ArrayType(image_struct, True), True),
                    StructField("external_urls", external_urls_struct, True),
                ]
            )

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "tracks":
            return {
                "primary_keys": ["track.id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "albums":
            return {
                "primary_keys": ["album.id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "playlists":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "artists":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        if table_name == "top_tracks":
            return {
                "primary_keys": ["id", "time_range"],
                "ingestion_type": "snapshot",
            }

        if table_name == "top_artists":
            return {
                "primary_keys": ["id", "time_range"],
                "ingestion_type": "snapshot",
            }

        if table_name == "recently_played":
            return {
                "primary_keys": ["played_at", "track.id"],
                "cursor_field": "played_at",
                "ingestion_type": "cdc",
            }

        if table_name == "user_profile":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "tracks":
            return self._read_saved_tracks(start_offset, table_options)

        if table_name == "albums":
            return self._read_saved_albums(start_offset, table_options)

        if table_name == "playlists":
            return self._read_playlists(start_offset, table_options)

        if table_name == "artists":
            return self._read_followed_artists(start_offset, table_options)

        if table_name == "top_tracks":
            return self._read_top_tracks(start_offset, table_options)

        if table_name == "top_artists":
            return self._read_top_artists(start_offset, table_options)

        if table_name == "recently_played":
            return self._read_recently_played(start_offset, table_options)

        if table_name == "user_profile":
            return self._read_user_profile(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_saved_tracks(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's saved tracks using GET /me/tracks.
        Uses offset-based pagination.
        """
        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/tracks"
        records: list[dict[str, Any]] = []
        offset = 0
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"limit": limit, "offset": offset}
            response_data = self._make_request(url, params)

            items = response_data.get("items", [])
            if not items:
                break

            for item in items:
                records.append(item)

            # Check if there are more pages
            if response_data.get("next") is None:
                break

            offset += limit
            pages_fetched += 1

        return iter(records), {}

    def _read_saved_albums(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's saved albums using GET /me/albums.
        """
        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/albums"
        records: list[dict[str, Any]] = []
        offset = 0
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"limit": limit, "offset": offset}
            response_data = self._make_request(url, params)

            items = response_data.get("items", [])
            if not items:
                break

            for item in items:
                records.append(item)

            if response_data.get("next") is None:
                break

            offset += limit
            pages_fetched += 1

        return iter(records), {}

    def _read_playlists(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's playlists using GET /me/playlists.
        """
        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/playlists"
        records: list[dict[str, Any]] = []
        offset = 0
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"limit": limit, "offset": offset}
            response_data = self._make_request(url, params)

            items = response_data.get("items", [])
            if not items:
                break

            for item in items:
                records.append(item)

            if response_data.get("next") is None:
                break

            offset += limit
            pages_fetched += 1

        return iter(records), {}

    def _read_followed_artists(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's followed artists using GET /me/following?type=artist.
        Uses cursor-based pagination with 'after' parameter.
        """
        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/following"
        records: list[dict[str, Any]] = []
        after_cursor: str | None = None
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"type": "artist", "limit": limit}
            if after_cursor:
                params["after"] = after_cursor

            response_data = self._make_request(url, params)

            artists_data = response_data.get("artists", {})
            items = artists_data.get("items", [])

            if not items:
                break

            for item in items:
                records.append(item)

            # Get cursor for next page
            cursors = artists_data.get("cursors", {})
            after_cursor = cursors.get("after")

            if not after_cursor:
                break

            pages_fetched += 1

        return iter(records), {}

    def _read_top_tracks(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's top tracks using GET /me/top/tracks.
        """
        time_range = table_options.get("time_range", "medium_term")
        if time_range not in ["short_term", "medium_term", "long_term"]:
            time_range = "medium_term"

        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/top/tracks"
        records: list[dict[str, Any]] = []
        offset = 0
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"time_range": time_range, "limit": limit, "offset": offset}
            response_data = self._make_request(url, params)

            items = response_data.get("items", [])
            if not items:
                break

            for item in items:
                # Add time_range as connector-derived field
                record = dict(item)
                record["time_range"] = time_range
                records.append(record)

            if response_data.get("next") is None:
                break

            offset += limit
            pages_fetched += 1

        return iter(records), {}

    def _read_top_artists(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's top artists using GET /me/top/artists.
        """
        time_range = table_options.get("time_range", "medium_term")
        if time_range not in ["short_term", "medium_term", "long_term"]:
            time_range = "medium_term"

        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        try:
            max_pages = int(table_options.get("max_pages", 100))
        except (TypeError, ValueError):
            max_pages = 100

        url = f"{self.base_url}/me/top/artists"
        records: list[dict[str, Any]] = []
        offset = 0
        pages_fetched = 0

        while pages_fetched < max_pages:
            params = {"time_range": time_range, "limit": limit, "offset": offset}
            response_data = self._make_request(url, params)

            items = response_data.get("items", [])
            if not items:
                break

            for item in items:
                # Add time_range as connector-derived field
                record = dict(item)
                record["time_range"] = time_range
                records.append(record)

            if response_data.get("next") is None:
                break

            offset += limit
            pages_fetched += 1

        return iter(records), {}

    def _read_recently_played(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user's recently played tracks using GET /me/player/recently-played.
        Supports incremental reads using 'after' timestamp cursor.
        """
        try:
            limit = int(table_options.get("limit", 50))
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 50))

        # Get cursor from start_offset if available
        after_timestamp: int | None = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
            if cursor:
                try:
                    after_timestamp = int(cursor)
                except (TypeError, ValueError):
                    pass

        url = f"{self.base_url}/me/player/recently-played"
        params = {"limit": limit}

        if after_timestamp:
            params["after"] = after_timestamp

        records: list[dict[str, Any]] = []
        max_played_at: str | None = None

        response_data = self._make_request(url, params)

        items = response_data.get("items", [])
        for item in items:
            records.append(item)

            played_at = item.get("played_at")
            if isinstance(played_at, str):
                if max_played_at is None or played_at > max_played_at:
                    max_played_at = played_at

        # Calculate next cursor from max played_at
        next_cursor = None
        if max_played_at:
            # Convert ISO 8601 to Unix timestamp in milliseconds
            try:
                from datetime import datetime

                dt = datetime.strptime(max_played_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                next_cursor = int(dt.timestamp() * 1000)
            except ValueError:
                try:
                    dt = datetime.strptime(max_played_at, "%Y-%m-%dT%H:%M:%SZ")
                    next_cursor = int(dt.timestamp() * 1000)
                except ValueError:
                    pass

        if not records and start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": str(next_cursor)} if next_cursor else {}

        return iter(records), next_offset

    def _read_user_profile(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read current user's profile using GET /me.
        Returns a single record.
        """
        url = f"{self.base_url}/me"
        user_data = self._make_request(url)

        return iter([user_data]), {}


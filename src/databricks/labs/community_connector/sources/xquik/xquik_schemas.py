"""Spark schemas and ingestion metadata for the Xquik connector."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SUPPORTED_TABLES = ["tweets_search", "user_profiles", "user_tweets", "trends"]

AUTHOR_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("username", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
    ]
)

MEDIA_SCHEMA = StructType(
    [
        StructField("type", StringType(), nullable=True),
        StructField("mediaUrl", StringType(), nullable=True),
        StructField("width", LongType(), nullable=True),
        StructField("height", LongType(), nullable=True),
    ]
)

URL_ENTITY_SCHEMA = StructType(
    [
        StructField("display_url", StringType(), nullable=True),
        StructField("expanded_url", StringType(), nullable=True),
        StructField("indices", ArrayType(LongType()), nullable=True),
        StructField("url", StringType(), nullable=True),
    ]
)

PROFILE_BIO_SCHEMA = StructType(
    [
        StructField("description", StringType(), nullable=True),
        StructField(
            "entities",
            StructType(
                [
                    StructField(
                        "description",
                        StructType(
                            [StructField("urls", ArrayType(URL_ENTITY_SCHEMA), nullable=True)]
                        ),
                        nullable=True,
                    ),
                    StructField(
                        "url",
                        StructType(
                            [StructField("urls", ArrayType(URL_ENTITY_SCHEMA), nullable=True)]
                        ),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)

HIGHLIGHTS_INFO_SCHEMA = StructType(
    [
        StructField("canHighlightTweets", BooleanType(), nullable=True),
        StructField("highlightedTweets", StringType(), nullable=True),
    ]
)

IDENTITY_VERIFICATION_SCHEMA = StructType(
    [
        StructField("isIdentityVerified", BooleanType(), nullable=True),
        StructField("verifiedSinceMsec", StringType(), nullable=True),
    ]
)

TWEET_FIELDS = [
    StructField("id", StringType(), nullable=False),
    StructField("text", StringType(), nullable=False),
    StructField("createdAt", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("lang", StringType(), nullable=True),
    StructField("retweetCount", LongType(), nullable=False),
    StructField("replyCount", LongType(), nullable=False),
    StructField("likeCount", LongType(), nullable=False),
    StructField("quoteCount", LongType(), nullable=False),
    StructField("viewCount", LongType(), nullable=False),
    StructField("bookmarkCount", LongType(), nullable=False),
    StructField("isReply", BooleanType(), nullable=True),
    StructField("isQuoteStatus", BooleanType(), nullable=True),
    StructField("conversationId", StringType(), nullable=True),
    StructField("author", AUTHOR_SCHEMA, nullable=True),
    StructField("media", ArrayType(MEDIA_SCHEMA), nullable=True),
]

TWEETS_SEARCH_SCHEMA = StructType(
    TWEET_FIELDS + [StructField("search_query", StringType(), nullable=False)]
)

USER_TWEETS_SCHEMA = StructType(
    TWEET_FIELDS + [StructField("source_username", StringType(), nullable=False)]
)

USER_PROFILES_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("username", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("description", StringType(), nullable=True),
        StructField("businessAccountAffiliatesCount", LongType(), nullable=True),
        StructField("creatorSubscriptionsCount", LongType(), nullable=True),
        StructField("favouritesCount", LongType(), nullable=True),
        StructField("followers", LongType(), nullable=True),
        StructField("following", LongType(), nullable=True),
        StructField("verified", BooleanType(), nullable=True),
        StructField("isVerified", BooleanType(), nullable=True),
        StructField("isBlueVerified", BooleanType(), nullable=True),
        StructField("hasGraduatedAccess", BooleanType(), nullable=True),
        StructField("hasHiddenSubscriptionsOnProfile", BooleanType(), nullable=True),
        StructField("highlightsInfo", HIGHLIGHTS_INFO_SCHEMA, nullable=True),
        StructField("identityVerification", IDENTITY_VERIFICATION_SCHEMA, nullable=True),
        StructField("isProfileTranslatable", BooleanType(), nullable=True),
        StructField("profilePicture", StringType(), nullable=True),
        StructField("coverPicture", StringType(), nullable=True),
        StructField("profileBannerUrl", StringType(), nullable=True),
        StructField("profileDescriptionLanguage", StringType(), nullable=True),
        StructField("profileImageShape", StringType(), nullable=True),
        StructField("profileInterstitialType", StringType(), nullable=True),
        StructField("profileSortEnabled", BooleanType(), nullable=True),
        StructField("profileTranslatorType", StringType(), nullable=True),
        StructField("profile_bio", PROFILE_BIO_SCHEMA, nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("createdAt", StringType(), nullable=True),
        StructField("mediaCount", LongType(), nullable=True),
        StructField("parodyCommentaryFanLabel", StringType(), nullable=True),
        StructField("possiblySensitive", BooleanType(), nullable=True),
        StructField("statusesCount", LongType(), nullable=True),
        StructField("protected", BooleanType(), nullable=True),
        StructField("superFollowEligible", BooleanType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("verifiedType", StringType(), nullable=True),
        StructField("configured_username", StringType(), nullable=False),
    ]
)

TRENDS_SCHEMA = StructType(
    [
        StructField("name", StringType(), nullable=False),
        StructField("woeid", LongType(), nullable=False),
        StructField("description", StringType(), nullable=True),
        StructField("query", StringType(), nullable=True),
        StructField("rank", LongType(), nullable=True),
        StructField("tweetVolume", LongType(), nullable=True),
        StructField("url", StringType(), nullable=True),
    ]
)

TABLE_SCHEMAS = {
    "tweets_search": TWEETS_SEARCH_SCHEMA,
    "user_profiles": USER_PROFILES_SCHEMA,
    "user_tweets": USER_TWEETS_SCHEMA,
    "trends": TRENDS_SCHEMA,
}

TABLE_METADATA = {
    "tweets_search": {
        "primary_keys": ["id", "search_query"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "user_profiles": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "user_tweets": {
        "primary_keys": ["id", "source_username"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "trends": {
        "primary_keys": ["woeid", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}

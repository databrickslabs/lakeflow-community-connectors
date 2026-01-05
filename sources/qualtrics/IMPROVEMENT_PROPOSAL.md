# Qualtrics Connector Improvement Proposal

## Executive Summary

The Qualtrics connector demonstrates strong functionality and methodology but has opportunities for enhancement in API coverage, code quality, and scalability.

## Evaluation Highlights

### Strengths Recognized by All Evaluators
- ✅ **Innovative async export workflow** (create → poll → download → parse)
- ✅ **Comprehensive documentation** with troubleshooting and performance guidance
- ✅ **Robust error handling** with retry logic and rate limiting
- ✅ **Multiple ingestion modes** (CDC, append, snapshot)
- ✅ **Complex nested structures** handled effectively with MapType/StructType

### Common Improvement Themes
1. **Limited API Coverage** - Missing key endpoints
2. **Incremental Sync Limitations** - Client-side filtering vs server-side queries
3. **Code Quality** - Print statements and magic numbers
4. **Scalability** - Large dataset optimization needed

---

## Proposed Improvements

### Priority 1: Code Quality & Best Practices (Quick Wins)

#### 1.1 Replace Print Statements with Structured Logging
**Current Issues:**
- 9 print() statements throughout the code
- No log levels (INFO, WARNING, ERROR)
- Cannot be configured or disabled in production

**Proposed Solution:**
```python
import logging

logger = logging.getLogger(__name__)

# Example replacements:
# Before: print(f"Error fetching surveys: {e}")
# After:  logger.error(f"Error fetching surveys: {e}", exc_info=True)

# Before: print(f"Rate limited. Waiting {retry_after} seconds...")
# After:  logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
```

**Benefits:**
- Production-ready logging
- Configurable log levels
- Better debugging and monitoring
- Consistent with framework standards

**Effort:** Low (1-2 hours)

#### 1.2 Extract Configuration Constants
**Current Issues:**
- Hardcoded values scattered throughout (max_attempts=60, sleep times)
- Difficult to tune without code changes
- Not documented

**Proposed Solution:**
```python
class QualtricsConfig:
    """Configuration constants for Qualtrics connector."""

    # Export polling configuration
    MAX_EXPORT_POLL_ATTEMPTS = 60
    EXPORT_POLL_INTERVAL_MIN = 1  # seconds
    EXPORT_POLL_INTERVAL_MAX = 2  # seconds

    # Rate limiting
    DEFAULT_RETRY_WAIT = 5  # seconds
    MAX_RETRIES = 3

    # Pagination
    DEFAULT_PAGE_SIZE = 100

    # Timeouts
    REQUEST_TIMEOUT = 30  # seconds
```

**Benefits:**
- Easy to tune performance
- Self-documenting
- Testable with different configurations

**Effort:** Low (2-3 hours)

---

### Priority 2: Enhanced API Coverage (High Value)

#### 2.1 Add XM Directory Tables
**Missing Endpoints:**
- Directories (XM Directory root)
- Segments (audience segments)
- Transactions (XM events)

**Business Value:**
- Enables customer journey analytics
- Required for Experience Management use cases
- Highly requested by enterprise users

**Proposed Tables:**
```python
# Add to self.tables:
"directories",      # XM Directory listings
"segments",         # Audience segments
"directory_contacts" # Contacts within directories (more complete than current contacts)
```

**API Documentation:**
- GET /directories
- GET /directories/{directoryId}/contacts
- GET /segments

**Effort:** Medium (8-12 hours per table including tests)

#### 2.2 Add Survey Metadata Tables
**Missing Endpoints:**
- Survey definitions (questions, blocks, flow)
- Response quotas
- Survey options

**Business Value:**
- Understand survey structure without manual inspection
- Automated survey analytics
- Question text lookup for response analysis

**Proposed Tables:**
```python
"survey_definitions", # Full survey structure
"survey_questions",   # Denormalized question text
"survey_quotas"      # Response quotas and limits
```

**API Documentation:**
- GET /survey-definitions/{surveyId}
- Includes questions, blocks, flow, scoring

**Effort:** Medium-High (12-16 hours including complex nested parsing)

#### 2.3 Add Mailing Lists Table
**Current Gap:** Gemini specifically mentioned this is incomplete

**Proposed Implementation:**
```python
"mailing_lists",     # Mailing list metadata
"mailing_list_contacts" # Contacts in mailing lists
```

**API Documentation:**
- GET /mailinglists
- GET /mailinglists/{mailingListId}/contacts

**Effort:** Medium (6-8 hours)

---

### Priority 3: Performance & Scalability Optimizations

#### 3.1 True Incremental Export for Survey Responses
**Current Limitation:**
- Downloads entire export every sync
- Filters client-side using string comparison
- Inefficient for large surveys

**Proposed Solution:**
```python
# Expose export parameters via table_options
table_options = {
    "surveyId": "SV_123",
    "startDate": "2024-01-01T00:00:00Z",  # NEW
    "endDate": "2024-01-31T23:59:59Z",    # NEW
    "format": "json",                      # NEW
    "compress": True                       # NEW
}

# Use Qualtrics export filters server-side
export_body = {
    "format": "json",
    "startDate": table_options.get("startDate"),
    "endDate": table_options.get("endDate"),
    "compress": True if table_options.get("compress", "true").lower() == "true" else False
}
```

**Benefits:**
- 10-100x faster for incremental syncs
- Reduced API usage
- Lower memory footprint
- Respects Qualtrics server-side filtering

**API Documentation:**
- Export API supports startDate/endDate filters
- Already in API spec, just not exposed

**Effort:** Medium (6-8 hours)

#### 3.2 Streaming for Large Response Exports
**Current Limitation:**
- Loads entire ZIP into memory
- Parses full JSON before yielding
- Can exhaust memory on 100K+ response surveys

**Proposed Solution:**
```python
def _download_response_export_streaming(self, survey_id: str, file_id: str) -> Iterator[dict]:
    """Stream responses from ZIP without loading full file into memory."""
    url = f"{self.base_url}/surveys/{survey_id}/export-responses/{file_id}/file"

    with requests.get(url, headers=self.headers, stream=True) as response:
        response.raise_for_status()

        # Stream ZIP processing
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            for json_filename in zip_file.namelist():
                if json_filename.endswith('.json'):
                    with zip_file.open(json_filename) as json_file:
                        # Stream JSON parsing
                        for line in json_file:
                            if line.strip():
                                record = json.loads(line)
                                yield self._process_response_record(record, survey_id)
```

**Benefits:**
- Handles 1M+ responses without OOM
- Constant memory usage
- Faster time to first record

**Effort:** Medium-High (8-10 hours including testing)

#### 3.3 Expose Performance Tuning via Table Options
**Proposed Enhancement:**
```python
table_options = {
    "surveyId": "SV_123",
    "pageSize": "500",           # NEW: For paginated endpoints
    "exportFormat": "json",      # NEW: vs "csv"
    "exportCompress": "true",    # NEW: Compression
    "pollInterval": "2",         # NEW: Override default polling
    "maxPollAttempts": "120"     # NEW: For very large surveys
}
```

**Benefits:**
- Users can optimize for their use case
- No code changes needed for tuning
- Self-service performance optimization

**Effort:** Low-Medium (4-6 hours)

---

### Priority 4: Code Reusability & Framework Contributions

#### 4.1 Extract Async Polling Utility
**Identified by:** GPT-5.2 and Gemini

**Current State:**
- Excellent polling implementation in Qualtrics
- Could benefit other connectors (GitHub Actions, Databricks Jobs, etc.)

**Proposed Solution:**
```python
# New file: libs/async_poll_utils.py

class AsyncJobPoller:
    """Reusable utility for polling async job completion."""

    def __init__(self,
                 get_status_func,
                 max_attempts=60,
                 min_interval=1,
                 max_interval=5,
                 adaptive=True):
        self.get_status = get_status_func
        self.max_attempts = max_attempts
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.adaptive = adaptive

    def poll_until_complete(self, job_id: str) -> dict:
        """Poll job status until complete or timeout."""
        for attempt in range(self.max_attempts):
            status_response = self.get_status(job_id)

            if status_response.get("status") == "complete":
                return status_response

            if status_response.get("status") == "failed":
                raise Exception(f"Job {job_id} failed")

            # Adaptive sleep based on progress
            if self.adaptive:
                progress = status_response.get("percentComplete", 0)
                sleep_time = self._calculate_sleep(progress)
            else:
                sleep_time = self.min_interval

            time.sleep(sleep_time)

        raise TimeoutError(f"Job {job_id} did not complete in {self.max_attempts} attempts")

    def _calculate_sleep(self, progress):
        """Adaptive sleep: faster polling when progress is high."""
        if progress < 50:
            return self.max_interval
        else:
            return self.min_interval
```

**Benefits:**
- Reusable across connectors
- Standardized async patterns
- Easier testing and maintenance

**Effort:** Medium (6-8 hours including tests and migration)

#### 4.2 Promote HTTP Retry Wrapper to Shared Library
**Identified by:** GPT-5.2

**Current State:**
- Excellent `_make_request()` method with retry logic
- Every connector reimplements similar logic

**Proposed Solution:**
```python
# Move to: libs/http_utils.py

class HTTPClientWithRetry:
    """HTTP client with automatic retry and rate limiting."""

    def __init__(self, base_url, headers, max_retries=3, timeout=30):
        self.base_url = base_url
        self.headers = headers
        self.max_retries = max_retries
        self.timeout = timeout

    def request(self, method, endpoint, **kwargs):
        """Make HTTP request with retry logic."""
        # Implement unified retry + rate limit handling
        # Based on Qualtrics implementation
```

**Benefits:**
- Consistent error handling across connectors
- Reduces code duplication
- Centralized rate limit logic
- Easier to add features (circuit breaker, metrics)

**Effort:** Medium (8-10 hours including migration of other connectors)

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks)
- ✅ Replace print statements with logging
- ✅ Extract configuration constants
- ✅ Expose export parameters via table_options
- **Impact:** Immediate production readiness improvements
- **Risk:** Low

### Phase 2: API Coverage Expansion (2-3 weeks)
- 🔄 Add mailing_lists table
- 🔄 Add survey_definitions table
- 🔄 Add XM Directory tables (directories, segments)
- **Impact:** Unlocks new use cases
- **Risk:** Medium (requires API testing)

### Phase 3: Performance Optimizations (2-3 weeks)
- 🔄 Implement true incremental exports
- 🔄 Add streaming for large datasets
- 🔄 Performance tuning via table options
- **Impact:** 10-100x performance improvement
- **Risk:** Medium (memory/streaming complexity)

### Phase 4: Framework Contributions (1-2 weeks)
- 🔄 Extract async polling utility
- 🔄 Promote HTTP client to shared library
- 🔄 Update other connectors to use shared utilities
- **Impact:** Benefits entire framework
- **Risk:** Medium (requires coordination)

**Total Estimated Effort:** 6-10 weeks

---

## Success Metrics

### Code Quality
- ✅ Zero print() statements (currently 9)
- ✅ All timeouts configurable (currently hardcoded)
- ✅ 100% test coverage maintained
- ✅ Pylint score > 9.0

### API Coverage
- ✅ 7+ tables (currently 4)
- ✅ Survey definitions table implemented
- ✅ XM Directory integration complete

### Performance
- ✅ 10x faster incremental syncs
- ✅ Handle 1M+ responses without OOM
- ✅ <5min for typical survey export

### Reusability
- ✅ 2+ connectors using shared async poller
- ✅ 3+ connectors using shared HTTP client
- ✅ Documentation for framework patterns

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| API rate limiting during testing | Medium | Low | Use test survey with few responses |
| Breaking changes to existing users | Low | High | Maintain backward compatibility |
| Memory issues with streaming | Low | Medium | Incremental testing with large datasets |
| Complex nested schema parsing | Medium | Medium | Comprehensive schema validation tests |

---

## Prioritization Rationale

**Why Phase 1 First:**
- Low risk, high value
- No API changes needed
- Improves production readiness immediately

**Why API Coverage Before Performance:**
- Unlocks new use cases sooner
- Performance optimizations benefit more tables
- Easier to test with more data

**Why Framework Contributions Last:**
- Requires stable patterns first
- Benefits accumulate over time
- Lower urgency than core functionality

---

## Appendix: Detailed Evaluation Scores

### Claude Opus: 2.3/3
**Strengths:** Async workflow, documentation, retry logic
**Weaknesses:** Limited coverage, true incremental lacking, print statements

### GPT-5.2: 2.5/3
**Strengths:** Response export workflow, multiple ingestion modes, rate limiting
**Weaknesses:** Limited API surface, client-side filtering, HTTP utils not shared

### Gemini: 2.2/5
**Strengths:** Complex async process, polling logic, nested structures
**Weaknesses:** Mailing Lists incomplete, need survey definitions, eventual consistency

**Consensus:** Strong foundation, needs expansion and refinement

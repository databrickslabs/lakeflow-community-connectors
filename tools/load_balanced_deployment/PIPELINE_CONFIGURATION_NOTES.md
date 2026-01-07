# Pipeline Configuration for Community Connectors

## CRITICAL: Community Connectors REQUIRE Serverless + PREVIEW

**Community connectors ONLY work with:**
- `channel: PREVIEW` (required for PySpark Data Source API support)
- `serverless: true` (required for community connector runtime)

**Using classic clusters or CURRENT channel WILL NOT WORK.**

## Upstream CLI Configuration

From `tools/community_connector/default_config.yaml`:
```yaml
pipeline:
  channel: PREVIEW          # REQUIRED for community connectors
  continuous: false
  development: true
  serverless: true          # REQUIRED for community connectors
```

## Load-Balanced Deployment Configuration

The load-balanced deployment toolkit MUST use the same settings:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: "Pipeline Name"
      catalog: "${var.catalog}"
      target: "${var.schema}"
      channel: PREVIEW        # REQUIRED
      serverless: true        # REQUIRED
      development: true
      continuous: false
      libraries: [...]
      # NO clusters configuration when using serverless
```

## Why These Settings Are Required

### PREVIEW Channel
- Community connectors use the **PySpark Data Source API**
- This API requires the PREVIEW channel for full support
- CURRENT channel lacks necessary runtime features

### Serverless Compute
- Community connectors require serverless compute runtime
- Classic clusters lack the necessary PySpark Data Source integration
- Serverless provides automatic platform credential injection for UC Connections

## Implementation in Load-Balanced Toolkit

### Current generate_dab_yaml.py (CORRECT)

```python
def generate_dab_yaml(...):
    pipeline_def = {
        "name": f"...",
        "catalog": "${var.catalog}",
        "target": "${var.schema}",
        "channel": "PREVIEW",      # Required for community connectors
        "serverless": True,         # Required for community connectors
        "development": True,
        "continuous": False,
        "libraries": [...]
        # NO clusters configuration
    }
```

### What NOT to Do (INCORRECT)

```python
# ❌ WRONG - This will NOT work for community connectors
pipeline_def = {
    "channel": "CURRENT",  # Wrong - lacks PySpark Data Source API support
    "serverless": False,   # Wrong - community connectors need serverless
    "clusters": [          # Wrong - no clusters when using serverless
        {
            "label": "default",
            "node_type_id": "i3.xlarge",
            "num_workers": 1
        }
    ]
}
```

## Benefits of Serverless + PREVIEW

1. **Auto-scaling** - Compute scales based on workload automatically
2. **No cluster management** - No need to specify node_type_id or workers
3. **Cost efficiency** - Pay only for actual usage
4. **Latest features** - Access to newest DLT and PySpark Data Source capabilities
5. **Platform credential injection** - UC Connection credentials injected automatically

## Related References

- Upstream CLI default config: `tools/community_connector/default_config.yaml`
- Upstream CLI adoption notes: `tools/load_balanced_deployment/UPSTREAM_CLI_ADOPTION.md`
- PySpark Data Source API documentation: Databricks docs

"""Regenerate schemas from a local copy of Rootly's public OpenAPI document.

Usage: python tests/unit/sources/rootly/generate_schemas.py /path/to/swagger.json
No network access or credentials are used. Review schema changes before committing.
"""

import hashlib
import json
import sys
from pathlib import Path

TABLES = (
    "incidents",
    "services",
    "users",
    "teams",
    "severities",
    "escalation_policies",
    "schedules",
)
DESTINATION = (
    Path(__file__).resolve().parents[4] / "src/databricks/labs/community_connector/sources/rootly"
)


def generate(openapi_path: Path) -> None:
    raw = openapi_path.read_bytes()
    document = json.loads(raw)
    definitions = document["components"]["schemas"]

    def resolve(node):
        if "$ref" in node:
            return resolve(definitions[node["$ref"].split("/")[-1]])
        result = dict(node)
        properties = dict(result.get("properties", {}))
        for part in node.get("allOf", []):
            resolved = resolve(part)
            properties.update(resolved.get("properties", {}))
        if properties:
            result["properties"] = {key: resolve(value) for key, value in properties.items()}
        if "items" in result:
            result["items"] = resolve(result["items"])
        result.pop("allOf", None)
        return result

    declarations = []
    structures = {}

    def expression(node, path):
        kind = node.get("type")
        if kind == "object" and node.get("properties"):
            fields = [
                f"    StructField({json.dumps(key)}, {expression(value, path + '_' + key)}, True),"
                for key, value in node["properties"].items()
            ]
            body = "StructType([\n" + "\n".join(fields) + "\n])"
            if body not in structures:
                name = path.upper() + "_SCHEMA"
                structures[body] = name
                declarations.append(name + " = " + body)
            return structures[body]
        if kind == "array":
            return f"ArrayType({expression(node['items'], path + '_item')}, True)"
        mapping = {
            "string": "StringType()",
            "integer": "LongType()",
            "number": "DoubleType()",
            "boolean": "BooleanType()",
            "object": "VariantType()",
        }
        if kind not in mapping:
            raise ValueError(f"Unsupported schema at {path}: {node}")
        return mapping[kind]

    rows = {}
    for table in TABLES:
        schema = document["paths"]["/v1/" + table]["get"]["responses"]["200"]["content"][
            "application/vnd.api+json"
        ]["schema"]
        rows[table] = resolve(schema)["properties"]["data"]["items"]
    entries = [f'    "{table}": {expression(row, table)},' for table, row in rows.items()]
    imports = [
        "ArrayType",
        "BooleanType",
        "LongType",
        "StringType",
        "StructField",
        "StructType",
        "VariantType",
    ]
    if any("DoubleType()" in declaration for declaration in declarations):
        imports.append("DoubleType")
    digest = hashlib.sha256(raw).hexdigest()
    header = (
        '"""Schemas derived from Rootly public OpenAPI; preserve nested JSON:API attributes.\n\n'
        "Source: https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json\n"
        f"Input SHA256: {digest}\n"
        "Regenerator: tests/unit/sources/rootly/generate_schemas.py\n"
        "Open-ended objects use VariantType; strings (including dates) remain strings.\n"
        '"""\n\n'
        "from pyspark.sql.types import (\n"
        + "".join(f"    {name},\n" for name in sorted(imports))
        + ")\n\n\n"
    )
    (DESTINATION / "rootly_schemas.py").write_text(
        header
        + "\n\n".join(declarations)
        + "\n\nTABLE_SCHEMAS = {\n"
        + "\n".join(entries)
        + "\n}\n",
        encoding="utf-8",
    )

    def field_rows(node, path):
        kind = node["type"]
        yield f"| `{path}` | {kind} |"
        if kind == "object":
            for key, child in node.get("properties", {}).items():
                yield from field_rows(child, path + "." + key)
        elif kind == "array":
            yield from field_rows(node["items"], path + "[]")

    sections = [
        "# Rootly schema field reference\n\n"
        "Generated from the public OpenAPI list-response schemas. This inventory includes "
        "every declared resource field, including nested fields. Undeclared object shapes "
        "are retained as VARIANT. Response-level pagination and included resources "
        "are not rows.\n\n"
        f"Input SHA256: `{digest}`.\n\n"
        "## Relationships and joins\n\n"
        "See [relationship keys and SQL examples](relationships.md) for joins "
        "between these tables.\n"
        "For example, `escalation_policies.attributes.group_ids` contains IDs "
        "matching `teams.id`.\n"
        "Rootly calls teams `groups` in relationship fields. These keys are "
        "already preserved in the\n"
        "nested schema; a top-level JSON:API `relationships` object is not "
        "required for each join.\n"
    ]
    for table, row in rows.items():
        fields = []
        for key, child in row["properties"].items():
            fields.extend(field_rows(child, key))
        sections.append(f"## {table}\n\n| Field | OpenAPI type |\n|---|---|\n" + "\n".join(fields))
    (DESTINATION / "schema_reference.md").write_text("\n\n".join(sections) + "\n")


if __name__ == "__main__":
    generate(Path(sys.argv[1]))

# Rootly relationships and joins

The connector retains foreign keys in `attributes`, nested resource envelopes, and declared
JSON:API `relationships`. Rootly's API calls teams `groups`, while the connector table is `teams`.
Use outer resource IDs (`id`) for joins, not display names or incident sequential numbers.

| From table | Foreign key path | To table / key | Shape |
|---|---|---|---|
| `escalation_policies` | `attributes.group_ids` | `teams.id` | Array of string IDs |
| `escalation_policies` | `attributes.service_ids` | `services.id` | Array of string IDs |
| `services` | `attributes.escalation_policy_id` | `escalation_policies.id` | String ID |
| `services` | `attributes.owner_group_ids` | `teams.id` | Array of string IDs |
| `schedules` | `attributes.owner_group_ids` | `teams.id` | Array of string IDs |
| `incidents` | `attributes.severity.data.id` | `severities.id` | String ID |
| `incidents` | `attributes.services[].data.id` | `services.id` | Nested array; explode before joining |
| `incidents` | `attributes.groups[].data.id` | `teams.id` | Nested array; explode before joining |
| `incidents` | `attributes.parent_incident_id` | `incidents.id` | String ID; self join |
| `incidents` | `attributes.duplicate_incident_id` | `incidents.id` | String ID; self join |
| `teams` | `attributes.user_ids` | `users.id` | Integer array; cast the exploded member to STRING |

These are paths in the declared list-response schema, not a promise that every response includes
or populates them. Empty/null keys, private resources, and separately refreshed tables can leave
unmatched rows. No `include` expansion is requested. Prefer left joins when retaining the source
row matters. The schema does not expose every Rootly relationship, and users' role relationships
point to role resources outside the seven supported tables.

## Teams and escalation policies

Replace the unqualified table names with your destination catalog/schema names as needed.
A team can have multiple matching policies, and a policy can refer to multiple teams.

```sql
SELECT
  t.id AS team_id,
  t.attributes.name AS team_name,
  p.id AS escalation_policy_id,
  p.attributes.name AS escalation_policy_name
FROM teams AS t
LEFT JOIN escalation_policies AS p
  ON array_contains(p.attributes.group_ids, t.id);
```

## Services and their escalation policies

```sql
SELECT
  s.id AS service_id,
  s.attributes.name AS service_name,
  p.id AS escalation_policy_id,
  p.attributes.name AS escalation_policy_name
FROM services AS s
LEFT JOIN escalation_policies AS p
  ON s.attributes.escalation_policy_id = p.id;
```

## Incident teams

```sql
WITH incident_teams AS (
  SELECT id AS incident_id, explode_outer(attributes.groups) AS incident_team
  FROM incidents
)
SELECT it.incident_id, t.id AS team_id, t.attributes.name AS team_name
FROM incident_teams AS it
LEFT JOIN teams AS t ON it.incident_team.data.id = t.id;
```

References: [Rootly public OpenAPI](https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json),
[list escalation policies](https://docs.rootly.com/api-reference/escalationpolicies/list-escalation-policies),
[list teams](https://docs.rootly.com/api-reference/teams/list-teams), and the
[generated field inventory](schema_reference.md).

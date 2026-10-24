ALTER TABLE infra_node ALTER COLUMN health_rules_json_version DROP DEFAULT;

-- Throws away what is in infra_node.units_json.
ALTER TABLE infra_node DROP COLUMN units_json;

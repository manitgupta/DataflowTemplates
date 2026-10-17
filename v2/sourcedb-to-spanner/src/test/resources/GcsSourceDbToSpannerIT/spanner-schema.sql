CREATE TABLE `user_profiles` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL,
  `name` STRING(MAX) AS (JSON_VALUE(`doc`, '$.name')) STORED
) PRIMARY KEY (`key`);

CREATE TABLE `knows` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `from_id` STRING(MAX) NOT NULL,
  `to_id` STRING(MAX) NOT NULL,
  `doc` JSON NOT NULL,
  `from_coll` STRING(MAX) AS (REGEXP_EXTRACT(`from_id`, r'^([^/]+)/')) STORED,
  `from_key` STRING(254) AS (REGEXP_EXTRACT(`from_id`, r'^[^/]+/(.*)$')) STORED,
  `to_coll` STRING(MAX) AS (REGEXP_EXTRACT(`to_id`, r'^([^/]+)/')) STORED,
  `to_key` STRING(254) AS (REGEXP_EXTRACT(`to_id`, r'^[^/]+/(.*)$')) STORED
) PRIMARY KEY (`key`);

CREATE INDEX `knows_by_from` ON `knows`(`from_id`);

CREATE INDEX `knows_by_to` ON `knows`(`to_id`);

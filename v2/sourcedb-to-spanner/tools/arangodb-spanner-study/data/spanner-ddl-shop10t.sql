CREATE TABLE `users` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `products` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `orders` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `events` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `type_zoo` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `hostile_keys` (
  `key` STRING(254) NOT NULL,
  `id` STRING(MAX),
  `rev` STRING(64),
  `doc` JSON NOT NULL
) PRIMARY KEY (`key`);
CREATE TABLE `follows` (
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
CREATE TABLE `purchased` (
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
) PRIMARY KEY (`key`)

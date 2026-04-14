-- ============================================================
-- 01_create_schemas.sql
-- Provision catalog schemas for the medallion architecture.
-- Run once per workspace (or re-run safely; all are IF NOT EXISTS).
-- Catalog: workspace  (default Unity Catalog catalog)
-- ============================================================

-- Bronze: raw CSV ingestion from the MinIO trade-lake volume
CREATE SCHEMA IF NOT EXISTS workspace.bronze
  COMMENT 'Raw ingestion layer — CSV snapshots loaded from the MinIO surveillance-lake volume';

-- Silver: deduplicated, normalised Delta tables
CREATE SCHEMA IF NOT EXISTS workspace.silver
  COMMENT 'Cleaned and deduplicated layer — types cast, strings normalised, duplicates dropped';

-- Gold: aggregated analytics tables powering the compliance dashboard
CREATE SCHEMA IF NOT EXISTS workspace.gold
  COMMENT 'Aggregate layer — trader risk rankings, asset activity, and daily alert metrics';

# Isra Nurul Habibi

**Data / Backend Engineer**

I build reliable data pipelines and backend services — focused on clean,
incremental data movement between systems and pragmatic cloud deployments.

## Skills

`Python` · `SQL / PostgreSQL` · `SQLAlchemy` · `ETL / Data Pipelines` ·
`Google Cloud Platform` · `Pandas` · `IoT / Sensor Data` · `Logging & Observability`

## Featured Project

### Incremental ETL Pipeline: Postgres → Cloud Postgres

*Python · SQLAlchemy · psycopg2 · PostgreSQL · GCP*

A lightweight, incremental data-sync pipeline that moves sensor and
environmental readings from a source PostgreSQL database to a
cloud-hosted destination database, designed for scheduled/repeated
deployment on GCP.

- Tracks the latest synced timestamp per table and pulls only new rows, avoiding full-table reloads.
- Handles multiple tables in a single run (e.g. multi-sensor weather data streams: temperature/humidity, gas, and light sensor readings).
- Credentials and connection strings are externalized via environment variables — nothing sensitive is hard-coded.
- Structured logging at each stage (fetch, copy, verify) for observability and easy debugging in production.

**Highlights:** Incremental loads · Env-based config · Multi-table orchestration · Cloud deployment

---

*Built and maintained independently. Updated 2026.*

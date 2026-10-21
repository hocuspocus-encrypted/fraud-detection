-- 0002 roles (PLAN.md Phase 1). Roles are cluster-wide; they are created NOLOGIN
-- here and given LOGIN + a password from .env by db/migrate.py.
--
--   pipeline_writer  writes pipeline tables (producer, Spark, generator)
--   worker           runs the investigation queue; never sees ground truth
--   agent_ro         the agent's tools; SELECT on an allow-list only
--
-- Table grants live in the migration that creates each table.
-- The existing Spark jobs still connect as fraud_user; moving them is out of scope.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        CREATE ROLE pipeline_writer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker') THEN
        CREATE ROLE worker NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent_ro') THEN
        CREATE ROLE agent_ro NOLOGIN;
    END IF;
END
$$;

-- Defense in depth only: a session can SET these back. Privileges are the real control.
ALTER ROLE agent_ro SET default_transaction_read_only = on;
ALTER ROLE agent_ro SET statement_timeout = '5s';
ALTER ROLE worker SET statement_timeout = '30s';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO pipeline_writer, worker, agent_ro;

GRANT SELECT, INSERT, UPDATE ON transactions, anomaly_flags TO pipeline_writer;
GRANT USAGE ON SEQUENCE anomaly_flags_id_seq TO pipeline_writer;
GRANT SELECT ON transactions, anomaly_flags TO worker;

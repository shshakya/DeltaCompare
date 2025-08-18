CREATE OR REPLACE PROCEDURE public.setup_fdw_for_publications(IN p_server_name text, IN p_username text, IN p_dbname text, IN p_password text, IN p_host text DEFAULT NULL::text, IN p_port text DEFAULT '5432'::text, INOUT p_return integer DEFAULT 0)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    v_host text := COALESCE(p_host, p_server_name);
    r RECORD;
    fdw_schema_name text;
   v_list text;
   v_msg   text;
  v_det   text;
  v_hint  text;
  v_ctx   text;
  v_code  text;
BEGIN
    -- 1) Ensure postgres_fdw extension
    PERFORM 1 FROM pg_extension WHERE extname = 'postgres_fdw';
    IF NOT FOUND THEN
        EXECUTE 'CREATE EXTENSION postgres_fdw';
    END IF;

   RAISE NOTICE 'Create extension completed';

    -- 2) Create or update the FOREIGN SERVER
    IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = p_server_name) THEN
        EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
            p_server_name, v_host, p_port, p_dbname
        );
    ELSE
        EXECUTE format(
            'drop server %I cascade',
            p_server_name
        );
       EXECUTE format(
            'CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
            p_server_name, v_host, p_port, p_dbname
        );
    END IF;

   RAISE NOTICE 'Create FDW server completed';

    -- 3) Create or update USER MAPPING for CURRENT_USER
    IF NOT EXISTS (
        SELECT 1
        FROM pg_user_mappings um
        JOIN pg_foreign_server s ON s.oid = um.srvid
        WHERE s.srvname = p_server_name
          AND um.umuser = (SELECT oid FROM pg_roles WHERE rolname = CURRENT_USER)
    ) THEN
        EXECUTE format(
            'CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
            p_server_name, p_username, p_password
        );
    ELSE
        EXECUTE format(
            'ALTER USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS (SET user %L, SET password %L)',
            p_server_name, p_username, p_password
        );
    END IF;

     RAISE NOTICE 'Create FDW user mapping completed';

    -- 4) For each schema in pg_publication_tables, create {schema}_fdw and IMPORT if empty

     FOR r IN SELECT DISTINCT schemaname FROM pg_publication_tables
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I;', r.schemaname || '_fdw');
   END LOOP;

   FOR r IN SELECT DISTINCT schemaname FROM pg_publication_tables
  LOOP
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I;', r.schemaname || '_fdw');

    SELECT string_agg(format('%I', tablename), ', ')
      INTO v_list
    FROM pg_publication_tables
    WHERE schemaname = r.schemaname;

    IF v_list IS NOT NULL THEN
      EXECUTE format(
        'IMPORT FOREIGN SCHEMA %I LIMIT TO (%s) FROM SERVER %I INTO %I;',
        r.schemaname, v_list, p_server_name, r.schemaname || '_fdw'
      );
    END IF;
  END LOOP;

    RAISE NOTICE 'Create FDW import schema completed';

    -- 5) Enable use_remote_estimate = true on the server (SET if present, otherwise ADD)
    BEGIN
        EXECUTE format('ALTER SERVER %I OPTIONS (SET use_remote_estimate %L);', p_server_name, 'true');
    EXCEPTION WHEN others THEN
        EXECUTE format('ALTER SERVER %I OPTIONS (ADD use_remote_estimate %L);', p_server_name, 'true');
    END;

    RAISE NOTICE 'Create FDW  use_remote_estimate completed';
    -- 6) Tune fetch_size and batch_size for all foreign tables in our {schema}_fdw schemas
    FOR r IN
        SELECT c.relname AS table_name, n.nspname AS schemaname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'f'
          AND (n.nspname,c.relname) IN (SELECT  schemaname || '_fdw',tablename FROM pg_publication_tables)
    LOOP
        -- fetch_size
        BEGIN
            EXECUTE format('ALTER FOREIGN TABLE %I.%I OPTIONS (SET fetch_size %L);', r.schemaname, r.table_name, '10000');
        EXCEPTION WHEN others THEN
            EXECUTE format('ALTER FOREIGN TABLE %I.%I OPTIONS (ADD fetch_size %L);', r.schemaname, r.table_name, '10000');
        END;

        -- batch_size
        BEGIN
            EXECUTE format('ALTER FOREIGN TABLE %I.%I OPTIONS (SET batch_size %L);', r.schemaname, r.table_name, '10000');
        EXCEPTION WHEN others THEN
            EXECUTE format('ALTER FOREIGN TABLE %I.%I OPTIONS (ADD batch_size %L);', r.schemaname, r.table_name, '10000');
        END;

        -- 7) ANALYZE each foreign table to populate local stats
        EXECUTE format('ANALYZE %I.%I;', r.schemaname, r.table_name);

       RAISE NOTICE 'Create FDW  perf. optimization completed for %I.%I',r.schemaname,r.table_name;
    END LOOP;

    RAISE NOTICE 'FDW setup complete for server: %', p_server_name;
exception
when others then
p_return :=1;
 GET STACKED DIAGNOSTICS
        v_msg  = MESSAGE_TEXT,
        v_det  = PG_EXCEPTION_DETAIL,
        v_hint = PG_EXCEPTION_HINT,
        v_ctx  = PG_EXCEPTION_CONTEXT,
        v_code = RETURNED_SQLSTATE;


END;
$procedure$

CREATE TABLE arprecip.matviews(
    mv_name NAME NOT NULL PRIMARY KEY,
    v_name NAME NOT NULL,
    last_refresh TIMESTAMP WITH TIME ZONE
);

CREATE OR REPLACE FUNCTION arprecip.create_matview(NAME, NAME)
  RETURNS VOID
  SECURITY DEFINER
  LANGUAGE plpgsql AS
$$
DECLARE
    matview ALIAS FOR $1;
    view_name ALIAS FOR $2;
    entry arprecip.matviews%ROWTYPE;
BEGIN
    SELECT * INTO entry FROM arprecip.matviews WHERE mv_name = matview;
    IF FOUND THEN
        RAISE EXCEPTION 'Materialized view % already exists.',
            matview;
    END IF;
    EXECUTE 'REVOKE ALL ON ' || view_name || ' FROM PUBLIC'; 
    EXECUTE 'GRANT SELECT ON ' || view_name || ' TO PUBLIC';
    EXECUTE 'CREATE TABLE ' || matview || ' AS SELECT * FROM ' || view_name;
    EXECUTE 'REVOKE ALL ON ' || matview || ' FROM PUBLIC';
    EXECUTE 'GRANT SELECT ON ' || matview || ' TO PUBLIC';
    INSERT INTO arprecip.matviews (mv_name, v_name, last_refresh)
        VALUES (matview, view_name, CURRENT_TIMESTAMP); 
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION arprecip.drop_matview(NAME) RETURNS VOID
 SECURITY DEFINER
 LANGUAGE plpgsql AS
$$
DECLARE
    matview ALIAS FOR $1;
    entry arprecip.matviews%ROWTYPE;
BEGIN
    SELECT * INTO entry FROM arprecip.matviews WHERE mv_name = matview;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Materialized view % does not exist.', matview;
    END IF;
    EXECUTE 'DROP TABLE ' || matview;
    DELETE FROM arprecip.matviews WHERE mv_name=matview;
    RETURN;
END
$$;

CREATE OR REPLACE FUNCTION arprecip.refresh_matview(name) RETURNS VOID
 SECURITY DEFINER
 LANGUAGE plpgsql AS 
$$
DECLARE 
        matview ALIAS FOR $1;
        entry arprecip.matviews%ROWTYPE;
BEGIN
        SELECT * INTO entry FROM arprecip.matviews WHERE mv_name = matview;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Materialized view % does not exist.', matview;
    END IF;
    EXECUTE 'DELETE FROM ' || matview;
    EXECUTE 'INSERT INTO ' || matview
        || ' SELECT * FROM ' || entry.v_name;
    UPDATE arprecip.matviews
        SET last_refresh=CURRENT_TIMESTAMP
        WHERE mv_name=matview;
    RETURN;
END
$$;

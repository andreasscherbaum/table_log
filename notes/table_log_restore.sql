
-- Notes about the creation of a function to restore from a logtable

/* Ways to do the restore
    - Make a function that returns a SETOF RECORD
        con: Has to write the types when using the function
	con: Forces a total materializing
	pro: Doesn't change DDL
    - Make a function that creates a VIEW
        con: It stays around after use
	pro: Query can optimize
    - Make a function that creates a TEMP TABLE
	con: Forces a total materializing onto storage
 */


-- The query that a view should consist of

SELECT * FROM (
    SELECT DISTINCT ON (''orig_pkey'') * 
    FROM ''log_name''
    WHERE trigger_changed <= (SELECT time FROM time)
    ORDER BY ''orig_pkey'' DESC, trigger_id DESC
) AS t1
WHERE trigger_tuple = 'new'
;


-- Testing of a SETOF RECORD returning function

CREATE TABLE test (
    i int PRIMARY KEY
);
INSERT INTO test SELECT * FROM generate_series(1,1000);

CREATE OR REPLACE FUNCTION select_all(table_name text) 
RETURNS SETOF RECORD AS $$
DECLARE
    row RECORD;
BEGIN
    FOR row IN EXECUTE 'SELECT * FROM ' || table_name
    LOOP
	RETURN NEXT row;
    END LOOP;
    RETURN;
END
$$ LANGUAGE plpgsql;

CREATE VIEW test_view AS SELECT * FROM test;

EXPLAIN ANALYZE SELECT * FROM test WHERE i < 500;

EXPLAIN ANALYZE SELECT * FROM select_all('test') AS foo(i int) WHERE i < 500;

EXPLAIN ANALYZE SELECT * FROM test_view WHERE i = 500;
DROP VIEW test_view;
CREATE VIEW test_view AS SELECT * FROM test;


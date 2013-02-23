-- create function
CREATE OR REPLACE FUNCTION table_log ()
    RETURNS opaque
    AS '$libdir/table_log' LANGUAGE 'C';
CREATE OR REPLACE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR, INT, INT)
    RETURNS VARCHAR
    AS '$libdir/table_log', 'table_log_restore_table' LANGUAGE 'C';
CREATE OR REPLACE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR, INT)
    RETURNS VARCHAR
    AS '$libdir/table_log', 'table_log_restore_table' LANGUAGE 'C';
CREATE OR REPLACE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ, CHAR)
    RETURNS VARCHAR
    AS '$libdir/table_log', 'table_log_restore_table' LANGUAGE 'C';
CREATE OR REPLACE FUNCTION "table_log_restore_table" (VARCHAR, VARCHAR, CHAR, CHAR, CHAR, TIMESTAMPTZ)
    RETURNS VARCHAR
    AS '$libdir/table_log', 'table_log_restore_table' LANGUAGE 'C';

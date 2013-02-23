-- create demo table
CREATE TABLE test (
  id                    INT                 NOT NULL
                                            PRIMARY KEY,
  name                  VARCHAR(20)         NOT NULL
);

-- create log table and add trigger
SELECT table_log_init(4, 'test');

-- test trigger
INSERT INTO test VALUES (1, 'name');
SELECT * FROM test;
SELECT * FROM test_log;
UPDATE test SET name='other name' WHERE id=1;
SELECT * FROM test;
SELECT * FROM test_log;

-- create restore table
SELECT table_log_restore_table('test', 'id', 'test_log', 'trigger_id', 'test_recover', NOW());
SELECT * FROM test_recover;

-- clean up
DROP TABLE test;
DROP TABLE test_log;


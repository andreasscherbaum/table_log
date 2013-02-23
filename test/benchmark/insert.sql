SET search_path TO tablelog_benchmark,public;

INSERT INTO test
    SELECT * FROM generate_series(1, 1000);


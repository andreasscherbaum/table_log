SET search_path TO tablelog_benchmark,public;

UPDATE test SET i=i+10000 WHERE i<=1000/2;
INSERT INTO time VALUES (now());
UPDATE test SET i=i+10000 WHERE i<=1000;

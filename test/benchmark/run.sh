#!/bin/bash

cd $(dirname $0)

psql < create.sql
time psql < insert.sql
time psql < update.sql
time psql < delete.sql


## Config

MODULE_big = table_log
OBJS = table_log.o table_log_restore.o
DATA = table_log_init.sql
DATA_built = table_log.sql
DOCS = README.table_log

## Standard PostgreSQL contrib Makefile
ifdef USE_PGXS
PGXS = $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/tablelog
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

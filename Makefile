subdir = contrib/table_log
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

MODULES = table_log
DATA_built = table_log.sql
DOCS = README.table_log

include $(top_srcdir)/contrib/contrib-global.mk

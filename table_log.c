/*
 * table_log () -- log changes to another table
 *
 * some code taken from noup.c
 *
 *
 * written by Andreas ' ads' Scherbaum (ads@ufp.de)
 *
 *
 * create trigger with log table name as argument
 * if no table name is given, the actual table name
 * plus '_log' will be used
 *
 * example:
 *
 * CREATE TRIGGER test_log_chg AFTER UPDATE OR INSERT OR DELETE ON test_table FOR EACH ROW
 *                EXECUTE PROCEDURE table_log();
 * ^^^^^ 'test_table_log' will be used to log changes
 *
 * CREATE TRIGGER test_log_chg AFTER UPDATE OR INSERT OR DELETE ON test_table FOR EACH ROW
 *                EXECUTE PROCEDURE table_log('log_table');
 * ^^^^^ 'log_table' will be used to log changes
 *
 * the log table needs exact the same columns as the
 * table where the trigger will be used
 * (but without any constraints)
 * plus three extra columns:
 *
 * trigger_mode VARCHAR(10)
 * trigger_tuple VARCHAR(5)
 * trigger_changed TIMESTAMP
 * 
 * trigger_mode contains 'INSERT', 'UPDATE' or 'DELETE'
 * trigger_tuple contains 'old' or 'new'
 *
 * on INSERT, a log with the 'new' tuple will be written,
 * on UPDATE a log with the old tuple and  a log with
 * the new tuple will be written and on DELETE a log
 * with the old tuple will be written
 *
 * a good method to create the log table from the existing table:
 *
 * -- create the table without data
 * SELECT * INTO test_log FROM test LIMIT 0;
 * ALTER TABLE test_log ADD COLUMN trigger_mode VARCHAR(10);
 * ALTER TABLE test_log ADD COLUMN trigger_tuple VARCHAR(5);
 * ALTER TABLE test_log ADD COLUMN trigger_changed TIMESTAMP;
 *
 * if you have any updates or improvements, please contact me
 *
 */

#include "executor/spi.h"	/* this is what you need to work with SPI */
#include "commands/trigger.h"	/* -"- and triggers */
#include "mb/pg_wchar.h"	/* support for the quoting functions */
#include <ctype.h>		/* tolower () */
#include <string.h>		/* strlen() */

extern Datum table_log(PG_FUNCTION_ARGS);
static char *do_quote_ident(char *iptr);
static char *do_quote_literal(char *iptr);
static void __table_log (TriggerData *trigdata, char *changed_mode, char *changed_tuple, HeapTuple tuple, int number_columns, char *log_table);

/* this is a V1 (new) function */
PG_FUNCTION_INFO_V1(table_log);

Datum table_log(PG_FUNCTION_ARGS) {
  TriggerData    *trigdata = (TriggerData *) fcinfo->context;
  int            ret;
  char           query[250];			/* for getting table infos (250 chars should be enough) */
  int            number_columns = 0;		/* counts the number columns in the table */
  int            number_columns_log = 0;	/* counts the number columns in the table */
  char           *log_table;

  /*
   * Some checks first...
   */

  /* Called by trigger manager ? */
  if (!CALLED_AS_TRIGGER(fcinfo)) {
    elog(ERROR, "table_log: not fired by trigger manager");
  }

  /* Should be called for ROW trigger */
  if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event)) {
    elog(ERROR, "table_log: can't process STATEMENT events");
  }

  /* Should be called AFTER */
  if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) {
    elog(ERROR, "table_log: must be fired after event");
  }

  /* Connect to SPI manager */
  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "table_log: SPI_connect returned %d", ret);
  }

  /* table where the query come from */
  snprintf(query, 249, "SELECT COUNT(pg_attribute.attname) AS a FROM pg_class, pg_attribute WHERE pg_class.oid='%i' AND pg_attribute.attnum > 0 AND pg_attribute.attrelid=pg_class.oid", (unsigned int)trigdata->tg_trigtuple->t_tableOid);
  if ((ret = SPI_exec(query, 0)) < 0) {
    elog(ERROR, "could get number columns from relation %s", SPI_getrelname(trigdata->tg_relation));
  }

  /* get the number columns in the table */
  if (SPI_processed > 0) {
    number_columns = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
    if (number_columns < 1) {
      elog(ERROR, "relation %s does not exist", SPI_getrelname(trigdata->tg_relation));
    }
  } else {
    elog(ERROR, "could not get number columns from relation %s", SPI_getrelname(trigdata->tg_relation));
  }

  /* name of the log table */
  if (trigdata->tg_trigger->tgnargs > 0) {
    /* check if a logtable argument is given */  
    /* if yes, use it */
    log_table = (char *) palloc((strlen(trigdata->tg_trigger->tgargs[0]) + 2) * sizeof(char));
    sprintf(log_table, "%s", trigdata->tg_trigger->tgargs[0]);
  } else {
    /* if no, use 'table name' + '_log' */
    log_table = (char *) palloc((strlen(do_quote_ident(SPI_getrelname(trigdata->tg_relation))) + 4) * sizeof(char));
    sprintf(log_table, "%s_log", SPI_getrelname(trigdata->tg_relation));
  }

  /* check if log table exists */
  snprintf(query, 249, "SELECT COUNT(pg_attribute.attname) AS a FROM pg_class, pg_attribute WHERE pg_class.relname=%s AND pg_attribute.attnum > 0 AND pg_attribute.attrelid=pg_class.oid", do_quote_literal(log_table));
  if ((ret = SPI_exec(query, 0)) < 0) {
    elog(ERROR, "could get number columns from relation %s", log_table);
  }

  /* get the number columns in the table */
  if (SPI_processed > 0) {
    number_columns_log = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
    if (number_columns_log < 1) {
      elog(ERROR, "relation %s does not exist", log_table);
    }
  } else {
    elog(ERROR, "could not get number columns in relation %s", log_table);
  }
  /* check if the logtable has 3 (or now 4) columns more than our table */
  if (number_columns_log != number_columns + 3 && number_columns_log != number_columns + 4) {
    elog(ERROR, "number colums in relation %s does not match columns in %s", SPI_getrelname(trigdata->tg_relation), log_table);
  }


  /* For each column in key ... */

  if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
    /* trigger called from INSERT */
    __table_log(trigdata, "INSERT", "new", trigdata->tg_trigtuple, number_columns, log_table);
  } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
    /* trigger called from UPDATE */
    __table_log(trigdata, "UPDATE", "old", trigdata->tg_trigtuple, number_columns, log_table);
    __table_log(trigdata, "UPDATE", "new", trigdata->tg_newtuple, number_columns, log_table);
  } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
    /* trigger called from DELETE */
    __table_log(trigdata, "DELETE", "old", trigdata->tg_trigtuple, number_columns, log_table);
  } else {
    elog(ERROR, "trigger fired by unknown event");
  }

  /* clean up */
  pfree(log_table);

  SPI_finish();
  return PointerGetDatum(trigdata->tg_trigtuple);
}

static void __table_log (TriggerData *trigdata, char *changed_mode, char *changed_tuple, HeapTuple tuple, int number_columns, char *log_table) {
  char     *before_char;
  int      i;
  /* start with 100 bytes */
  int      size_query = 100;
  char     *query;
  char     *query_start;
  int      ret;

  /* add all sizes we need and know at this point */
  size_query = size_query + strlen(changed_mode) + strlen(changed_tuple) + strlen(log_table);

  /* calculate size of the columns */
  for (i = 1; i <= number_columns; i++) {
    /* the column name */
    size_query = size_query + strlen(do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, i))) + 3;
    /* the value */
    before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, i);
    /* old size plus this char and 3 bytes for , and so */
    if (before_char == NULL) {
      size_query = size_query + 6;
    } else {
      size_query = size_query + strlen(do_quote_literal(before_char)) + 3;
    }
  }

  /* allocate memory */
  query_start = (char *) palloc(size_query * sizeof(char));
  query = query_start;

  /* build query */
  sprintf(query, "INSERT INTO %s (", do_quote_ident(log_table));
  query = query_start + strlen(query);

  /* add colum names */
  for (i = 1; i <= number_columns; i++) {
    sprintf(query, "%s, ", do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, i)));
    query = query_start + strlen(query_start);
  }

  /* add the 3 extra colum names */
  sprintf(query, "trigger_mode, trigger_tuple, trigger_changed) VALUES (");
  query = query_start + strlen(query_start);

  /* add values */
  for (i = 1; i <= number_columns; i++) {
    before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, i);
    if (before_char == NULL) {
      sprintf(query, "NULL, ");
    } else {
      sprintf(query, "%s, ", do_quote_literal(before_char));
    }
    query = query_start + strlen(query_start);
  }

  /* add the 3 extra values */
  sprintf(query, "%s, %s, NOW());", do_quote_literal(changed_mode), do_quote_literal(changed_tuple));
  query = query_start + strlen(query_start);

  /* execute insert */
  ret = SPI_exec(query_start, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "could not insert log information into relation %s (error: %d)", log_table, ret);
  }

  /* clean up */
  pfree(query_start);

}



/*
 * MULTIBYTE dependant internal functions follow
 *
 */
/* from src/backend/utils/adt/quote.c and slightly modified */

#ifndef MULTIBYTE

/* Return a properly quoted identifier */
static char * do_quote_ident(char *iptr) {
  char    *result;
  char    *result_return;
  char    *cp1;
  char    *cp2;
  int     len;

  len = strlen(iptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  cp1 = VARDATA(iptr);
  cp2 = VARDATA(result);

  *result++ = '"';
  while (len-- > 0) {
    if (*iptr == '"') {
      *result++ = '"';
    }
    if (*iptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *iptr++;
  }
  *result++ = '"';
  *result++ = '\0';

  return result_return;
}

/* Return a properly quoted literal value */
static char * do_quote_literal(char *lptr) {
  char    *result;
  char    *result_return;
  int     len;

  len = strlen(lptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '\'';
  while (len-- > 0) {
    if (*lptr == '\'') {
      *result++ = '\\';
    }
    if (*lptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *lptr++;
  }
  *result++ = '\'';
  *result++ = '\0';

  return result_return;
}

#else

/* Return a properly quoted identifier (MULTIBYTE version) */
static char * do_quote_ident(char *iptr) {
  char    *result;
  char    *result_return;
  int     len;
  int     wl;

  len = strlen(iptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '"';
  while (len > 0) {
    if ((wl = pg_mblen(iptr)) != 1) {
      len -= wl;

      while (wl-- > 0) {
        *result++ = *iptr++;
      }
      continue;
    }

    if (*iptr == '"') {
      *result++ = '"';
    }
    if (*iptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *iptr++;

    len--;
  }
  *result++ = '"';
  *result++ = '\0';

  return result_return;
}

/* Return a properly quoted literal value (MULTIBYTE version) */
static char * do_quote_literal(char *lptr) {
  char    *result;
  char    *result_return;
  int     len;
  int     wl;

  len = strlen(lptr);
  result = (char *) palloc(len * 2 + 3);
  result_return = result;

  *result++ = '\'';
  while (len > 0) {
    if ((wl = pg_mblen(lptr)) != 1) {
      len -= wl;

      while (wl-- > 0) {
        *result++ = *lptr++;
      }
      continue;
    }

    if (*lptr == '\'') {
      *result++ = '\\';
    }
    if (*lptr == '\\') {
      /* just add a backslash, the ' will be follow */
      *result++ = '\\';
    }
    *result++ = *lptr++;

    len--;
  }
  *result++ = '\'';
  *result++ = '\0';

  return result_return;
}

#endif

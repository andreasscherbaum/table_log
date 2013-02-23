/*
 * table_log () -- log changes to another table
 *
 *
 * see README.table_log for details
 *
 *
 * written by Andreas 'ads' Scherbaum (ads@wars-nicht.de)
 * changes by Kim Hansen <kimhanse@gmail.com>
 *
 */

#include "postgres.h"

#include <string.h>		/* strlen() */

#include "commands/trigger.h"	/* -"- and triggers */
#include "executor/spi.h"	/* this is what you need to work with SPI */
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"	/* support for the quoting functions */
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"


/*
#define TABLE_LOG_DEBUG 1
*/
/*
#define TABLE_LOG_DEBUG_QUERY 1
*/


#if defined TABLE_LOG_DEBUG
#define debug_msg1(x)     elog(NOTICE, x)
#define debug_msg2(x,y)   elog(NOTICE, x,y)
#define debug_msg3(x,y,z) elog(NOTICE, x,y,z)
#else
#define debug_msg1(x)
#define debug_msg2(x,y)
#define debug_msg3(x,y,z)
#endif


/*
 * Exported function
 */
extern Datum table_log(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(table_log);

/*
 * Internal functions
 */
static char *do_quote_ident(char *iptr);
static char *do_quote_literal(char *iptr);
static void __table_log (TriggerData *trigdata, char *changed_mode, 
char *changed_tuple, HeapTuple tuple, int number_columns, 
char *log_table, bool use_session_user, char *log_schema);
static int count_columns (TupleDesc tupleDesc);


/*
table_log()

trigger function for logging table changes

parameter:
  - log table name (optional)
return:
  - trigger data (for Pg)
*/
Datum table_log(PG_FUNCTION_ARGS) {
  TriggerData    *trigdata = (TriggerData *) fcinfo->context;
  int            ret;
  char           query[250 + NAMEDATALEN * 2];	/* for getting table infos (250 chars (+ two times the length of all names (schema + name)) should be enough) */
  int            number_columns = 0;		/* counts the number columns in the table */
  int            number_columns_log = 0;	/* counts the number columns in the table */
  char           *orig_schema;
  char           *log_schema;
  char           *log_table;
  bool            use_session_user = false;          /* should we write the current (session) user to the log table? */
  /*
   * Some checks first...
   */

  debug_msg1("start table_log()");

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

  if (trigdata->tg_trigger->tgnargs > 3) {
    elog(ERROR, "table_log: too many arguments to trigger");
  }
  
  debug_msg1("prechecks done, now getting original table attributes");

  /* Connect to SPI manager */
  ret = SPI_connect();
  if (ret != SPI_OK_CONNECT) {
    elog(ERROR, "table_log: SPI_connect returned %d", ret);
  }

  orig_schema = get_namespace_name(RelationGetNamespace(trigdata->tg_relation));

  number_columns = count_columns(trigdata->tg_relation->rd_att);
  if (number_columns < 1) {
    elog(ERROR, "table_log: can this happen? (number columns < 1)");
  }
  debug_msg2("number column: %i", number_columns);

  /* name of the log schema */
  if (trigdata->tg_trigger->tgnargs > 2) {
    /* check if a log schema argument is given, if yes, use it */
    log_schema = trigdata->tg_trigger->tgargs[2];
  } else {
    /* if no, use orig_schema */
    log_schema = orig_schema;
  }
  debug_msg2("log schema: %s", log_schema);

  /* should we write the current user? */
  if (trigdata->tg_trigger->tgnargs > 1) {
    /* check if a second argument is given */  
    /* if yes, use it, if it is 1 */
    if (atoi(trigdata->tg_trigger->tgargs[1]) == 1) {
      use_session_user = true;
      debug_msg1("will write session user to 'trigger_user'");
    }
  }

  /* name of the log table */
  if (trigdata->tg_trigger->tgnargs > 0) {
    /* check if a logtable argument is given */  
    /* if yes, use it */
    log_table = (char *) palloc((strlen(trigdata->tg_trigger->tgargs[0]) + 2) * sizeof(char));
    sprintf(log_table, "%s", trigdata->tg_trigger->tgargs[0]);
  } else {
    /* if no, use 'table name' + '_log' */
    log_table = (char *) palloc((strlen(do_quote_ident(SPI_getrelname(trigdata->tg_relation))) + 5) * sizeof(char));
    sprintf(log_table, "%s_log", SPI_getrelname(trigdata->tg_relation));
  }

  debug_msg2("log table: %s", log_table);

  debug_msg1("now validate the log table");

  /* get the number columns in table */
  snprintf(query, NAMEDATALEN * 2 + 1, "%s.%s", do_quote_ident(log_schema), do_quote_ident(log_table));
  number_columns_log = count_columns(RelationNameGetTupleDesc(query));
  if (number_columns_log < 1) {
    elog(ERROR, "could not get number columns in relation: %s", log_table);
  }

  /* check if the logtable has 3 (or now 4) columns more than our table */
  /* +1 if we should write the session user */
  if (use_session_user == false) {
    /* without session user */
    if (number_columns_log != number_columns + 3 && number_columns_log != number_columns + 4) {
      elog(ERROR, "number colums in relation %s(%d) does not match columns in %s(%d)", SPI_getrelname(trigdata->tg_relation), number_columns, log_table, number_columns_log);
    }
  } else {
    /* with session user */
    if (number_columns_log != number_columns + 3 + 1 && number_columns_log != number_columns + 4 + 1) {
      elog(ERROR, "number colums in relation %s does not match columns in %s", SPI_getrelname(trigdata->tg_relation), log_table);
    }
  }
  debug_msg1("log table OK");

  /* For each column in key ... */

  debug_msg1("copy data ...");
  if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
    /* trigger called from INSERT */
    debug_msg1("mode: INSERT -> new");
    __table_log(trigdata, "INSERT", "new", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
  } else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) {
    /* trigger called from UPDATE */
    debug_msg1("mode: UPDATE -> old");
    __table_log(trigdata, "UPDATE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
    debug_msg1("mode: UPDATE -> new");
    __table_log(trigdata, "UPDATE", "new", trigdata->tg_newtuple, number_columns, log_table, use_session_user, log_schema);
  } else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {
    /* trigger called from DELETE */
    debug_msg1("mode: DELETE -> old");
    __table_log(trigdata, "DELETE", "old", trigdata->tg_trigtuple, number_columns, log_table, use_session_user, log_schema);
  } else {
    elog(ERROR, "trigger fired by unknown event");
  }

  debug_msg1("cleanup, trigger done");
  /* clean up */
  pfree(log_table);

  /* close SPI connection */
  SPI_finish();
  /* return trigger data */
  return PointerGetDatum(trigdata->tg_trigtuple);
}


/*
__table_log()

helper function for table_log()

parameter:
  - trigger data
  - change mode (INSERT, UPDATE, DELETE)
  - tuple to log (old, new)
  - pointer to tuple
  - number columns in table
  - logging table
  - flag for writing session user
  - log schema
return:
  none
*/
static void __table_log (TriggerData *trigdata, char *changed_mode, char *changed_tuple, HeapTuple tuple, int number_columns, char *log_table, bool use_session_user, char *log_schema) {
  char     *before_char;
  int      i;
  /* start with 100 bytes + schema + tablename*/
  int      size_query = 100 + NAMEDATALEN * 2 + 1;
  char     *query;
  char     *query_start;
  int      ret;

  debug_msg1("calculating query size");
  /* add all sizes we need and know at this point */
  size_query += strlen(changed_mode) + strlen(changed_tuple) + strlen(log_table) + strlen(log_schema);

  /* calculate size of the columns */
  for (i = 1; i <= number_columns; i++) {
    /* the column name */
    /*
    size_query += strlen(do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, i))) + 3;
    */
    size_query += NAMEDATALEN + 3;
    /* the value */
    before_char = SPI_getvalue(tuple, trigdata->tg_relation->rd_att, i);
    /* old size plus this char and 3 bytes for , and so */
    if (before_char == NULL) {
      size_query += 6;
    } else {
      /*
      size_query += strlen(do_quote_literal(before_char)) + 3;
      */
      /* just add double sized memory, speed up things but needs more mem */
      size_query += strlen(before_char) * 2 + 3;
    }
  }

  if (use_session_user == true) {
    /* add memory for session user */
    size_query += NAMEDATALEN + 20;
  }

#ifdef TABLE_LOG_DEBUG_QUERY
  debug_msg2("query size: %i", size_query);
#endif
  debug_msg1("build query");
  /* allocate memory */
  query_start = (char *) palloc(size_query * sizeof(char));
  query = query_start;

  /* build query */
  sprintf(query, "INSERT INTO %s.%s (", do_quote_ident(log_schema), do_quote_ident(log_table));
  query = query_start + strlen(query);

  /* add colum names */
  for (i = 1; i <= number_columns; i++) {
    sprintf(query, "%s, ", do_quote_ident(SPI_fname(trigdata->tg_relation->rd_att, i)));
    query = query_start + strlen(query_start);
  }

  /* add session user */
  if (use_session_user == true) {
    sprintf(query, "trigger_user, ");
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

  /* add session user */
  if (use_session_user == true) {
    sprintf(query, "SESSION_USER, ");
    query = query_start + strlen(query_start);
  }
  /* add the 3 extra values */
  sprintf(query, "%s, %s, NOW());", do_quote_literal(changed_mode), do_quote_literal(changed_tuple));
  query = query_start + strlen(query_start);

#ifdef TABLE_LOG_DEBUG_QUERY
  debug_msg2("query: %s", query_start);
#else
  debug_msg1("execute query");
#endif /*TABLE_LOG_DEBUG_QUERY */
  /* execute insert */
  ret = SPI_exec(query_start, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "could not insert log information into relation %s (error: %d)", log_table, ret);
  }
  debug_msg1("copy done");

  /* clean up */
  pfree(query_start);
}


/*
 * Will count and return the number of columns in the table described by 
 * tupleDesc. It needs to ignore droped columns.
 */
static int count_columns (TupleDesc tupleDesc) {
  int count = 0;
  int i;
  for (i = 0; i < tupleDesc->natts; ++i) {
    if (! tupleDesc->attrs[i]->attisdropped) {
      ++count;
    }
  }
  return count;
}


/*
 * The two quote functions are taken from contrib/dblink/dblink.c
 */

/*
 * Return a properly quoted literal value.
 * Uses quote_literal in quote.c
 */
static char * do_quote_literal(char *rawstr) {
  text *rawstr_text;
  text *result_text;
  char *result;

  rawstr_text = DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(rawstr)));
  result_text = DatumGetTextP(DirectFunctionCall1(quote_literal, PointerGetDatum(rawstr_text)));
  result = DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(result_text)));

  return result;
}

/*
 * Return a properly quoted identifier.
 * Uses quote_ident in quote.c
 */
static char * do_quote_ident(char *rawstr) {
  text *rawstr_text;
  text *result_text;
  char *result;

  rawstr_text = DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(rawstr)));
  result_text = DatumGetTextP(DirectFunctionCall1(quote_ident, PointerGetDatum(rawstr_text)));
  result = DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(result_text)));

  return result;
}


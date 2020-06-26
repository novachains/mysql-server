#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_sql.h"
#include "ha_sdb.h"
#include "item_sum.h"
#include "sdb_cl.h"
#include "sdb_conn.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb_idx.h"
#include "ha_sdb_log.h"
#include "ha_sdb_thd.h"
#include "ha_ldb_udf.h"
#include "ha_sdb_util.h"
#include <client.hpp>
#include <my_bit.h>
#include <mysql/plugin.h>
#include <mysql/psi/mysql_file.h>
#include "bson/lib/base64.h"
#include <sql_class.h>
#include <sql_insert.h>
#include <sql_table.h>
#include <time.h>
#include <sql_update.h>
#include <sql_base.h>
#include <sql_parse.h>

#ifdef IS_MYSQL
#include <table_trigger_dispatcher.h>
#include <json_dom.h>
#include "ha_sdb_part.h"
#endif
using namespace ldbclient;

my_bool getdigest_init(UDF_INIT *init, UDF_ARGS *args, char *message)
{
  init->ptr = NULL;
  std::string *str = new std::string();
  init->ptr = (char *)str;
  return 0;
}

void getdigest_deinit(UDF_INIT *init __attribute__((unused)))
{
  std::string *str = (std::string*)init->ptr;
  if (str) { delete str; }
}
const char *getdigest(UDF_INIT *init __attribute__((unused)),
               UDF_ARGS *args, char *result, unsigned long *length,
               char *is_null, char *error __attribute__((unused)))
{
  int rc = LDB_ERR_OK;
  bson::BSONObj digest;
  THD *thd = my_thread_get_THR_THD();

  Sdb_conn connection(thd_get_thread_id(thd));
  rc = connection.connect();
  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  if (args->arg_count == 0) {
     rc = connection.getDigest(digest);
  }
  else {
     try {
         bson::BSONObj addr;
         bson::fromjson(args->args[0], addr);
         rc = connection.getDigest(digest, addr);
     }
     catch (std::exception) {
         rc = -1;
     }

  }

  connection.disconnect();

  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  if (!digest.isEmpty())
  {
    string *str = (string *)init->ptr;
    str->assign(digest.toString(false,true));
    *length = str->length();
    return str->c_str();
  }
  *is_null = 1;
  *error = 1;
  return 0;
}

my_bool verifydigest_init(UDF_INIT *init, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT)
  {
    strcpy(message,"Wrong arguments to verifydigest;  Use the source");
    return 1;
  }
  init->ptr = NULL;
  string *str = new string();
  init->ptr = (char *)str;
  return 0;
}

void verifydigest_deinit(UDF_INIT *init __attribute__((unused)))
{
  string *str = (string*)init->ptr;
  if (str) { delete str; }
}
const char *verifydigest(UDF_INIT *init __attribute__((unused)),
                   UDF_ARGS *args, char *result, unsigned long *length,
                   char *is_null, char *error __attribute__((unused)))
{
  int rc = LDB_ERR_OK;
  bson::BSONObj resultObj;
  THD *thd = my_thread_get_THR_THD();

  Sdb_conn connection(thd_get_thread_id(thd));
  rc = connection.connect();
  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  try {
      bson::BSONObj addr;
      bson::fromjson(args->args[0], addr);
      rc = connection.get_ldb().verifyJournal(resultObj,addr);
  }
  catch (std::exception) {
      rc = -1;
  }

  connection.disconnect();

  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  if (!resultObj.isEmpty())
  {
    string *str = (string *)init->ptr;
    str->assign(resultObj.toString(false,true));
    *length = str->length();
    return str->c_str();
  }
  *is_null = 1;
  *error = 1;
  return 0;
}

my_bool getentry_init(UDF_INIT *init, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT)
  {
    strcpy(message,"Wrong arguments to verifydigest;  Use the source");
    return 1;
  }
  init->ptr = NULL;
  string *str = new string();
  init->ptr = (char *)str;
  return 0;
}

void getentry_deinit(UDF_INIT *init __attribute__((unused)))
{
  string *str = (string*)init->ptr;
  if (str) { delete str; }
}
const char *getentry(UDF_INIT *init __attribute__((unused)),
               UDF_ARGS *args, char *result, unsigned long *length,
               char *is_null, char *error __attribute__((unused)))
{
  int rc = LDB_ERR_OK;
  bson::BSONObj resultObj;
  THD *thd = my_thread_get_THR_THD();

  Sdb_conn connection(thd_get_thread_id(thd));
  rc = connection.connect();
  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  try {
      bson::BSONObj addr;
      bson::fromjson(args->args[0], addr);
      rc = connection.get_ldb().getEntry(resultObj,addr);
  }
  catch (std::exception) {
      rc = -1;
  }

  connection.disconnect();

  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  if (!resultObj.isEmpty())
  {
    string *str = (string *)init->ptr;
    str->assign(resultObj.toString(false,true));
    *length = str->length();
    return str->c_str();
  }
  *is_null = 1;
  *error = 1;
  return 0;
}

my_bool verifyentry_init(UDF_INIT *init, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT)
  {
    strcpy(message,"Wrong arguments to verifydigest;  Use the source");
    return 1;
  }
  init->ptr = NULL;
  string *str = new string();
  init->ptr = (char *)str;
  return 0;
}

void verifyentry_deinit(UDF_INIT *init __attribute__((unused)))
{
  string *str = (string*)init->ptr;
  if (str) { delete str; }
}
const char *verifyentry(UDF_INIT *init __attribute__((unused)),
                  UDF_ARGS *args, char *result, unsigned long *length,
                  char *is_null, char *error __attribute__((unused)))
{
  int rc = LDB_ERR_OK;
  bson::BSONObj resultObj;
  THD *thd = my_thread_get_THR_THD();

  Sdb_conn connection(thd_get_thread_id(thd));
  rc = connection.connect();
  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  try {
      bson::BSONObj addr;
      bson::fromjson(args->args[0], addr);
      rc = connection.get_ldb().verifyEntry(resultObj,addr);
  }
  catch (std::exception) {
      rc = -1;
  }

  connection.disconnect();

  if (0 != rc) {
    *is_null = 1;
    *error = 1;
    return 0;
  }

  if (!resultObj.isEmpty())
  {
    string *str = (string *)init->ptr;
    str->assign(resultObj.toString(false,true));
    *length = str->length();
    return str->c_str();
  }
  *is_null = 1;
  *error = 1;
  return 0;
}

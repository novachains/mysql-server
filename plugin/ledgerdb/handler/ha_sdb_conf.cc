/* Copyright (c) 2018, SequoiaDB and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif

#include "ha_sdb_conf.h"
#include "ha_sdb_lock.h"

static const char *LDB_ADDR_DFT = "localhost:11810";
static const char *LDB_USER_DFT = "";
static const char *LDB_PASSWORD_DFT = "";
static const char *LDB_DEFAULT_TOKEN = "";
static const char *LDB_DEFAULT_CIPHERFILE = "~/sequoiadb/passwd";
static const my_bool LDB_USE_PARTITION_DFT = TRUE;
static const my_bool LDB_DEBUG_LOG_DFT = FALSE;
static const my_bool LDB_DEFAULT_USE_BULK_INSERT = TRUE;
static const my_bool LDB_DEFAULT_USE_AUTOCOMMIT = TRUE;
static const int LDB_DEFAULT_BULK_INSERT_SIZE = 2000;
/* Always doing transactions on LDB, commit on LDB will make sure all the
   replicas have completed sync datas. So default replsize: 1 to
   improve write row performance.*/
static const int LDB_DEFAULT_REPLICA_SIZE = 1;
static const uint LDB_DEFAULT_SELECTOR_PUSHDOWN_THRESHOLD = 30;
static const longlong LDB_DEFAULT_ALTER_TABLE_OVERHEAD_THRESHOLD = 10000000;
static const my_bool LDB_DEFAULT_USE_TRANSACTION = TRUE;
/*temp parameter "OPTIMIZER_SWITCH_SELECT_COUNT", need remove later*/
static const my_bool OPTIMIZER_SWITCH_SELECT_COUNT = TRUE;
my_bool ldb_optimizer_select_count = OPTIMIZER_SWITCH_SELECT_COUNT;

char *sdb_conn_str = NULL;
char *ldb_user = NULL;
char *ldb_password = NULL;
char *ldb_password_token = NULL;
char *ldb_password_cipherfile = NULL;
my_bool ldb_auto_partition = LDB_USE_PARTITION_DFT;
my_bool ldb_use_bulk_insert = LDB_DEFAULT_USE_BULK_INSERT;
int ldb_bulk_insert_size = LDB_DEFAULT_BULK_INSERT_SIZE;
int ldb_replica_size = LDB_DEFAULT_REPLICA_SIZE;
my_bool ldb_use_autocommit = LDB_DEFAULT_USE_AUTOCOMMIT;
my_bool ldb_debug_log = LDB_DEBUG_LOG_DFT;
ulong ldb_error_level = LDB_ERROR;

static const char *ldb_optimizer_options_names[] = {
    "direct_count", "direct_delete", "direct_update", NullS};

TYPELIB ldb_optimizer_options_typelib = {
    array_elements(ldb_optimizer_options_names) - 1, "",
    ldb_optimizer_options_names, NULL};

::String ldb_encoded_password;
Sdb_encryption ldb_passwd_encryption;
Sdb_rwlock ldb_password_lock;

static const char *ldb_error_level_names[] = {"error", "warning", NullS};

TYPELIB ldb_error_level_typelib = {array_elements(ldb_error_level_names) - 1,
                                   "", ldb_error_level_names, NULL};

static int sdb_conn_addr_validate(THD *thd, struct st_mysql_sys_var *var,
                                  void *save, struct st_mysql_value *value) {
  // The buffer size is not important. Because st_mysql_value::val_str
  // internally calls the Item_string::val_str, which doesn't need a buffer.
  static const uint LDB_CONN_ADDR_BUF_SIZE = 3072;
  char buff[LDB_CONN_ADDR_BUF_SIZE];
  int len = sizeof(buff);
  const char *arg_conn_addr = value->val_str(value, buff, &len);

  ha_sdb_conn_addrs parser;
  int rc = parser.parse_conn_addrs(arg_conn_addr);
  *static_cast<const char **>(save) = (0 == rc) ? arg_conn_addr : NULL;
  return rc;
}

static void ldb_password_update(THD *thd, struct st_mysql_sys_var *var,
                                void *var_ptr, const void *save) {
  Sdb_rwlock_write_guard guard(ldb_password_lock);
  const char *new_password = *static_cast<const char *const *>(save);
  ldb_password = const_cast<char *>(new_password);
  ldb_encrypt_password();
}

// Please declare configuration in the format below:
// [// LDB_DOC_OPT = IGNORE]
// static MYSQL_XXXVAR_XXX(name, varname, opt,
//                         "<English Description>"
//                         "(Default: <Default Value>)"
//                         /*<Chinese Description>*/,
//                         check, update, def);

static MYSQL_SYSVAR_STR(conn_addr, sdb_conn_str,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB addresses. (Default: \"localhost:11810\")"
                        /*SequoiaDB 连接地址。*/,
                        sdb_conn_addr_validate, NULL, LDB_ADDR_DFT);
static MYSQL_SYSVAR_STR(user, ldb_user,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication user. "
                        "(Default: \"\")"
                        /*SequoiaDB 鉴权用户。*/,
                        NULL, NULL, LDB_USER_DFT);
static MYSQL_SYSVAR_STR(password, ldb_password,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication password. "
                        "(Default: \"\")"
                        /*SequoiaDB 鉴权密码。*/,
                        NULL, ldb_password_update, LDB_PASSWORD_DFT);
static MYSQL_SYSVAR_STR(token, ldb_password_token,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication password token. "
                        "(Default: \"\")"
                        /*SequoiaDB 鉴权加密口令。*/,
                        NULL, NULL, LDB_DEFAULT_TOKEN);
static MYSQL_SYSVAR_STR(cipherfile, ldb_password_cipherfile,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "SequoiaDB authentication cipherfile. "
                        "(Default: \"~/sequoiadb/passwd\")"
                        /*SequoiaDB 鉴权密码文件路径。*/,
                        NULL, NULL, LDB_DEFAULT_CIPHERFILE);
// LDB_DOC_OPT = IGNORE
static MYSQL_SYSVAR_BOOL(use_partition, ldb_auto_partition,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_INVISIBLE,
                         "Create partition table on SequoiaDB. "
                         "(Default: ON). This option is abandoned, please use "
                         "sequoiadb_auto_partition instead."
                         /*是否启用自动分区(已弃用)。*/,
                         NULL, NULL, LDB_USE_PARTITION_DFT);
static MYSQL_SYSVAR_BOOL(auto_partition, ldb_auto_partition,
                         PLUGIN_VAR_OPCMDARG,
                         "Automatically create partition table on SequoiaDB. "
                         "(Default: ON)"
                         /*是否启用自动分区。*/,
                         NULL, NULL, LDB_USE_PARTITION_DFT);
static MYSQL_SYSVAR_BOOL(use_bulk_insert, ldb_use_bulk_insert,
                         PLUGIN_VAR_OPCMDARG,
                         "Enable bulk insert to SequoiaDB. (Default: ON)"
                         /*是否启用批量插入。*/,
                         NULL, NULL, LDB_DEFAULT_USE_BULK_INSERT);
static MYSQL_SYSVAR_INT(bulk_insert_size, ldb_bulk_insert_size,
                        PLUGIN_VAR_OPCMDARG,
                        "Maximum number of records per bulk insert. "
                        "(Default: 2000)"
                        /*批量插入时每批的插入记录数。*/,
                        NULL, NULL, LDB_DEFAULT_BULK_INSERT_SIZE, 1, 100000, 0);
static MYSQL_SYSVAR_INT(replica_size, ldb_replica_size, PLUGIN_VAR_OPCMDARG,
                        "Replica size of write operations. "
                        "(Default: 1)"
                        /*写操作需同步的副本数。取值范围为[-1, 7]。*/,
                        NULL, NULL, LDB_DEFAULT_REPLICA_SIZE, -1, 7, 0);
// LDB_DOC_OPT = IGNORE
static MYSQL_SYSVAR_BOOL(use_autocommit, ldb_use_autocommit,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_INVISIBLE,
                         "Enable autocommit of SequoiaDB storage engine. "
                         "(Default: ON). This option is abandoned, please use "
                         "autocommit instead."
                         /*是否启用自动提交模式(已弃用)。*/,
                         NULL, NULL, LDB_DEFAULT_USE_AUTOCOMMIT);
static MYSQL_SYSVAR_BOOL(debug_log, ldb_debug_log, PLUGIN_VAR_OPCMDARG,
                         "Turn on debug log of SequoiaDB storage engine. "
                         "(Default: OFF)"
                         /*是否打印debug日志。*/,
                         NULL, NULL, LDB_DEBUG_LOG_DFT);
static MYSQL_SYSVAR_ENUM(
    error_level, ldb_error_level, PLUGIN_VAR_RQCMDARG,
    "Sequoiadb error level for updating sharding key error."
    "(Default: error), available choices: error, warning"
    /* 错误级别控制，为error输出错误信息，为warning输出告警信息。*/,
    NULL, NULL, LDB_ERROR, &ldb_error_level_typelib);
// LDB_DOC_OPT = IGNORE
static MYSQL_SYSVAR_BOOL(optimizer_select_count, ldb_optimizer_select_count,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_INVISIBLE,
                         "Optimizer switch for simple select count. "
                         "(Default: ON). This option is abandoned, please use "
                         "sequoiadb-optimizer-options='direct_count' instead."
                         /*是否开启优化select count(*)行为。*/,
                         NULL, NULL, TRUE);
static MYSQL_THDVAR_LONGLONG(
    alter_table_overhead_threshold, PLUGIN_VAR_OPCMDARG,
    "Overhead threshold of table alteration. When count of records exceeds it, "
    "the alteration that needs to update the full table will be prohibited. "
    "(Default: 10000000)."
    /*更改表开销阈值。当表记录数超过这个阈值，需要全表更新的更改操作将被禁止。*/
    ,
    NULL, NULL, LDB_DEFAULT_ALTER_TABLE_OVERHEAD_THRESHOLD, 0, INT_MAX64, 0);
static MYSQL_THDVAR_UINT(selector_pushdown_threshold, PLUGIN_VAR_OPCMDARG,
                         "The threshold of selector push down to SequoiaDB. "
                         "(Default: 30)"
                         /*查询字段下压触发阈值，取值范围[0, 100]，单位：%。*/,
                         NULL, NULL, LDB_DEFAULT_SELECTOR_PUSHDOWN_THRESHOLD, 0,
                         100, 0);
static MYSQL_THDVAR_BOOL(execute_only_in_mysql, PLUGIN_VAR_OPCMDARG,
                         "Commands execute only in mysql. (Default: OFF)"
                         /*DDL 命令只在 MySQL 执行，不下压到 SequoiaDB 执行。*/,
                         NULL, NULL, FALSE);

static MYSQL_THDVAR_BOOL(rollback_on_timeout, PLUGIN_VAR_OPCMDARG,
                         "Roll back the complete transaction on lock wait "
                         "timeout. (Default: OFF)"
                         /*记录锁超时是否中断并回滚整个事务。*/,
                         NULL, NULL, FALSE);

static MYSQL_THDVAR_SET(
    optimizer_options, PLUGIN_VAR_OPCMDARG,
    "Optimizer_options[=option[,option...]], where "
    "option can be 'direct_count', 'direct_delete', 'direct_update'."
    "direct_count: use count() instead of reading records "
    "one by one for count queries. "
    "direct_delete: direct delete without reading records."
    "direct_update: direct update without reading records."
    "(Default: \"direct_count,direct_delete,direct_update\")"
    /*SequoiaDB 优化选项开关，以决定是否优化计数、更新、删除操作。*/,
    NULL, NULL, LDB_OPTIMIZER_OPTIONS_DEFAULT, &ldb_optimizer_options_typelib);

struct st_mysql_sys_var *ldb_sys_vars[] = {
    MYSQL_SYSVAR(conn_addr),
    MYSQL_SYSVAR(user),
    MYSQL_SYSVAR(password),
    MYSQL_SYSVAR(token),
    MYSQL_SYSVAR(cipherfile),
    MYSQL_SYSVAR(use_partition),
    MYSQL_SYSVAR(auto_partition),
    MYSQL_SYSVAR(use_bulk_insert),
    MYSQL_SYSVAR(bulk_insert_size),
    MYSQL_SYSVAR(replica_size),
    MYSQL_SYSVAR(use_autocommit),
    MYSQL_SYSVAR(debug_log),
    MYSQL_SYSVAR(error_level),
    MYSQL_SYSVAR(optimizer_select_count),
    MYSQL_SYSVAR(alter_table_overhead_threshold),
    MYSQL_SYSVAR(selector_pushdown_threshold),
    MYSQL_SYSVAR(execute_only_in_mysql),
    MYSQL_SYSVAR(optimizer_options),
    MYSQL_SYSVAR(rollback_on_timeout),
    NULL};

ha_sdb_conn_addrs::ha_sdb_conn_addrs() : conn_num(0) {
  for (int i = 0; i < LDB_COORD_NUM_MAX; i++) {
    addrs[i] = NULL;
  }
}

ha_sdb_conn_addrs::~ha_sdb_conn_addrs() {
  clear_conn_addrs();
}

void ha_sdb_conn_addrs::clear_conn_addrs() {
  for (int i = 0; i < conn_num; i++) {
    if (addrs[i]) {
      free(addrs[i]);
      addrs[i] = NULL;
    }
  }
  conn_num = 0;
}

int ha_sdb_conn_addrs::parse_conn_addrs(const char *conn_addr) {
  int rc = 0;
  const char *p = conn_addr;

  if (NULL == conn_addr || 0 == strlen(conn_addr)) {
    rc = -1;
    goto error;
  }

  clear_conn_addrs();

  while (*p != 0) {
    const char *p_tmp = NULL;
    size_t len = 0;
    if (conn_num >= LDB_COORD_NUM_MAX) {
      goto done;
    }

    p_tmp = strchr(p, ',');
    if (NULL == p_tmp) {
      len = strlen(p);
    } else {
      len = p_tmp - p;
    }
    if (len > 0) {
      char *p_addr = NULL;
      const char *comma_pos = strchr(p, ',');
      const char *colon_pos = strchr(p, ':');
      if (!colon_pos || (comma_pos && comma_pos < colon_pos)) {
        rc = -1;
        goto error;
      }
      p_addr = (char *)malloc(len + 1);
      if (NULL == p_addr) {
        rc = -1;
        goto error;
      }
      memcpy(p_addr, p, len);
      p_addr[len] = 0;
      addrs[conn_num] = p_addr;
      ++conn_num;
    }
    p += len;
    if (*p == ',') {
      p++;
    }
  }

done:
  return rc;
error:
  goto done;
}

const char **ha_sdb_conn_addrs::get_conn_addrs() const {
  return (const char **)addrs;
}

int ha_sdb_conn_addrs::get_conn_num() const {
  return conn_num;
}

int ldb_encrypt_password() {
  static const uint DISPLAY_MAX_LEN = 1;
  int rc = 0;
  ::String src_password(ldb_password, &my_charset_bin);

  rc = ldb_passwd_encryption.encrypt(src_password, ldb_encoded_password);
  if (rc) {
    goto error;
  }

  for (uint i = 0; i < src_password.length(); ++i) {
    src_password[i] = '*';
  }

  if (src_password.length() > DISPLAY_MAX_LEN) {
    src_password[DISPLAY_MAX_LEN] = 0;
  }
done:
  return rc;
error:
  goto done;
}

int ldb_get_password(::String &res) {
  Sdb_rwlock_read_guard guard(ldb_password_lock);
  return ldb_passwd_encryption.decrypt(ldb_encoded_password, res);
}

uint ldb_selector_pushdown_threshold(THD *thd) {
  return THDVAR(thd, selector_pushdown_threshold);
}

/*
   will not open collection on ldb, make sure called before
   collection's action.
*/
bool ldb_execute_only_in_mysql(THD *thd) {
  return THDVAR(thd, execute_only_in_mysql);
}

longlong ldb_alter_table_overhead_threshold(THD *thd) {
  return THDVAR(thd, alter_table_overhead_threshold);
}

ulonglong ldb_get_optimizer_options(THD *thd) {
  return THDVAR(thd, optimizer_options);
}

bool ldb_rollback_on_timeout(THD *thd) {
  return THDVAR(thd, rollback_on_timeout);
}

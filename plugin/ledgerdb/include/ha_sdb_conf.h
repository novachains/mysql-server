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

#ifndef LDB_CONF__H
#define LDB_CONF__H

#include <my_global.h>
#include "ha_sdb_util.h"
#include <mysql/plugin.h>
#include <sql_string.h>

#define LDB_OPTIMIZER_OPTION_SELECT_COUNT (1ULL << 0)
#define LDB_OPTIMIZER_OPTION_DELETE (1ULL << 1)
#define LDB_OPTIMIZER_OPTION_UPDATE (1ULL << 2)

#define LDB_OPTIMIZER_OPTIONS_DEFAULT                                \
  (LDB_OPTIMIZER_OPTION_SELECT_COUNT | LDB_OPTIMIZER_OPTION_DELETE | \
   LDB_OPTIMIZER_OPTION_UPDATE)

#if MYSQL_VERSION_ID >= 50725
#define LDB_INVISIBLE | PLUGIN_VAR_INVISIBLE
#else
#define LDB_INVISIBLE
#endif

#define LDB_COORD_NUM_MAX 128
class ha_sdb_conn_addrs {
 public:
  ha_sdb_conn_addrs();
  ~ha_sdb_conn_addrs();

  int parse_conn_addrs(const char *conn_addrs);

  const char **get_conn_addrs() const;

  int get_conn_num() const;

 private:
  ha_sdb_conn_addrs(const ha_sdb_conn_addrs &rh) {}

  ha_sdb_conn_addrs &operator=(const ha_sdb_conn_addrs &rh) { return *this; }

  void clear_conn_addrs();

 private:
  char *addrs[LDB_COORD_NUM_MAX];
  int conn_num;
};

int ldb_encrypt_password();
int ldb_get_password(String &res);
uint ldb_selector_pushdown_threshold(THD *thd);
bool ldb_execute_only_in_mysql(THD *thd);
longlong ldb_alter_table_overhead_threshold(THD *thd);
ulonglong ldb_get_optimizer_options(THD *thd);
bool ldb_rollback_on_timeout(THD *thd);

extern char *sdb_conn_str;
extern char *ldb_user;
extern char *ldb_password_token;
extern char *ldb_password_cipherfile;
extern my_bool ldb_auto_partition;
extern my_bool ldb_use_bulk_insert;
extern int ldb_bulk_insert_size;
extern int ldb_replica_size;
extern my_bool ldb_use_autocommit;
extern my_bool ldb_debug_log;
extern st_mysql_sys_var *ldb_sys_vars[];
extern ulong ldb_error_level;

extern String ldb_encoded_password;

#endif

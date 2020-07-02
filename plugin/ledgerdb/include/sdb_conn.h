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

#ifndef LDB_CONN__H
#define LDB_CONN__H

#include "ha_sdb_sql.h"
#include <client.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include "ha_sdb_def.h"

#if defined IS_MYSQL
#include <my_thread_local.h>
#elif defined IS_MARIADB
#include <my_pthread.h>
#endif

class Sdb_cl;
class Sdb_statistics;
const static bson::BSONObj ldbStaticObject;
class Sdb_conn {
 public:
  Sdb_conn(my_thread_id _tid);

  ~Sdb_conn();

  int connect();
  void disconnect();

  ldbclient::ldb &get_ldb();

  my_thread_id thread_id();

  int begin_transaction();

  int commit_transaction(const bson::BSONObj &hint = LDB_EMPTY_BSON);

  int rollback_transaction();

  bool is_transaction_on();

  int get_cl(char *cs_name, char *cl_name, Sdb_cl &cl);

  int create_cl(char *cs_name, char *cl_name,
                const bson::BSONObj &options = LDB_EMPTY_BSON,
                bool *created_cs = NULL, bool *created_cl = NULL);

  int rename_cl(char *cs_name, char *old_cl_name, char *new_cl_name);

  int drop_cl(char *cs_name, char *cl_name);

  int drop_cs(char *cs_name);

  int get_cl_statistics(char *cs_name, char *cl_name, Sdb_statistics &stats);

  int snapshot(bson::BSONObj &obj, int snap_type,
               const bson::BSONObj &condition = LDB_EMPTY_BSON,
               const bson::BSONObj &selected = LDB_EMPTY_BSON,
               const bson::BSONObj &orderBy = LDB_EMPTY_BSON,
               const bson::BSONObj &hint = LDB_EMPTY_BSON,
               longlong numToSkip = 0);

  int get_last_result_obj(bson::BSONObj &result, bool get_owned = false);

  int set_session_attr(const bson::BSONObj &option);

  int interrupt_operation();

  inline bool is_valid() { return m_connection.isValid(); }

  inline void set_pushed_autocommit() { pushed_autocommit = true; }

  inline bool get_pushed_autocommit() { return pushed_autocommit; }

  int get_last_error(bson::BSONObj &errObj) {
    return m_connection.getLastErrorObj(errObj);
  };

  int getDigest(bson::BSONObj &result, bson::BSONObj addr = ldbStaticObject);

 private:
  int retry(boost::function<int()> func);

 private:
  ldbclient::ldb m_connection;
  bool m_transaction_on;
  my_thread_id m_thread_id;
  bool pushed_autocommit;
};

#endif

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

#ifndef LDB_THD__H
#define LDB_THD__H

#include <mysql/plugin.h>
#include <client.hpp>
#include <partition_element.h>
#include "sdb_conn.h"
#include <boost/shared_ptr.hpp>

extern handlerton* ldb_hton;
struct Sdb_share;

class Sdb_cl_copyer;
#ifdef IS_MYSQL
class Sdb_part_alter_ctx;
#endif

struct Sdb_local_table_statistics {
  int no_uncommitted_rows_count;
};

typedef struct st_thd_ldb_share {
  boost::shared_ptr<Sdb_share> share_ptr;
  struct Sdb_local_table_statistics stat;
} THD_LDB_SHARE;

class Thd_ldb {
 private:
  Thd_ldb(THD* thd);
  ~Thd_ldb();

 public:
  static Thd_ldb* seize(THD* thd);
  static void release(Thd_ldb* thd_ldb);

  int recycle_conn();
  inline my_thread_id thread_id() const { return m_thread_id; }
  inline bool is_slave_thread() const { return m_slave_thread; }
  inline Sdb_conn* get_conn() { return &m_conn; }
  inline bool valid_conn() { return m_conn.is_valid(); }
  bool get_auto_commit() { return auto_commit; }
  void set_auto_commit(bool all) { auto_commit = all; }

  uint lock_count;
  uint start_stmt_count;
  uint save_point_count;
  ulonglong found;
  ulonglong updated;
  ulonglong deleted;
  ulonglong duplicated;
  bool replace_on_dup;

  // store stats info for each open table share
  // update stats of m_share after transaction commit
  HASH open_table_shares;

  // For ALTER TABLE in ALGORITHM COPY
  Sdb_cl_copyer* cl_copyer;

#ifdef IS_MYSQL
  Sdb_part_alter_ctx* part_alter_ctx;
#endif

 private:
  THD* m_thd;
  my_thread_id m_thread_id;
  const bool m_slave_thread;  // cached value of m_thd->slave_thread
  Sdb_conn m_conn;
  bool auto_commit;
};

// Set Thd_ldb pointer for THD
static inline void thd_set_thd_ldb(THD* thd, Thd_ldb* thd_ldb) {
  thd_set_ha_data(thd, ldb_hton, thd_ldb);
}

// Get Thd_ldb pointer from THD
static inline Thd_ldb* thd_get_thd_ldb(THD* thd) {
  return (Thd_ldb*)thd_get_ha_data(thd, ldb_hton);
}

// Make sure THD has a Thd_ldb struct assigned
int check_ldb_in_thd(THD* thd, Sdb_conn** conn, bool validate_conn = false);

#endif /* LDB_THD__H */

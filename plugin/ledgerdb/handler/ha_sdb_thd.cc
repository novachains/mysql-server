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

#include <my_global.h>
#include <sql_class.h>
#include <my_base.h>
#include "ha_sdb_thd.h"
#include "ha_sdb_log.h"
#include "ha_sdb_errcode.h"

uchar* thd_ldb_share_get_key(THD_LDB_SHARE* thd_ldb_share, size_t* length,
                             my_bool not_used MY_ATTRIBUTE((unused))) {
  *length = sizeof(thd_ldb_share->share_ptr.get());
  return (uchar*)thd_ldb_share->share_ptr.get();
}

extern void free_thd_open_shares_elem(void* share_ptr);

Thd_ldb::Thd_ldb(THD* thd)
    : m_thd(thd),
      m_slave_thread(thd->slave_thread),
      m_conn(thd_get_thread_id(thd)) {
  m_thread_id = thd_get_thread_id(thd);
  lock_count = 0;
  auto_commit = false;
  start_stmt_count = 0;
  save_point_count = 0;
  found = 0;
  updated = 0;
  deleted = 0;
  duplicated = 0;
  cl_copyer = NULL;
#ifdef IS_MYSQL
  part_alter_ctx = NULL;
#endif

  (void)ldb_hash_init(&open_table_shares, table_alias_charset, 5, 0, 0,
                      (my_hash_get_key)thd_ldb_share_get_key,
                      free_thd_open_shares_elem, 0, PSI_INSTRUMENT_ME);
}

Thd_ldb::~Thd_ldb() {
  my_hash_free(&open_table_shares);
}

Thd_ldb* Thd_ldb::seize(THD* thd) {
  Thd_ldb* thd_ldb = new (std::nothrow) Thd_ldb(thd);
  if (NULL == thd_ldb) {
    return NULL;
  }

  return thd_ldb;
}

void Thd_ldb::release(Thd_ldb* thd_ldb) {
  delete thd_ldb;
}

int Thd_ldb::recycle_conn() {
  int rc = LDB_ERR_OK;
  rc = m_conn.connect();
  return rc;
}

// Make sure THD has a Thd_ldb struct allocated and associated
int check_ldb_in_thd(THD* thd, Sdb_conn** conn, bool validate_conn) {
  int rc = 0;
  Thd_ldb* thd_ldb = thd_get_thd_ldb(thd);
  if (NULL == thd_ldb) {
    thd_ldb = Thd_ldb::seize(thd);
    if (NULL == thd_ldb) {
      rc = HA_ERR_OUT_OF_MEM;
      return rc;
    }
    thd_set_thd_ldb(thd, thd_ldb);
  }

  if (validate_conn && !thd_ldb->valid_conn()) {
    rc = thd_ldb->recycle_conn();
    if (rc != 0) {
      return rc;
    }
  }

  DBUG_ASSERT(thd_ldb->is_slave_thread() == thd->slave_thread);
  *conn = thd_ldb->get_conn();
  return rc;
}

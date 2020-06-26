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

#include "sdb_conn.h"
#include <sql_class.h>
#include <client.hpp>
#include <sstream>
#include "sdb_cl.h"
#include "ha_sdb_conf.h"
#include "ha_sdb_util.h"
#include "ha_sdb_errcode.h"
#include "ha_sdb_conf.h"
#include "ha_sdb_log.h"
#include "ha_sdb.h"
#include "ha_sdb_def.h"

static int ldb_proc_id() {
#ifdef _WIN32
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

Sdb_conn::Sdb_conn(my_thread_id _tid)
    : m_transaction_on(false), m_thread_id(_tid), pushed_autocommit(false) {}

Sdb_conn::~Sdb_conn() {}

ldbclient::ldb &Sdb_conn::get_ldb() {
  return m_connection;
}

my_thread_id Sdb_conn::thread_id() {
  return m_thread_id;
}

int Sdb_conn::retry(boost::function<int()> func) {
  int rc = LDB_ERR_OK;
  int retry_times = 2;
retry:
  rc = func();
  if (rc != LDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  if (IS_LDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_ldb_code(rc);
  goto done;
}

int Sdb_conn::connect() {
  int rc = LDB_ERR_OK;
  String password;
  bson::BSONObj option;
  const char *hostname = NULL;
  char source_str[PREFIX_THREAD_ID_LEN + HOST_NAME_MAX + 64] = {0};
  // 64 bytes is for string of proc_id and thread_id.

  int hostname_len = (int)strlen(glob_hostname);

  if (0 >= hostname_len) {
    static char empty[] = "";
    hostname = empty;
  } else {
    hostname = glob_hostname;
  }

  if (!m_connection.isValid()) {
    m_transaction_on = false;
    ha_sdb_conn_addrs conn_addrs;
    rc = conn_addrs.parse_conn_addrs(sdb_conn_str);
    if (LDB_ERR_OK != rc) {
      LDB_LOG_ERROR("Failed to parse connection addresses, rc=%d", rc);
      goto error;
    }

    rc = ldb_get_password(password);
    if (LDB_ERR_OK != rc) {
      LDB_LOG_ERROR("Failed to decrypt password, rc=%d", rc);
      goto error;
    }
    if(password.length()) {
      rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                conn_addrs.get_conn_num(), ldb_user,
                                password.ptr());
    } else {
      rc = m_connection.connect(conn_addrs.get_conn_addrs(),
                                conn_addrs.get_conn_num(), ldb_user,
                                ldb_password_token, ldb_password_cipherfile);
    }
    if (LDB_ERR_OK != rc) {
        switch (rc) {
          case LDB_FNE:
            LDB_LOG_ERROR("Cipherfile not exist, rc=%d", rc);
            rc = LDB_AUTH_AUTHORITY_FORBIDDEN;
            break; 
          case LDB_AUTH_USER_NOT_EXIST:
            LDB_LOG_ERROR("User specified is not exist, you can add the user by ldbpasswd tool, rc=%d", rc);
            rc = LDB_AUTH_AUTHORITY_FORBIDDEN;
            break; 
          default:
            break;
      }
      LDB_LOG_ERROR("Failed to connect to sequoiadb, rc=%d", rc);
      goto error;
    }

    snprintf(source_str, sizeof(source_str), "%s%s%s:%d:%llu", PREFIX_THREAD_ID,
             strlen(hostname) ? ":" : "", hostname, ldb_proc_id(),
             (ulonglong)thread_id());
    bool auto_commit = true;
    /* TODO
    option = BSON(SOURCE_THREAD_ID << source_str << TRANSAUTOROLLBACK << false
                                   << TRANSAUTOCOMMIT << auto_commit);
    rc = set_session_attr(option);
    if (LDB_ERR_OK != rc) {
      LDB_LOG_ERROR("Failed to set session attr, rc=%d", rc);
      goto error;
    }*/
  }

done:
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

void Sdb_conn::disconnect() {
  m_connection.disconnect();
}
int Sdb_conn::begin_transaction() {
  DBUG_ENTER("Sdb_conn::begin_transaction");
  int rc = LDB_ERR_OK;
  int retry_times = 2;

  while (!m_transaction_on) {
    if (pushed_autocommit) {
      m_transaction_on = true;
    } else {
      rc = m_connection.transactionBegin();
      if (LDB_ERR_OK == rc) {
        m_transaction_on = true;
      } else if (IS_LDB_NET_ERR(rc) && --retry_times > 0) {
        connect();
      } else {
        goto error;
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Begin transaction, flag: %d", pushed_autocommit));
  }

done:
  DBUG_RETURN(rc);
error:
  convert_ldb_code(rc);
  goto done;
}

int Sdb_conn::commit_transaction(const bson::BSONObj &hint) {
  DBUG_ENTER("Sdb_conn::commit_transaction");
  int rc = LDB_ERR_OK;
  if (m_transaction_on) {
    m_transaction_on = false;
    if (!pushed_autocommit) {
      rc = m_connection.transactionCommit();
      if (rc != LDB_ERR_OK) {
        goto error;
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Commit transaction, flag: %d", pushed_autocommit));
    pushed_autocommit = false;
  }

done:
  DBUG_RETURN(rc);
error:
  if (IS_LDB_NET_ERR(rc)) {
    connect();
  }
  convert_ldb_code(rc);
  goto done;
}

int Sdb_conn::rollback_transaction() {
  DBUG_ENTER("Sdb_conn::rollback_transaction");
  if (m_transaction_on) {
    int rc = LDB_ERR_OK;
    m_transaction_on = false;
    if (!pushed_autocommit) {
      rc = m_connection.transactionRollback();
      if (IS_LDB_NET_ERR(rc)) {
        connect();
      }
    }
    DBUG_PRINT("Sdb_conn::info",
               ("Rollback transaction, flag: %d", pushed_autocommit));
    pushed_autocommit = false;
  }
  DBUG_RETURN(0);
}

bool Sdb_conn::is_transaction_on() {
  return m_transaction_on;
}

int Sdb_conn::get_cl(char *cs_name, char *cl_name, Sdb_cl &cl) {
  int rc = LDB_ERR_OK;
  cl.close();

  rc = cl.init(this, cs_name, cl_name);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  if (IS_LDB_NET_ERR(rc)) {
    connect();
  }
  convert_ldb_code(rc);
  goto done;
}

int Sdb_conn::create_cl(char *cs_name, char *cl_name,
                        const bson::BSONObj &options, bool *created_cs,
                        bool *created_cl) {
  int rc = LDB_ERR_OK;
  int retry_times = 2;
  ldbclient::ldbCollectionSpace cs;
  std::unique_ptr<ldbclient::ldbCollection> clPtr;
  ldbclient::ldbCollection *cl = nullptr;
  clPtr.reset(cl);
  bool new_cs = false;
  bool new_cl = false;

retry:
  rc = m_connection.getCollectionSpace(cs_name, cs);
  if (LDB_DMS_CS_NOTEXIST == rc) {
    bson::BSONObj obj;
    rc = m_connection.createCollectionSpace(cs_name, obj, cs);
    if (LDB_OK == rc) {
      new_cs = true;
    }
  }

  if (LDB_ERR_OK != rc && LDB_DMS_CS_EXIST != rc) {
    goto error;
  }

  rc = cs.createCollection(cl_name, options, &cl);
  if (LDB_DMS_EXIST == rc) {
    rc = cs.getCollection(cl_name, &cl);
    /* CS cached on ldbclient. so LDB_DMS_CS_NOTEXIST maybe retuned here. */
  } else if (LDB_OK == rc) {
    new_cl = true;
  }

  if (rc != LDB_ERR_OK) {
    goto error;
  }

done:
  if (created_cs) {
    *created_cs = new_cs;
  }
  if (created_cl) {
    *created_cl = new_cl;
  }
  return rc;
error:
  if (IS_LDB_NET_ERR(rc)) {
    if (!m_transaction_on && retry_times-- > 0 && 0 == connect()) {
      goto retry;
    }
  }
  convert_ldb_code(rc);
  if (new_cs) {
    drop_cs(cs_name);
    new_cs = false;
    new_cl = false;
  } else if (new_cl) {
    drop_cl(cs_name, cl_name);
    new_cl = false;
  }
  goto done;
}

int conn_rename_cl(ldbclient::ldb *connection, char *cs_name, char *old_cl_name,
                   char *new_cl_name) {
  int rc = LDB_ERR_OK;
  ldbclient::ldbCollectionSpace cs;

  rc = connection->getCollectionSpace(cs_name, cs);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  rc = cs.renameCollection(old_cl_name, new_cl_name);
  if (rc != LDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::rename_cl(char *cs_name, char *old_cl_name, char *new_cl_name) {
  return retry(boost::bind(conn_rename_cl, &m_connection, cs_name, old_cl_name,
                           new_cl_name));
}

int conn_drop_cl(ldbclient::ldb *connection, char *cs_name, char *cl_name) {
  int rc = LDB_ERR_OK;
  ldbclient::ldbCollectionSpace cs;

  rc = connection->getCollectionSpace(cs_name, cs);
  if (rc != LDB_ERR_OK) {
    if (LDB_DMS_CS_NOTEXIST == rc) {
      // There is no specified collection space, igonre the error.
      rc = 0;
      goto done;
    }
    goto error;
  }

  rc = cs.dropCollection(cl_name);
  if (rc != LDB_ERR_OK) {
    if (LDB_DMS_NOTEXIST == rc) {
      // There is no specified collection, igonre the error.
      rc = 0;
      goto done;
    }
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_conn::drop_cl(char *cs_name, char *cl_name) {
  return retry(boost::bind(conn_drop_cl, &m_connection, cs_name, cl_name));
}

int conn_drop_cs(ldbclient::ldb *connection, char *cs_name) {
  int rc = connection->dropCollectionSpace(cs_name);
  if (LDB_DMS_CS_NOTEXIST == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_conn::drop_cs(char *cs_name) {
  return retry(boost::bind(conn_drop_cs, &m_connection, cs_name));
}
/*
int conn_exec(ldbclient::ldb *connection, const char *sql,
              ldbclient::ldbCursor *cursor) {
  return connection->exec(sql, *cursor);
}
*/
int Sdb_conn::get_cl_statistics(char *cs_name, char *cl_name,
                                Sdb_statistics &stats) {
  static const int PAGE_SIZE_MIN = 4096;
  static const int PAGE_SIZE_MAX = 65536;

  int rc = LDB_ERR_OK;
  ldbclient::ldbCursor cursor;
  bson::BSONObj obj;
  Sdb_cl cl;

  DBUG_ASSERT(NULL != cs_name);
  DBUG_ASSERT(strlength(cs_name) != 0);
/*
  rc = get_cl(cs_name, cl_name, cl);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  rc = cl.get_detail(cursor);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  stats.page_size = PAGE_SIZE_MAX;
  stats.total_data_pages = 0;
  stats.total_index_pages = 0;
  stats.total_data_free_space = 0;
  stats.total_records = 0;

  while (!(rc = cursor.next(obj, false))) {
    try {
      bson::BSONObjIterator it(obj.getField(LDB_FIELD_DETAILS).Obj());
      if (!it.more()) {
        continue;
      }
      bson::BSONObj detail = it.next().Obj();
      bson::BSONObjIterator iter(detail);

      int page_size = 0;
      int total_data_pages = 0;
      int total_index_pages = 0;
      longlong total_data_free_space = 0;
      longlong total_records = 0;

      while (iter.more()) {
        bson::BSONElement ele = iter.next();
        if (!strcmp(ele.fieldName(), LDB_FIELD_PAGE_SIZE)) {
          page_size = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), LDB_FIELD_TOTAL_DATA_PAGES)) {
          total_data_pages = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), LDB_FIELD_TOTAL_INDEX_PAGES)) {
          total_index_pages = ele.numberInt();
        } else if (!strcmp(ele.fieldName(), LDB_FIELD_TOTAL_DATA_FREE_SPACE)) {
          total_data_free_space = ele.numberLong();
        } else if (!strcmp(ele.fieldName(), LDB_FIELD_TOTAL_RECORDS)) {
          total_records = ele.numberLong();
        }
      }

      // When exception occurs, page size may be 0. Fix it to default.
      if (0 == page_size) {
        page_size = PAGE_SIZE_MAX;
      }
      // For main cl, each data node may have different page size,
      // so calculate pages base on the min page size.
      if (page_size < stats.page_size) {
        stats.page_size = page_size;
      }
      stats.total_data_pages +=
          (total_data_pages * (page_size / PAGE_SIZE_MIN));
      stats.total_index_pages +=
          (total_index_pages * (page_size / PAGE_SIZE_MIN));

      stats.total_data_free_space += total_data_free_space;
      stats.total_records += total_records;

    } catch (bson::assertion &e) {
      DBUG_ASSERT(false);
      LDB_LOG_ERROR("Cannot parse collection detail info. %s", e.what());
      rc = LDB_SYS;
      goto error;
    }
  }
  if (LDB_DMS_EOC == rc) {
    rc = LDB_ERR_OK;
  }
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  stats.total_data_pages /= (stats.page_size / PAGE_SIZE_MIN);
  stats.total_index_pages /= (stats.page_size / PAGE_SIZE_MIN);
*/
done:
  cursor.close();
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

int conn_snapshot(ldbclient::ldb *connection, bson::BSONObj *obj, int snap_type,
                  const bson::BSONObj *condition, const bson::BSONObj *selected,
                  const bson::BSONObj *order_by, const bson::BSONObj *hint,
                  longlong num_to_skip) {
/*  int rc = LDB_ERR_OK;
  ldbclient::ldbCursor cursor;

  rc = connection->getSnapshot(cursor, snap_type, *condition, *selected,
                               *order_by, *hint, num_to_skip, 1);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  rc = cursor.next(*obj);
  if (rc != LDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  goto done;*/
}

int Sdb_conn::snapshot(bson::BSONObj &obj, int snap_type,
                       const bson::BSONObj &condition,
                       const bson::BSONObj &selected,
                       const bson::BSONObj &order_by, const bson::BSONObj &hint,
                       longlong num_to_skip) {
  return retry(boost::bind(conn_snapshot, &m_connection, &obj, snap_type,
                           &condition, &selected, &order_by, &hint,
                           num_to_skip));
}

int conn_get_last_result_obj(ldbclient::ldb *connection, bson::BSONObj *result,
                             bool get_owned) {
  return connection->getLastResultObj(*result, get_owned);
}

int Sdb_conn::get_last_result_obj(bson::BSONObj &result, bool get_owned) {
  return retry(
      boost::bind(conn_get_last_result_obj, &m_connection, &result, get_owned));
}

int conn_set_session_attr(ldbclient::ldb *connection,
                          const bson::BSONObj *option) {
  return connection->setSessionAttr(*option);
}

int Sdb_conn::set_session_attr(const bson::BSONObj &option) {
  return retry(boost::bind(conn_set_session_attr, &m_connection, &option));
}

int conn_interrupt(ldbclient::ldb *connection) {
  return connection->interruptOperation();
}

int Sdb_conn::interrupt_operation() {
  return retry(boost::bind(conn_interrupt, &m_connection));
}

int Sdb_conn::getDigest(bson::BSONObj &result, bson::BSONObj addr){
  return m_connection.getDigest(result, addr);
}

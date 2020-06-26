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
#include <my_base.h>
#include "sdb_cl.h"
#include "sdb_conn.h"
#include "ha_sdb_errcode.h"

using namespace ldbclient;

Sdb_cl::Sdb_cl() : m_conn(nullptr), m_thread_id(0), m_cl(nullptr), m_cursor(nullptr) {}

Sdb_cl::~Sdb_cl() {
  close();
}

int Sdb_cl::retry(boost::function<int()> func) {
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
    bool is_transaction = m_conn->is_transaction_on();
    if (!is_transaction && retry_times-- > 0 && 0 == m_conn->connect()) {
      goto retry;
    }
  }
  convert_ldb_code(rc);
  goto done;
}

int cl_init(ldbclient::ldbCollection **cl, Sdb_conn *connection, char *cs_name,
            char *cl_name) {
  int rc = LDB_ERR_OK;
  ldbCollectionSpace cs;

  rc = connection->get_ldb().getCollectionSpace(cs_name, cs);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  rc = cs.getCollection(cl_name, cl);
  if (rc != LDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_cl::init(Sdb_conn *connection, char *cs_name, char *cl_name) {
  int rc = LDB_ERR_OK;

  if (nullptr == connection || nullptr == cs_name || nullptr == cl_name) {
    rc = LDB_ERR_INVALID_ARG;
    goto error;
  }

  m_conn = connection;
  m_thread_id = connection->thread_id();

  rc = retry(boost::bind(cl_init, &m_cl, connection, cs_name, cl_name));
done:
  return rc;
error:
  goto done;
}

bool Sdb_cl::is_transaction_on() {
  return m_conn->is_transaction_on();
}

const char *Sdb_cl::get_cs_name() {
  return m_cl->getCSName();
}

const char *Sdb_cl::get_cl_name() {
  return m_cl->getCollectionName();
}

int cl_query(ldbclient::ldbCollection *cl, ldbclient::ldbCursor **cursor,
             const bson::BSONObj *condition, const bson::BSONObj *selected,
             const bson::BSONObj *order_by, const bson::BSONObj *hint,
             longlong num_to_skip, longlong num_to_return, int flags) {
  return cl->query(cursor, *condition, *selected, *order_by, *hint,
                   num_to_skip, num_to_return, flags);
}

int Sdb_cl::query(const bson::BSONObj &condition, const bson::BSONObj &selected,
                  const bson::BSONObj &order_by, const bson::BSONObj &hint,
                  longlong num_to_skip, longlong num_to_return, int flags) {
  if (strcmp(m_cl->getFullName(), "ledgerdb.transHistory") == 0)
  {
     int trxId = 0;
     trxId = condition.getField("trxId").numberInt();
     return m_conn->get_ldb().transHistory(&m_cursor, trxId);
  }
  else {
     return retry(boost::bind(cl_query, m_cl, &m_cursor, &condition, &selected,
                              &order_by, &hint, num_to_skip, num_to_return,
                              flags));
  }
}

int cl_query_one(ldbclient::ldbCollection *cl, bson::BSONObj *obj,
                 const bson::BSONObj *condition, const bson::BSONObj *selected,
                 const bson::BSONObj *order_by, const bson::BSONObj *hint,
                 longlong num_to_skip, int flags) {
  int rc = LDB_ERR_OK;
  std::unique_ptr<ldbclient::ldbCursor> cursor;
  ldbclient::ldbCursor *cursor_tmp = cursor.get();
  rc = cl->query(&cursor_tmp, *condition, *selected, *order_by, *hint,
                 num_to_skip, 1, flags);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

  rc = cursor_tmp->next(*obj);
  if (rc != LDB_ERR_OK) {
    goto error;
  }
done:
  return rc;
error:
  goto done;
}

int Sdb_cl::query_one(bson::BSONObj &obj, const bson::BSONObj &condition,
                      const bson::BSONObj &selected,
                      const bson::BSONObj &order_by, const bson::BSONObj &hint,
                      longlong num_to_skip, int flags) {
  return retry(boost::bind(cl_query_one, m_cl, &obj, &condition, &selected,
                           &order_by, &hint, num_to_skip, flags));
}

int cl_query_and_remove(ldbclient::ldbCollection *cl,
                        ldbclient::ldbCursor *cursor,
                        const bson::BSONObj *condition,
                        const bson::BSONObj *selected,
                        const bson::BSONObj *order_by,
                        const bson::BSONObj *hint, longlong num_to_skip,
                        longlong num_to_return, int flags) {
  return cl->queryAndRemove(&cursor, *condition, *selected, *order_by, *hint,
                            num_to_skip, num_to_return, flags);
}

int Sdb_cl::query_and_remove(const bson::BSONObj &condition,
                             const bson::BSONObj &selected,
                             const bson::BSONObj &order_by,
                             const bson::BSONObj &hint, longlong num_to_skip,
                             longlong num_to_return, int flags) {
  return retry(boost::bind(cl_query_and_remove, m_cl, m_cursor, &condition,
                           &selected, &order_by, &hint, num_to_skip,
                           num_to_return, flags));
}

int Sdb_cl::current(bson::BSONObj &obj, my_bool get_owned) {
  int rc = LDB_ERR_OK;
  rc = m_cursor->current(obj, get_owned);
  if (rc != LDB_ERR_OK) {
    if (LDB_DMS_EOC == rc) {
      rc = HA_ERR_END_OF_FILE;
    }
    goto error;
  }

done:
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

int Sdb_cl::next(bson::BSONObj &obj, my_bool get_owned) {
  int rc = LDB_ERR_OK;
  rc = m_cursor->next(obj, get_owned);
  if (rc != LDB_ERR_OK) {
    if (LDB_DMS_EOC == rc) {
      rc = HA_ERR_END_OF_FILE;
    }
    goto error;
  }

done:
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

int cl_insert(ldbclient::ldbCollection *cl, bson::BSONObj *obj,
              bson::BSONObj &hint, int flag, bson::BSONObj *result) {
  return cl->insert(*obj, flag, result);
}

int Sdb_cl::insert(bson::BSONObj &obj, bson::BSONObj &hint, int flag,
                   bson::BSONObj *result) {
  return retry(boost::bind(cl_insert, m_cl, &obj, hint, flag, result));
}

int Sdb_cl::insert(std::vector<bson::BSONObj> &objs, bson::BSONObj &hint,
                   int flag, bson::BSONObj *result) {
  int rc = LDB_ERR_OK;

  rc = m_cl->insert(objs, flag, result);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

int Sdb_cl::bulk_insert(int flag, std::vector<bson::BSONObj> &objs) {
  int rc = LDB_ERR_OK;

  rc = m_cl->bulkInsert(flag, objs);
  if (rc != LDB_ERR_OK) {
    goto error;
  }

done:
  return rc;
error:
  convert_ldb_code(rc);
  goto done;
}

int cl_upsert(ldbclient::ldbCollection *cl, const bson::BSONObj *rule,
              const bson::BSONObj *condition, const bson::BSONObj *hint,
              const bson::BSONObj *set_on_insert, int flag,
              bson::BSONObj *result) {
  return cl->upsert(*rule, *condition, *hint, *set_on_insert, flag, result);
}

int Sdb_cl::upsert(const bson::BSONObj &rule, const bson::BSONObj &condition,
                   const bson::BSONObj &hint,
                   const bson::BSONObj &set_on_insert, int flag,
                   bson::BSONObj *result) {
  return retry(boost::bind(cl_upsert, m_cl, &rule, &condition, &hint,
                           &set_on_insert, flag, result));
}

int cl_update(ldbclient::ldbCollection *cl, const bson::BSONObj *rule,
              const bson::BSONObj *condition, const bson::BSONObj *hint,
              int flag, bson::BSONObj *result) {
  return cl->update(*rule, *condition, *hint, flag, result);
}

int Sdb_cl::update(const bson::BSONObj &rule, const bson::BSONObj &condition,
                   const bson::BSONObj &hint, int flag, bson::BSONObj *result) {
  return retry(
      boost::bind(cl_update, m_cl, &rule, &condition, &hint, flag, result));
}

int cl_del(ldbclient::ldbCollection *cl, const bson::BSONObj *condition,
           const bson::BSONObj *hint, int flag, bson::BSONObj *result) {
  return cl->del(*condition, *hint, flag, result);
}

int Sdb_cl::del(const bson::BSONObj &condition, const bson::BSONObj &hint,
                int flag, bson::BSONObj *result) {
  return retry(boost::bind(cl_del, m_cl, &condition, &hint, flag, result));
}

int cl_create_index(ldbclient::ldbCollection *cl,
                    const bson::BSONObj *index_def, const char *name,
                    my_bool is_unique, my_bool is_enforced) {
  int rc = cl->createIndex(*index_def, name, is_unique, is_enforced);
  if (LDB_IXM_REDEF == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_cl::create_index(const bson::BSONObj &index_def, const char *name,
                         my_bool is_unique, my_bool is_enforced) {
  return retry(boost::bind(cl_create_index, m_cl, &index_def, name, is_unique,
                           is_enforced));
}

/*
   Test if index is created by v3.2.2 or earlier.
*/
bool is_old_version_index(ldbclient::ldbCollection *cl,
                          const bson::BSONObj &index_def, const char *name,
                          const bson::BSONObj &options) {
  bool rs = false;
  bson::BSONObj info;
  try {
    do {
      int rc = LDB_ERR_OK;
      rc = cl->getIndex(name, info);
      if (rc != LDB_ERR_OK) {
        break;
      }

      bson::BSONObj def_obj = info.getField(LDB_FIELD_IDX_DEF).Obj();
      bson::BSONObj key_obj = def_obj.getField(LDB_FIELD_KEY).Obj();
      if (!key_obj.equal(index_def)) {
        break;
      }

      bool opt_unique = options.getField(LDB_FIELD_UNIQUE).booleanSafe();
      bool def_unique = def_obj.getField(LDB_FIELD_UNIQUE2).booleanSafe();
      if (opt_unique != def_unique) {
        break;
      }

      bool opt_not_null = options.getField(LDB_FIELD_NOT_NULL).booleanSafe();
      bool def_not_null = def_obj.getField(LDB_FIELD_NOT_NULL).booleanSafe();
      if (!(opt_not_null && !def_not_null)) {
        break;
      }

      rs = true;

    } while (0);
  } catch (bson::assertion e) {
    DBUG_ASSERT(false);
  }

  return rs;
}

int cl_create_index2(ldbclient::ldbCollection *cl,
                     const bson::BSONObj *index_def, const char *name,
                     const bson::BSONObj *options) {
  int rc = cl->createIndex(*index_def, name, *options);
  if (LDB_IXM_REDEF == rc ||
      (LDB_IXM_EXIST == rc &&
       is_old_version_index(cl, *index_def, name, *options))) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_cl::create_index(const bson::BSONObj &index_def, const char *name,
                         const bson::BSONObj &options) {
  return retry(
      boost::bind(cl_create_index2, m_cl, &index_def, name, &options));
}

int cl_drop_index(ldbclient::ldbCollection *cl, const char *name) {
  int rc = cl->dropIndex(name);
  if (LDB_IXM_NOTEXIST == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_cl::drop_index(const char *name) {
  return retry(boost::bind(cl_drop_index, m_cl, name));
}

int cl_truncate(ldbclient::ldbCollection *cl) {
  return cl->truncate();
}

int Sdb_cl::truncate() {
  return retry(boost::bind(cl_truncate, m_cl));
}

int cl_set_attributes(ldbclient::ldbCollection *cl,
                      const bson::BSONObj *options) {
  return cl->setAttributes(*options);
}

int Sdb_cl::set_attributes(const bson::BSONObj &options) {
  return retry(boost::bind(cl_set_attributes, m_cl, &options));
}

int cl_drop_auto_increment(ldbclient::ldbCollection *cl,
                           const char *field_name) {
  int rc = cl->dropAutoIncrement(field_name);
  if (LDB_AUTOINCREMENT_FIELD_NOT_EXIST == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_cl::drop_auto_increment(const char *field_name) {
  return retry(boost::bind(cl_drop_auto_increment, m_cl, field_name));
}

int cl_create_auto_increment(ldbclient::ldbCollection *cl,
                             const bson::BSONObj *options) {
  int rc = cl->createAutoIncrement(*options);
  if (LDB_AUTOINCREMENT_FIELD_CONFLICT == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;
}

int Sdb_cl::create_auto_increment(const bson::BSONObj &options) {
  return retry(boost::bind(cl_create_auto_increment, m_cl, &options));
}

void Sdb_cl::close() {
  if (m_cursor) { m_cursor->close(); }
}

my_thread_id Sdb_cl::thread_id() {
  return m_thread_id;
}

int cl_drop(ldbclient::ldbCollection *cl) {
/*  int rc = cl->drop();
  if (LDB_DMS_NOTEXIST == rc) {
    rc = LDB_ERR_OK;
  }
  return rc;*/
}

int Sdb_cl::drop() {
  return retry(boost::bind(cl_drop, m_cl));
}

int cl_get_count(ldbclient::ldbCollection *cl, longlong &count,
                 const bson::BSONObj *condition, const bson::BSONObj *hint) {
//  return cl->getCount(count, *condition, *hint);
}

int Sdb_cl::get_count(longlong &count, const bson::BSONObj &condition,
                      const bson::BSONObj &hint) {
  return retry(boost::bind(cl_get_count, m_cl, count, &condition, &hint));
}

int cl_get_indexes(ldbclient::ldbCollection *cl,
                   std::vector<bson::BSONObj> *infos) {
 // return cl->getIndexes(*infos);
}

int Sdb_cl::get_indexes(std::vector<bson::BSONObj> &infos) {
  return retry(boost::bind(cl_get_indexes, m_cl, &infos));
}

int cl_attach_collection(ldbclient::ldbCollection *cl,
                         const char *sub_cl_fullname,
                         const bson::BSONObj *options) {
//  return cl->attachCollection(sub_cl_fullname, *options);
}

int Sdb_cl::attach_collection(const char *sub_cl_fullname,
                              const bson::BSONObj &options) {
  return retry(
      boost::bind(cl_attach_collection, m_cl, sub_cl_fullname, &options));
}

int cl_detach_collection(ldbclient::ldbCollection *cl,
                         const char *sub_cl_fullname) {
//  return cl->detachCollection(sub_cl_fullname);
}

int Sdb_cl::detach_collection(const char *sub_cl_fullname) {
  return retry(boost::bind(cl_detach_collection, m_cl, sub_cl_fullname));
}

int cl_split(ldbclient::ldbCollection *cl, const char *source_group_name,
             const char *target_group_name, const bson::BSONObj *split_cond,
             const bson::BSONObj *split_end_cond) {
//  return cl->split(source_group_name, target_group_name, *split_cond,
//                   *split_end_cond);
}

int Sdb_cl::split(const char *source_group_name, const char *target_group_name,
                  const bson::BSONObj &split_cond,
                  const bson::BSONObj &split_end_cond) {
  return retry(boost::bind(cl_split, m_cl, source_group_name,
                           target_group_name, &split_cond, &split_end_cond));
}

int cl_get_detail(ldbclient::ldbCollection *cl, ldbclient::ldbCursor *cursor) {
//  return cl->getDetail(*cursor);
}

int Sdb_cl::get_detail(ldbclient::ldbCursor &cursor) {
  return retry(boost::bind(cl_get_detail, m_cl, &cursor));
}

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

#ifndef LDB_ERR_CODE__H
#define LDB_ERR_CODE__H

#define IS_LDB_NET_ERR(rc)                             \
  (LDB_NETWORK_CLOSE == (rc) || LDB_NETWORK == (rc) || \
   LDB_NOT_CONNECTED == (rc))

#define LDB_INVALID_BSONOBJ_SIZE_ASSERT_ID 10334
#define LDB_BUF_BUILDER_MAX_SIZE_ASSERT_ID 13548

enum LDB_ERR_CODE {
  LDB_ERR_OK = 0,
  LDB_ERR_COND_UNKOWN_ITEM = 30000,
  LDB_ERR_COND_PART_UNSUPPORTED,
  LDB_ERR_COND_UNSUPPORTED,
  LDB_ERR_COND_INCOMPLETED,
  LDB_ERR_COND_UNEXPECTED_ITEM,
  LDB_ERR_OVF,
  LDB_ERR_SIZE_OVF,
  LDB_ERR_TYPE_UNSUPPORTED,
  LDB_ERR_INVALID_ARG,
  LDB_ERR_EOF,

  LDB_ERR_INNER_CODE_BEGIN = 40000,
  LDB_ERR_INNER_CODE_END = 50000
};

typedef enum { LDB_ERROR, LDB_WARNING } enum_ldb_error_level;

void convert_ldb_code(int &rc);

int get_ldb_code(int rc);

#endif

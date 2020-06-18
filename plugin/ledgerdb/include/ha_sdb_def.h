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

#ifndef LDB_DEF__H
#define LDB_DEF__H

#include <client.hpp>

#define LDB_CS_NAME_MAX_SIZE 127
#define LDB_CL_NAME_MAX_SIZE 127
#define LDB_CL_FULL_NAME_MAX_SIZE (LDB_CS_NAME_MAX_SIZE + LDB_CL_NAME_MAX_SIZE)

#define LDB_FIELD_MAX_LEN (16 * 1024 * 1024)
#define LDB_IDX_FIELD_SIZE_MAX 1024
#define LDB_MATCH_FIELD_SIZE_MAX 1024
#define LDB_NUL_BIT_SIZE 1
#define LDB_PFS_META_LEN \
  60  // longest lli(19 digits+ 1 sign) + llu(20 digits) + 19 chars + 1 '\0'

#define LDB_CHARSET my_charset_utf8mb4_bin

#define LDB_OID_LEN 12
#define LDB_OID_FIELD "_id"

#define SOURCE_THREAD_ID "Source"
#define PREFIX_THREAD_ID "MySQL"
#define PREFIX_THREAD_ID_LEN 6
#define TRANSAUTOROLLBACK "TransAutoRollback"
#define TRANSAUTOCOMMIT "TransAutoCommit"

#define LDB_FIELD_NAME_AUTOINCREMENT "AutoIncrement"
#define LDB_FIELD_NAME_FIELD "Field"
#define LDB_FIELD_NAME "Name"
#define LDB_FIELD_NAME2 "name"
#define LDB_FIELD_ID "ID"
#define LDB_FIELD_SEQUENCE_NAME "SequenceName"
#define LDB_FIELD_SEQUENCE_ID "SequenceID"
#define LDB_FIELD_CURRENT_VALUE "CurrentValue"
#define LDB_FIELD_INCREMENT "Increment"
#define LDB_FIELD_START_VALUE "StartValue"
#define LDB_FIELD_ACQUIRE_SIZE "AcquireSize"
#define LDB_FIELD_CACHE_SIZE "CacheSize"
#define LDB_FIELD_MIN_VALUE "MinValue"
#define LDB_FIELD_MAX_VALUE "MaxValue"
#define LDB_FIELD_CYCLED "Cycled"
#define LDB_FIELD_GENERATED "Generated"
#define LDB_FIELD_INITIAL "Initial"
#define LDB_FIELD_LAST_GEN_ID "LastGenerateID"
#define LDB_FIELD_INFO "$ClientInfo"
#define LDB_FIELD_PORT "ClientPort"
#define LDB_FIELD_QID "ClientQID"

#define LDB_FIELD_SHARDING_KEY "ShardingKey"
#define LDB_FIELD_SHARDING_TYPE "ShardingType"
#define LDB_FIELD_PARTITION "Partition"
#define LDB_FIELD_REPLSIZE "ReplSize"
#define LDB_FIELD_COMPRESSED "Compressed"
#define LDB_FIELD_COMPRESSION_TYPE "CompressionType"
#define LDB_FIELD_COMPRESS_LZW "lzw"
#define LDB_FIELD_COMPRESS_SNAPPY "snappy"
#define LDB_FIELD_COMPRESS_NONE "none"
#define LDB_FIELD_ISMAINCL "IsMainCL"
#define LDB_FIELD_AUTO_SPLIT "AutoSplit"
#define LDB_FIELD_GROUP "Group"
#define LDB_FIELD_AUTOINDEXID "AutoIndexId"
#define LDB_FIELD_ENSURE_SHARDING_IDX "EnsureShardingIndex"
#define LDB_FIELD_STRICT_DATA_MODE "StrictDataMode"
#define LDB_FIELD_LOB_SHD_KEY_FMT "LobShardingKeyFormat"
#define LDB_FIELD_AUTOINCREMENT LDB_FIELD_NAME_AUTOINCREMENT
#define LDB_FIELD_CATAINFO "CataInfo"
#define LDB_FIELD_SUBCL_NAME "SubCLName"
#define LDB_FIELD_LOW_BOUND "LowBound"
#define LDB_FIELD_UP_BOUND "UpBound"
#define LDB_FIELD_ATTRIBUTE "Attribute"
#define LDB_FIELD_COMPRESSION_TYPE_DESC "CompressionTypeDesc"
#define LDB_FIELD_GROUP_NAME "GroupName"

#define LDB_FIELD_IDX_DEF "IndexDef"
#define LDB_FIELD_UNIQUE "Unique"
#define LDB_FIELD_ENFORCED "Enforced"
#define LDB_FIELD_NOT_NULL "NotNull"
#define LDB_FIELD_UNIQUE2 "unique"
#define LDB_FIELD_ENFORCED2 "enforced"
#define LDB_FIELD_KEY "key"

#define LDB_FIELD_UPDATED_NUM "UpdatedNum"
#define LDB_FIELD_MODIFIED_NUM "ModifiedNum"
#define LDB_FIELD_DELETED_NUM "DeletedNum"
#define LDB_FIELD_DUP_NUM "DuplicatedNum"
#define LDB_FIELD_INDEX_NAME "IndexName"
#define LDB_FIELD_INDEX_VALUE "IndexValue"
#define LDB_FIELD_PEER_ID "PeerID"
#define LDB_FIELD_CURRENT_FIELD "CurrentField"

#define LDB_FIELD_DETAIL "detail"
#define LDB_FIELD_DESCRIPTION "description"

#define LDB_GET_LAST_ERROR_FAILED "Get last error object failed."
#define LDB_GET_CONNECT_FAILED "Get connect to the specified address failed"
#define LDB_ACQUIRE_TRANSACTION_LOCK "Acquire transaction lock"

#define LDB_COMMENT "sequoiadb"
#define LDB_FIELD_AUTO_PARTITION "auto_partition"
#define LDB_FIELD_USE_PARTITION "use_partition"
#define LDB_FIELD_TABLE_OPTIONS "table_options"
#define LDB_FIELD_PARTITION_OPTIONS "partition_options"

#define LDB_FIELD_DETAILS "Details"
#define LDB_FIELD_PAGE_SIZE "PageSize"
#define LDB_FIELD_TOTAL_DATA_PAGES "TotalDataPages"
#define LDB_FIELD_TOTAL_INDEX_PAGES "TotalIndexPages"
#define LDB_FIELD_TOTAL_DATA_FREE_SPACE "TotalDataFreeSpace"
#define LDB_FIELD_TOTAL_RECORDS "TotalRecords"

#define LDB_PART_SEP "#P#"
#define LDB_SUB_PART_SEP "#SP#"
#define LDB_FIELD_PART_HASH_ID "_phid_"

#define LDB_DEFAULT_FILL_MESSAGE ""
#define LDB_ITEM_IGNORE_TYPE "ignore"

const static bson::BSONObj LDB_EMPTY_BSON;

#endif

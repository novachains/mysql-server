/*******************************************************************************

   Copyright (C) 2011-2018 SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *******************************************************************************/


/** \file clientDef.h
    \brief common include file C++ Client Driver
*/

#ifndef CLIENT_DEFINITION_H__
#define CLIENT_DEFINITION_H__

/** snapshot of all the contexts of all the sessions */
#define LDB_SNAP_CONTEXTS         0
/** snapshot of the contexts of current session */
#define LDB_SNAP_CONTEXTS_CURRENT 1
/** snapshot of all the sessions */
#define LDB_SNAP_SESSIONS         2
/** snapshot of current session */
#define LDB_SNAP_SESSIONS_CURRENT 3
/** snapshot of collections */
#define LDB_SNAP_COLLECTIONS      4
/** snapshot of collection spaces */
#define LDB_SNAP_COLLECTIONSPACES 5
/** snapshot of database */
#define LDB_SNAP_DATABASE         6
/** snapshot of system */
#define LDB_SNAP_SYSTEM           7
/** snapshot of catalog */
#define LDB_SNAP_CATALOG          8
/** snapshot of all the transactions of all the sessions */
#define LDB_SNAP_TRANSACTIONS     9
/** snapshot of all transactions of current session */
#define LDB_SNAP_TRANSACTIONS_CURRENT 10
/** snapshot of access plans */
#define LDB_SNAP_ACCESSPLANS      11
/** snapshot of health */
#define LDB_SNAP_HEALTH           12
/** snapshot of configs */
#define LDB_SNAP_CONFIGS          13
/** snapshot of service tasks */
#define LDB_SNAP_SVCTASKS         14
/** snapshot of sequences */
#define LDB_SNAP_SEQUENCES        15
/** reserved */
#define LDB_SNAP_RESERVED1        16
/** reserved */
#define LDB_SNAP_RESERVED2        17
/** snapshot of queries */
#define LDB_SNAP_QUERIES          18
/** snapshot of latch waits */
#define LDB_SNAP_LATCHWAITS       19
/** snapshot of lock waits */
#define LDB_SNAP_LOCKWAITS        20

/** list of all the contexts of all the sessions */
#define LDB_LIST_CONTEXTS         0
/** list of the contexts of current session */
#define LDB_LIST_CONTEXTS_CURRENT 1
/** list of all the sessions */
#define LDB_LIST_SESSIONS         2
/** list of current session */
#define LDB_LIST_SESSIONS_CURRENT 3
/** list of collections */
#define LDB_LIST_COLLECTIONS      4
/** list of collection spaces */
#define LDB_LIST_COLLECTIONSPACES 5
/** list of strorage units */
#define LDB_LIST_STORAGEUNITS     6
/** list of all the groups */
#define LDB_LIST_GROUPS           7
/** list of store procedures */
#define LDB_LIST_STOREPROCEDURES  8
/** list of domains */
#define LDB_LIST_DOMAINS          9
/** list of tasks */
#define LDB_LIST_TASKS            10
/** list of all the transactions of all the sessions */
#define LDB_LIST_TRANSACTIONS     11
/** list of all transactions of current session */
#define LDB_LIST_TRANSACTIONS_CURRENT 12
/** list of service tasks */
#define LDB_LIST_SVCTASKS         14
/** list of sequences */
#define LDB_LIST_SEQUENCES        15
/** list users */
#define LDB_LIST_USERS            16
/** list backups */
#define LDB_LIST_BACKUPS          17
/** reserved */
#define LDB_LIST_RESERVED1        18
#define LDB_LIST_RESERVED2        19
#define LDB_LIST_RESERVED3        20

// reserved
#define LDB_LIST_CL_IN_DOMAIN     129
// reserved
#define LDB_LIST_CS_IN_DOMAIN     130


/** define the callback for interruption when read/write data */
typedef bool (*socketInterruptFunc)(void) ;

/** define cache strategy for CPP client to improve performance */
typedef struct _sdbClientConf
{
   bool enableCacheStrategy ; /**< The flag to OPEN the cache strategy */
   uint32_t  cacheTimeInterval ;   /**< The life cycle(in seconds) of cached object */
} sdbClientConf ;


#endif // CLIENT_DEFINITION_H__

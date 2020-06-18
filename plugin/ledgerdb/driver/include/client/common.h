/*******************************************************************************
   Copyright (C) 2019-2020 PlanetRover Ltd.

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

#ifndef COMMON_H__
#define COMMON_H__

#include "msg/msg.h"
#include <cstdint>
#define CLIENT_RECORD_ID_FIELD "_id"
#define CLIENT_RECORD_ID_INDEX "$id"
#define CLIENT_RECORD_ID_FIELD_STRLEN 3

#define SOCKET_INVALIDSOCKET  -1

#define LDB_MD5_DIGEST_LENGTH 16
#define CLI_INT_TO_STR_MAX_SIZE 10

#define clientItoa(x,y,z) if (y) { snprintf(y, z, "%d", (int32_t)(x) );}

typedef struct _htbNode
{
   uint64_t lastTime ;
   char *name ;
} htbNode ;

typedef struct _hashTable
{
   uint32_t  capacity ;
   htbNode **node ;
} hashTable ;

/// for regulate query flag
int32_t regulateQueryFlags( int32_t flags ) ;

int32_t eraseSingleFlag( int32_t flags, int32_t erasedFlag ) ;

int32_t clientCheckRetMsgHeader( const char *pSendBuf, const char *pRecvBuf,
                               int endianConvert ) ;


int32_t clientBuildUpdateMsgCpp ( char **ppBuffer, int32_t *bufferSize,
                                const char *CollectionName,
                                int32_t flag, uint64_t reqID,
                                const char*selector,
                                const char*updator,
                                const char*hint,
                                int endianConvert ) ;

int32_t clientAppendInsertMsgCpp ( char **ppBuffer, int32_t *bufferSize,
                                 const char *insertor,
                                 int endianConvert ) ;

int32_t clientBuildInsertMsgCpp ( char **ppBuffer, int32_t *bufferSize,
                                const char *CollectionName,
                                int32_t flag, uint64_t reqID,
                                const char *insertor,
                                int endianConvert ) ;

int32_t clientBuildCmdMsgCpp  ( char **ppBuffer, int32_t *bufferSize,
                                uint32_t commandID,
                                const char *query,
                                int endianConvert ) ;
int32_t clientBuildQueryMsgCpp  ( char **ppBuffer, int32_t *bufferSize,
                                const char *CollectionName,
                                int32_t flag, uint64_t reqID,
                                int64_t numToSkip,
                                int64_t numToReturn,
                                const char *query,
                                const char *fieldSelector,
                                const char *orderBy,
                                const char *hint,
                                int endianConvert ) ;

int32_t clientBuildDeleteMsgCpp ( char **ppBuffer, int32_t *bufferSize,
                                const char *CollectionName,
                                int32_t flag, uint64_t reqID,
                                const char *deletor,
                                const char *hint,
                                int endianConvert ) ;

int32_t clientBuildAggrRequestCpp( char **ppBuffer, int32_t *bufferSize,
                                 const char *CollectionName,
                                 const char *obj,
                                 int endianConvert ) ;

int32_t clientAppendAggrRequestCpp ( char **ppBuffer, int32_t *bufferSize,
                                   const char *obj,
                                   int endianConvert ) ;

int32_t clientBuildAuthCrtMsgCpp( char **ppBuffer, int32_t *bufferSize,
                                const char *pUsrName,
                                const char *pPasswd,
                                const char *pOptions,
                                uint64_t reqID, int endianConvert ) ;
/*
int32_t clientBuildUpdateMsg ( char **ppBuffer, int32_t *bufferSize,
                             const char *CollectionName, int32_t flag,
                             uint64_t reqID,
                             bson *selector, bson *updator,
                             bson *hint, int endianConvert ) ;

int32_t clientAppendInsertMsg ( char **ppBuffer, int32_t *bufferSize,
                              bson *insertor, int endianConvert ) ;

int32_t clientBuildInsertMsg ( char **ppBuffer, int32_t *bufferSize,
                             const char *CollectionName, int32_t flag,
                             uint64_t reqID,
                             bson *insertor, int endianConvert ) ;

int32_t clientBuildQueryMsg  ( char **ppBuffer, int32_t *bufferSize,
                             const char *CollectionName, int32_t flag,
                             uint64_t reqID,
                             int64_t numToSkip, int64_t numToReturn,
                             const bson *query, const bson *fieldSelector,
                             const bson *orderBy, const bson *hint,
                             int endianConvert ) ;

int32_t clientBuildDeleteMsg ( char **ppBuffer, int32_t *bufferSize,
                             const char *CollectionName,
                             int32_t flag, uint64_t reqID,
                             bson *deletor,
                             bson *hint,
                             int endianConvert ) ;

int32_t clientAppendOID ( bson *obj, bson_iterator *ret ) ;

int32_t clientBuildAggrRequest1( char **ppBuffer, int32_t *bufferSize,
                               const char *CollectionName, bson **objs,
                               int32_t num, int endianConvert ) ;

int32_t clientBuildAggrRequest( char **ppBuffer, int32_t *bufferSize,
                              const char *CollectionName, bson *obj,
                              int endianConvert ) ;

int32_t clientAppendAggrRequest ( char **ppBuffer, int32_t *bufferSize,
                                bson *obj, int endianConvert ) ;

int32_t clientBuildAuthCrtMsg( char **ppBuffer, int32_t *bufferSize,
                             const char *pUsrName,
                             const char *pPasswd,
                             const bson *options,
                             uint64_t reqID, int endianConvert ) ;
*/

int32_t clientBuildGetMoreMsg ( char **ppBuffer, int32_t *bufferSize,
                              int32_t numToReturn,
                              int64_t contextID, uint64_t reqID,
                              int endianConvert ) ;

int32_t clientBuildKillContextsMsg ( char **ppBuffer, int32_t *bufferSize,
                                   uint64_t reqID, int32_t numContexts,
                                   const int64_t *pContextIDs,
                                   int endianConvert ) ;

int32_t clientBuildInterruptMsg ( char **ppBuffer, int32_t *bufferSize,
                                uint64_t reqID, int isSelf,
                                int endianConvert ) ;

int32_t clientExtractReply ( char *pBuffer, int32_t *flag, int64_t *contextID,
                           int32_t *startFrom, int32_t *numReturned,
                           int endianConvert ) ;

int32_t clientBuildDisconnectMsg ( char **ppBuffer, int32_t *bufferSize,
                                 uint64_t reqID, int endianConvert ) ;

int32_t clientBuildSqlMsg( char **ppBuffer, int32_t *bufferSize,
                         const char *sql, uint64_t reqID,
                         int endianConvert ) ;

int32_t clientBuildAuthMsg( char **ppBuffer, int32_t *bufferSize,
                          const char *pUsrName,
                          const char *pPasswd,
                          uint64_t reqID, int endianConvert ) ;

int32_t clientBuildAuthDelMsg( char **ppBuffer, int32_t *bufferSize,
                             const char *pUsrName,
                             const char *pPasswd,
                             uint64_t reqID, int endianConvert ) ;

int32_t clientBuildTransactionBegMsg( char **ppBuffer, int32_t *bufferSize,
                                    uint64_t reqID,
                                    int endianConvert ) ;

int32_t clientBuildTransactionCommitMsg( char **ppBuffer, int32_t *bufferSize,
                                       uint64_t reqID,
                                       int endianConvert ) ;

int32_t clientBuildTransactionRollbackMsg( char **ppBuffer, int32_t *bufferSize,
                                         uint64_t reqID,
                                         int endianConvert ) ;

int32_t clientBuildSysInfoRequest ( char **ppBuffer, int32_t *pBufferSize ) ;

int32_t clientExtractSysInfoReply ( char *pBuffer, bool *endianConvert,
                                  int32_t *osType ) ;

int32_t clientValidateSql( const char *sql, int isExec ) ;

int32_t clientBuildTestMsg( char **ppBuffer, int32_t *bufferSize,
                          const char *msg, uint64_t reqID,
                          int endianConvert ) ;

/*
   Other tool functions
*/
int32_t md5Encrypt( const char *src,
                  char *code,
                  uint32_t size ) ;

int32_t clientReplicaGroupExtractNode ( const char *data,
                                      char *pHostName,
                                      int32_t hostNameSize,
                                      char *pServiceName,
                                      int32_t serviceNameSize,
                                      int32_t *pNodeID ) ;




#endif // COMMON_H__


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


/** \file client.hpp
    \brief C++ Client Driver
*/


#ifndef CLIENT_HPP__
#define CLIENT_HPP__
#include "oss/core.h"
#include "client/common.h"
#include "client/clientDef.h"
#include "client/fromjson.hpp"
#include "bson/bson.h"
#include "oss/ossSocket.hpp"
#include "oss/ossUtil.hpp"
#include <set>
#include <map>
#include <mutex>
#include <string>
#include <vector>


/** This macro is for internal use, not a public api, it will be removed in the future */
#define RELEASE_INNER_HANDLE( handle ) \
do                                     \
{                                      \
   if ( handle )                       \
   {                                   \
      delete handle ;                  \
      handle = nullptr ;                  \
   }                                   \
} while( 0 )

#define DLLEXPORT LDB_EXPORT

/** define page size to 4k */
#define LDB_PAGESIZE_4K           4096
/** define page size to 8k */
#define LDB_PAGESIZE_8K           8192
/** define page size to 16k */
#define LDB_PAGESIZE_16K          16384
/** define page size to 32k */
#define LDB_PAGESIZE_32K          32768
/** define page size to 64k */
#define LDB_PAGESIZE_64K          65536
/** 0 means using database's default pagesize, it 64k now */
#define LDB_PAGESIZE_DEFAULT      0


/** The flag represent whether insert continue(no errors were reported) when hitting index key duplicate error */
#define FLG_INSERT_CONTONDUP      0x00000001
/** The flag represent whether insert return detail result */
#define FLG_INSERT_RETURNNUM      0x00000002
/** The flag represent replacing the existing record by the new record and continuing when insert hitting index key duplicate error */
#define FLG_INSERT_REPLACEONDUP   0x00000004

// client socket timeout value
// since client and server may not sit in the same network, we need
// to set this value bigger than engine socket timeout
// this value is in millisec
// set to 10 seconds timeout
#define LDB_CLIENT_SOCKET_TIMEOUT_DFT 10000

/** class name 'ldbReplicaNode' will be deprecated in version 2.x, use 'ldbNode' instead of it. */
#define ldbReplicaNode         ldbNode

/** Force to use specified hint to query, if database have no index assigned by the hint, fail to query. */
#define QUERY_FORCE_HINT                  0x00000080
/** Enable parallel sub query, each sub query will finish scanning different part of the data. */
#define QUERY_PARALLED                    0x00000100
/** In general, query won't return data until cursor gets from database, when add this flag, return data in query response, it will be more high-performance */
#define QUERY_WITH_RETURNDATA             0x00000200
/** Enable prepare more data when query */
#define QUERY_PREPARE_MORE                0x00004000
/** The sharding key in update rule is not filtered, when executing queryAndUpdate. */
#define QUERY_KEEP_SHARDINGKEY_IN_UPDATE  0x00008000
/** When the transaction is turned on and the transaction isolation level is "RC",
    the transaction lock will be released after the record is read by default.
    However, when setting this flag, the transaction lock will not released until
    the transaction is committed or rollback. When the transaction is turned off or
    the transaction isolation level is "RU", the flag does not work. */
#define QUERY_FOR_UPDATE                  0x00010000


/** The sharding key in update rule is not filtered, when executing update or upsert. */
#define UPDATE_KEEP_SHARDINGKEY           QUERY_KEEP_SHARDINGKEY_IN_UPDATE
/** The flag represent whether update return detail result */
#define UPDATE_RETURNNUM                  0x00000004

/** The flag represent whether update return detail result */
#define FLG_DELETE_RETURNNUM              0x00000004

#define LDB_INDEX_SORT_BUFFER_DEFAULT_SIZE   64

/** \namespace ldbclient
    \brief LedgerDB Driver for C++
*/
namespace ldbclient
{
#define CLIENT_COLLECTION_NAMESZ           127
#define CLIENT_CS_NAMESZ                   127
#define CLIENT_CL_FULLNAME_SZ              ( CLIENT_COLLECTION_NAMESZ + CLIENT_CS_NAMESZ + 1 )

   const static bson::BSONObj _ldbStaticObject ;

   class ldbCursor ;
   class ldbCollection ;
   class ldb ;
   class _ldb ;
   class _ossSocket ;

   /** Callback function when the reply message is error **/
   typedef void (*ERROR_ON_REPLY_FUNC)( const char *pErrorObj,
                                        uint32_t objSize,
                                        int32_t flag,
                                        const char *pDescription,
                                        const char *pDetail ) ;

   /** \fn int32_t ldbSetErrorOnReplyCallback ( ERROR_ON_REPLY_FUNC func )
       \brief Set the callback function when reply message if error from server
       \param [in] func The callback function when called on reply error
   */
   LDB_EXPORT void ldbSetErrorOnReplyCallback( ERROR_ON_REPLY_FUNC func ) ;

   /** \class  ldbCursor
         \brief Database operation interfaces of cursor.
   */
   class ldbCursor
   {
   private :
      ldbCursor ( const ldbCursor& other ) ;
      ldbCursor& operator=( const ldbCursor& ) ;
   public :
      ldbCursor ();
      ~ldbCursor ();

      /** \fn  int32_t next ( bson::BSONObj &obj, bool getOwned = true )
            \brief Return the next document of current cursor, and move forward
            \param [in] getOwned Whether the return bson object should have its own buffer, default to be true.
                <ul>
                <li>
                true : In this case the return bson object is a new full (and owned) copy of the next document and it
                       keep the contents in it's own buffer, that mean you can use the return bson object whenever you
                       want.
                <li>
                false : In this case the return bson object does not keep contents in it's own buffer, and you should
                        use the return bson object before the receive buffer of the connection is overwrite by another
                        operation.
            \param [out] obj The return bson object
            \retval LDB_OK Operation Success
            \retval Others Operation Fail
      */
      int32_t next    ( bson::BSONObj &obj, bool getOwned = true );

      /** \fn int32_t current ( bson::BSONObj &obj, bool getOwned = true )
            \brief Return the current document of cursor, and don't move
            \param [in] getOwned Whether the return bson object should have its own buffer, default to be true.
                <ul>
                <li>
                true : In this case the return bson object is a new full (and owned) copy of the current document and it
                       keep the contents in it's own buffer, that mean you can use the return bson object whenever you
                       want.
                <li>
                false : In this case the return bson object does not keep contents in it's own buffer, and you should
                        use the return bson object before the receive buffer of the connection is overwrite by another
                        operation.
            \param [out] obj The return bson object
            \retval LDB_OK Operation Success
            \retval Others Operation Fail
      */
      int32_t current ( bson::BSONObj &obj, bool getOwned = true );

      /** \fn int32_t close ()
            \brief Close the cursor's connection to database.
            \retval LDB_OK Operation Success
            \retval Others Operation Fail
      */
      int32_t close ();
   private:
      _ldb                 *_connection ;
      ldbCollection        *_collection ;

      char                 *_pSendBuffer ;
      int32_t               _sendBufferSize ;
      char                 *_pReceiveBuffer ;
      int32_t               _receiveBufferSize ;

      int64_t               _contextID ;
      bool                  _isClosed ;

      uint64_t              _totalRead ;
      int32_t               _offset ;

   private:
      int32_t  _killCursor () ;
      int32_t  _readNextBuffer () ;
      void     _attachConnection ( _ldb *connection ) ;
      void     _attachCollection ( ldbCollection *collection ) ;
      void     _detachConnection() ;
      void     _detachCollection() ;
      void     _close() ;

      friend class ldbCollection ;
      friend class _ldbNode ;
      friend class _ldb ;
   } ;

   /** \class ldbCollection
         \brief Database operation interfaces of collection.
   */
   class ldbCollection
   {
   private :
      /** \fn ldbCollection ( const ldbCollection& other ) ;
            \brief Copy constructor
            \param[in] A const object reference of class ldbCollection.
      */
      ldbCollection ( const ldbCollection& other ) ;

      /** \fn ldbCollection& operator=( const ldbCollection& )
            \brief Assignment constructor
            \param[in] a const reference of class ldbCollection.
            \retval A const object reference of class ldbCollection.
      */
      ldbCollection& operator=( const ldbCollection& ) ;
   public :
      /** \fn ldbCollection ()
          \brief Default constructor
      */
      ldbCollection (const char*, ldbclient::_ldb&);

      /** \fn ~ldbCollection ()
          \brief Destructor.
      */
      ~ldbCollection ();

      /** \fn int32_t getCount ( int64_t &count,
                               const bson::BSONObj &condition,
                               const bson::BSONObj &hint )
          \brief Get the count of matching documents in current collection.
          \param [in] condition The matching rule, return the count of all documents if this parameter is empty
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [out] count The count of matching documents, matches all records if not provided.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getCount ( int64_t &count,
                       const bson::BSONObj &condition = _ldbStaticObject,
                       const bson::BSONObj &hint = _ldbStaticObject );

      /** \fn int32_t alterCollection ( const bson::BSONObj &options )
          \brief Alter the current collection
          \param [in] options The modified options as following:

              ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
              ShardingKey  : Assign the sharding key
              ShardingType : Assign the sharding type
              Partition    : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20]
              CompressionType : The compression type of data, could be "snappy" or "lzw"
              EnsureShardingIndex : Assign to true to build sharding index
              StrictDataMode : Using strict date mode in numeric operations or not
                             e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
              AutoIncrement: Assign attributes of an autoincrement field or batch autoincrement fields.
                             e.g. {AutoIncrement:{Field:"a",MaxValue:2000}},
                                  {AutoIncrement:[{Field:"a",MaxValue:2000},{Field:"a",MaxValue:4000}]}

          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t alterCollection ( const bson::BSONObj &options );

      /** \fn int32_t insert ( bson::BSONObj &obj )
          \brief Insert a bson object into current collection
          \param [in] obj The bson object to be inserted.
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t insert ( const bson::BSONObj &obj);

      /** \fn int32_t insert ( const bson::BSONObj &obj,
                             int32_t flags,
                             bson::BSONObj *pResult = nullptr )
          \brief Insert a bson object into current collection.
          \param [in] obj The bson object to be inserted.
          \param [in] flags The flag to control the behavior of inserting. The
                            value of flag default to be 0, and it can choose
                            the follow values:
               <ul>
               <li>
               0:                    while 0 is set(default to be 0), database
                                     will stop inserting when some records hit
                                     index key duplicate error.
               <li>
               FLG_INSERT_CONTONDUP:
                                     if some records hit index key duplicate
                                     error, database will skip them and go on
                                     inserting.
               <li>
               FLG_INSERT_REPLACEONDUP:
                                      if the record hit index key duplicate
                                      error, database will replace the existing
                                      record by the inserting new record.
          \param [out] pResult The result of inserting. Can be nullptr or a bson:
               <ul>
               <li> nullptr:
                     when this argument is nullptr.
               <li> empty bson: when this argument is not nullptr but there is no
                                result return.
               </ul>


          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t insert ( const bson::BSONObj &obj,
                     int32_t flags,
                     bson::BSONObj *pResult = nullptr );

      /** \fn int32_t insert ( std::vector<bson::BSONObj> &objs,
                             int32_t flags = 0,
                             bson::BSONObj *pResult = nullptr )
          \brief Insert a bson object into current collection.
          \param [in] objs The bson objects to be inserted.
          \param [in] flags The flag to control the behavior of inserting. The
                            value of flag default to be 0, and it can choose
                            the follow values:
               <ul>
               <li>
               0:                    while 0 is set(default to be 0), database
                                     will stop inserting when some records hit
                                     index key duplicate error.
               <li>
               FLG_INSERT_CONTONDUP:
                                     if some records hit index key duplicate
                                     error, database will skip them and go on
                                     inserting.
               <li>
               FLG_INSERT_REPLACEONDUP:
                                     if the record hit index key duplicate
                                     error, database will replace the existing
                                     record by the inserting new record and then
                                     go on inserting.

          \param [out] pResult The result of inserting. Can be nullptr or a bson:
               <ul>
               <li> nullptr:
                     when this argument is nullptr.
               <li> empty bson: when this argument is not nullptr but there is no
                                result return.
               </ul>

          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t insert ( std::vector<bson::BSONObj> &objs,
                     int32_t flags = 0,
                     bson::BSONObj *pResult = nullptr );

      /** \fn int32_t insert ( bson::BSONObj objs[],
                             int32_t size,
                             int32_t flags = 0,
                             bson::BSONObj *pResult = nullptr )
          \brief Insert bson objects into current collection.
          \param [in] objs The array of bson objects to be inserted.
          \param [in] size The size of the array.
          \param [in] flags The flag to control the behavior of inserting. The
                            value of flag default to be 0, and it can choose
                            the follow values:
               <ul>
               <li>
               0:                    while 0 is set(default to be 0), database
                                     will stop inserting when some records hit
                                     index key duplicate error.
               <li>
               FLG_INSERT_CONTONDUP:
                                     if some records hit index key duplicate
                                     error, database will skip them and go on
                                     inserting.
               <li>
               FLG_INSERT_REPLACEONDUP:
                                     if the record hit index key duplicate
                                     error, database will replace the existing
                                     record by the inserting new record and then
                                     go on inserting.

          \param [out] pResult The result of inserting.
                       Can be nullptr or a bson:
               <ul>
               <li> nullptr:
                     when this argument is nullptr.
               <li> empty bson: when this argument is not nullptr but there is no
                                result return.
               </ul>

          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t insert ( const bson::BSONObj objs[],
                     int32_t size,
                     int32_t flags = 0,
                     bson::BSONObj *pResult = nullptr );

      /** \fn  int32_t bulkInsert ( int32_t flags,
                                  std::vector<bson::BSONObj> &objs )
          \brief Insert a bulk of bson objects into current collection.
          \param [in] flags The flag to control the behavior of inserting. The
                            value of flag default to be 0, and it can choose
                            the follow values:
               <ul>
               <li>
               0:                    while 0 is set(default to be 0), database
                                     will stop inserting when some records hit
                                     index key duplicate error.
               <li>
               FLG_INSERT_CONTONDUP:
                                     if some records hit index key duplicate
                                     error, database will skip them and go on
                                     inserting.
               <li>
               FLG_INSERT_REPLACEONDUP:
                                     if the record hit index key duplicate
                                     error, database will replace the existing
                                     record by the inserting new record and then
                                     go on inserting.

          \param [in] objs The bson objects to be inserted.
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t bulkInsert ( int32_t flags,
                         std::vector<bson::BSONObj> &objs );

      /** \fn  int32_t update ( const bson::BSONObj &rule,
                           const bson::BSONObj &condition,
                           const bson::BSONObj &hint,
                           int32_t flag
                         )
          \brief Update the matching documents in current collection
          \param [in] rule The updating rule
          \param [in] condition The matching rule, update all the documents if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] flag The query flag, default to be 0. Please see the definition of follow flags for more detail
          \code
              UPDATE_KEEP_SHARDINGKEY
          \endcode
          \param [out] pResult The detail result for updating.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
          \note When flag is set to 0, it won't work to update the "ShardingKey" field, but the
                    other fields take effect
      */
      int32_t update ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition = _ldbStaticObject,
                     const bson::BSONObj &hint      = _ldbStaticObject,
                     int32_t flag = 0,
                     bson::BSONObj *pResult = nullptr);

      /** \fn int32_t upsert ( const bson::BSONObj &rule,
                           const bson::BSONObj &condition = _ldbStaticObject,
                           const bson::BSONObj &hint      = _ldbStaticObject,
                           int32_t flag = 0
                         )
          \brief Update the matching documents in current collection, insert if no matching
          \param [in] rule The updating rule
          \param [in] condition The matching rule, update all the documents if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] setOnInsert The setOnInsert assigns the specified values to the fileds when insert
          \param [in] flag The query flag, default to be 0. Please see the definition of follow flags for more detail
          \code
              UPDATE_KEEP_SHARDINGKEY
          \endcode
          \param [out] pResult The detail result for upserting
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
          \note When flag is set to 0, it won't work to update the "ShardingKey" field, but the
                    other fields take effect
      */
      int32_t upsert ( const bson::BSONObj &rule,
                     const bson::BSONObj &condition   = _ldbStaticObject,
                     const bson::BSONObj &hint        = _ldbStaticObject,
                     const bson::BSONObj &setOnInsert = _ldbStaticObject,
                     int32_t flag                       = 0,
                     bson::BSONObj *pResult           = nullptr);

      /** \fn   int32_t del ( const bson::BSONObj &condition,
                        const bson::BSONObj &hint
                      )
          \brief Delete the matching documents in current collection
          \param [in] condition The matching rule, delete all the documents if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] flag Reserved
          \param [out] pResult The detail result for deleting
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t del ( const bson::BSONObj &condition = _ldbStaticObject,
                  const bson::BSONObj &hint      = _ldbStaticObject,
                  int32_t flag                     = 0,
                  bson::BSONObj *pResult         = nullptr);

      /* \fn int32_t query  ( ldbCursor **cursor,
                           const bson::BSONObj &condition,
                           const bson::BSONObj &selected,
                           const bson::BSONObj &orderBy,
                           const bson::BSONObj &hint,
                           uint64_t numToSkip,
                           uint64_t numToReturn,
                           int32_t flags
                          )
          \brief Get the matching documents in current collection
          \param [in] condition The matching rule, return all the documents if not provided
          \param [in] selected The selective rule, return the whole document if not provided
          \param [in] orderBy The ordered rule, result set is unordered if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] numToSkip Skip the first numToSkip documents, default is 0
          \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
          \param [in] flags The query flags, default to be 0. Please see the definition of follow flags for more detail. Usage: e.g. set ( QUERY_FORCE_HINT | QUERY_WITH_RETURNDATA ) to param flags
          \code
              QUERY_FORCE_HINT
              QUERY_PARALLED
              QUERY_WITH_RETURNDATA
              QUERY_FOR_UPDATE
          \endcode
          \param [out] cursor The cursor of current query
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t query  ( ldbCursor **cursor,
                     const bson::BSONObj &condition = _ldbStaticObject,
                     const bson::BSONObj &selected  = _ldbStaticObject,
                     const bson::BSONObj &orderBy   = _ldbStaticObject,
                     const bson::BSONObj &hint      = _ldbStaticObject,
                     uint64_t numToSkip          = 0,
                     uint64_t numToReturn        = -1,
                     int32_t flags              = 0);

      /** \fn int32_t queryOne( BSONObj &obj,
                              const bson::BSONObj &condition,
                              const bson::BSONObj &selected,
                              const bson::BSONObj &orderBy,
                              const bson::BSONObj &hint,
                              uint64_t numToSkip,
                              int32_t flag
                             )
          \brief Get the first matching documents in current collection
          \param [in] condition The matching rule, return all the documents if not provided
          \param [in] selected The selective rule, return the whole document if not provided
          \param [in] orderBy The ordered rule, result set is unordered if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] numToSkip Skip the first numToSkip documents, default is 0
          \param [in] flag The query flag, default to be 0. Please see the definition of follow flags for more detail. Usage: e.g. set ( QUERY_FORCE_HINT | QUERY_WITH_RETURNDATA ) to param flag
          \code
              QUERY_FORCE_HINT
              QUERY_PARALLED
              QUERY_WITH_RETURNDATA
              QUERY_FOR_UPDATE
          \endcode
          \param [out] obj The first matching object
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t queryOne( bson::BSONObj &obj,
                      const bson::BSONObj &condition = _ldbStaticObject,
                      const bson::BSONObj &selected  = _ldbStaticObject,
                      const bson::BSONObj &orderBy   = _ldbStaticObject,
                      const bson::BSONObj &hint      = _ldbStaticObject,
                      uint64_t numToSkip    = 0,
                      int32_t flag         = 0 );

      /** \fn int32_t queryAndUpdate ( ldbCursor **cursor,
                                     const bson::BSONObj &update,
                                     const bson::BSONObj &condition,
                                     const bson::BSONObj &selected,
                                     const bson::BSONObj &orderBy,
                                     const bson::BSONObj &hint,
                                     uint64_t numToSkip,
                                     uint64_t numToReturn,
                                     int32_t flag,
                                     bool returnNew
                                  )
          \brief Get the matching documents in current collection and update
          \param [in] update The update rule, can't be empty
          \param [in] condition The matching rule, return all the documents if not provided
          \param [in] selected The selective rule, return the whole document if not provided
          \param [in] orderBy The ordered rule, result set is unordered if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] numToSkip Skip the first numToSkip documents, default is 0
          \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
          \param [in] flag The query flag, default to be 0. Please see the definition of follow flags for more detail. Usage: e.g. set ( QUERY_FORCE_HINT | QUERY_WITH_RETURNDATA ) to param flag
          \code
              QUERY_FORCE_HINT
              QUERY_PARALLED
              QUERY_WITH_RETURNDATA
              QUERY_KEEP_SHARDINGKEY_IN_UPDATE
              QUERY_FOR_UPDATE
          \endcode
          \param [in] returnNew When true, returns the updated document rather than the original
          \param [out] cursor The cursor of current query
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t queryAndUpdate ( ldbCursor **cursor,
                             const bson::BSONObj &update,
                             const bson::BSONObj &condition = _ldbStaticObject,
                             const bson::BSONObj &selected  = _ldbStaticObject,
                             const bson::BSONObj &orderBy   = _ldbStaticObject,
                             const bson::BSONObj &hint      = _ldbStaticObject,
                             uint64_t numToSkip                = 0,
                             uint64_t numToReturn              = -1,
                             int32_t flag                     = 0,
                             bool returnNew              = false
                          )
      {
         return _queryAndModify( cursor, condition, selected, orderBy,
                                 hint, update, numToSkip, numToReturn,
                                 flag, true, returnNew ) ;
      }

      /** \fn INT32 queryAndRemove ( sdbCursor &cursor,
                                     const bson::BSONObj &condition,
                                     const bson::BSONObj &selected,
                                     const bson::BSONObj &orderBy,
                                     const bson::BSONObj &hint,
                                     INT64 numToSkip,
                                     INT64 numToReturn,
                                     INT32 flag
                                  )
          \brief Get the matching documents in current collection and remove
          \param [in] condition The matching rule, return all the documents if not provided
          \param [in] selected The selective rule, return the whole document if not provided
          \param [in] orderBy The ordered rule, result set is unordered if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] numToSkip Skip the first numToSkip documents, default is 0
          \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
          \param [in] flag The query flag, default to be 0. Please see the definition of follow flags for more detail. Usage: e.g. set ( QUERY_FORCE_HINT | QUERY_WITH_RETURNDATA ) to param flag
          \code
              QUERY_FORCE_HINT
              QUERY_PARALLED
              QUERY_WITH_RETURNDATA
              QUERY_FOR_UPDATE
          \endcode
          \param [out] cursor The cursor of current query
          \retval SDB_OK Operation Success
          \retval Others Operation Fail
      */
      INT32 queryAndRemove ( ldbCursor **cursor,
                             const bson::BSONObj &condition = _ldbStaticObject,
                             const bson::BSONObj &selected  = _ldbStaticObject,
                             const bson::BSONObj &orderBy   = _ldbStaticObject,
                             const bson::BSONObj &hint      = _ldbStaticObject,
                             INT64 numToSkip                = 0,
                             INT64 numToReturn              = -1,
                             INT32 flag                     = 0
                          );

      /** \fn int32_t createIndex ( const bson::BSONObj &indexDef,
                                  const char *pIndexName,
                                  bool isUnique,
                                  bool isEnforced,
                                  int32_t sortBufferSize )
          \brief Create the index in current collection
          \param [in] indexDef The bson structure of index element, e.g. {name:1, age:-1}
          \param [in] pIndexName The index name
          \param [in] isUnique Whether the index elements are unique or not
          \param [in] isEnforced Whether the index is enforced unique
                                 This element is meaningful when isUnique is set to true
          \param [in] sortBufferSize The size of sort buffer used when creating index, the unit is MB,
                                     zero means don't use sort buffer
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createIndex ( const bson::BSONObj &indexDef,
                          const char *pIndexName,
                          bool isUnique,
                          bool isEnforced,
                          int32_t sortBufferSize =
                          LDB_INDEX_SORT_BUFFER_DEFAULT_SIZE );

      /** \fn int32_t createIndex ( const bson::BSONObj &indexDef,
                                  const char *pIndexName,
                                  const bson::BSONObj &options )
          \brief Create the index in current collection
          \param [in] indexDef The bson structure of index element, e.g. {name:1, age:-1}
          \param [in] pIndexName The index name
          \param [in] options The options are as below:

              Unique:    Whether the index elements are unique or not
              Enforced:  Whether the index is enforced unique.
                         This element is meaningful when Unique is true
              NotNull:   Any field of index key should exist and cannot be null when NotNull is true
              SortBufferSize: The size of sort buffer used when creating index.
                              Unit is MB. Zero means don't use sort buffer
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createIndex ( const bson::BSONObj &indexDef,
                          const char *pIndexName,
                          const bson::BSONObj &options );

      /* \fn int32_t getIndexes ( ldbCursor **cursor,
                               const char *pIndexName )
          \brief Get all of or one of the indexes in current collection
          \param [in] pIndexName  The index name, returns all of the indexes if this parameter is null
          \param [out] cursor The cursor of all the result for current query
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getIndexes ( ldbCursor **cursor,
                         const char *pIndexName );

      /** \fn int32_t getIndexes ( std::vector<bson::BSONObj> &infos )
          \brief Get all of the indexes in current collection.
          \param [out] infos Vector for the information of all the index.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getIndexes ( std::vector<bson::BSONObj> &infos );

      /** \fn int32_t getIndex ( const char *pIndexName, bson::BSONObj &info )
          \brief Get the specified index in current collection.
          \param [in] pIndexName  The index name, returns all of the indexes if this parameter is null
          \param [out] info The information of the index.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getIndex ( const char *pIndexName, bson::BSONObj &info );

      /** \fn int32_t dropIndex ( const char *pIndexName )
          \brief Drop the index in current collection
          \param [in] pIndexName The index name
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t dropIndex ( const char *pIndexName );

      /** \fn INT32 createAutoIncrement ( const bson::BSONObj &options )
          \brief Create an autoincrement field on collection
          \param [in] options The options as following:

              Field          : The name of autoincrement field
              StartValue     : The start value of autoincrement field
              MinValue       : The minimum value of autoincrement field
              MaxValue       : The maxmun value of autoincrement field
              Increment      : The increment value of autoincrement field
              CacheSize      : The cache size of autoincrement field
              AcquireSize    : The acquire size of autoincrement field
              Cycled         : The cycled flag of autoincrement field
              Generated      : The generated mode of autoincrement field

          \retval SDB_OK Operation Success
          \retval Others Operation Fail
      */
      INT32 createAutoIncrement ( const bson::BSONObj &options );
      INT32 createAutoIncrement ( const std::vector<bson::BSONObj> &options );

      /** \fn INT32 dropAutoIncrement ( const char * fieldName )
          \brief Drop an autoincrement field on collection
          \param [in] fieldName The name of autoincrement field
          \retval SDB_OK Operation Success
          \retval Others Operation Fail
      */
      INT32 dropAutoIncrement ( const char * fieldName );

      /** \fn const char *getCollectionName ()
          \brief Get the name of specified collection in current collection space
          \return The name of specified collection.
      */
      const char *getCollectionName() { return &_collectionName[0] ; }

      /** \fn const char *getCSName ()
          \brief Get the name of current collection space
          \return The name of current collection space.
      */
      const char *getCSName() { return &_collectionSpaceName[0] ; }

      /** \fn const char *getFullName ()
          \brief Get the full name of specified collection in current collection space
          \return The full name of specified collection.
      */
      const char *getFullName() { return &_collectionFullName[0] ; }

      /* \fn int32_t aggregate ( ldbCursor **cursor,
                               std::vector<bson::BSONObj> &obj
                             )
          \brief Execute aggregate operation in specified collection
          \param [in] obj The array of bson objects
          \param [out] cursor The cursor handle of result
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t aggregate ( ldbCursor **cursor,
                        std::vector<bson::BSONObj> &obj);

      /* \fn  int32_t getQueryMeta ( ldbCursor **cursor,
                                   const bson::BSONObj &condition = _ldbStaticObject,
                                   const bson::BSONObj &selected = _ldbStaticObject,
                                   const bson::BSONObj &orderBy = _ldbStaticObject,
                                   uint64_t numToSkip = 0,
                                   uint64_t numToReturn = -1 ) ;
          \brief Get the index blocks' or data blocks' infomation for concurrent query
          \param [in] condition The matching rule, return all the documents if not provided
          \param [in] orderBy The ordered rule, result set is unordered if not provided
          \param [in] hint Specified the index used to scan data. e.g. {"":"ageIndex"} means
                          using index "ageIndex" to scan data(index scan);
                          {"":null} means table scan. when hint is not provided,
                          database automatically match the optimal index to scan data
          \param [in] numToSkip Skip the first numToSkip documents, default is 0
          \param [in] numToReturn Only return numToReturn documents, default is -1 for returning all results
          \param [out] cursor The cursor of current query
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getQueryMeta ( ldbCursor **cursor,
                           const bson::BSONObj &condition = _ldbStaticObject,
                           const bson::BSONObj &orderBy = _ldbStaticObject,
                           const bson::BSONObj &hint = _ldbStaticObject,
                           uint64_t numToSkip = 0,
                           uint64_t numToReturn = -1 );

      /** \fn int32_t truncate()
          \brief truncate the collection
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t truncate();

      /** \fn int32_t setAttributes ( const bson::BSONObj &options )
          \brief Alter the current collection
          \param [in] options The modified options as following:

              ReplSize     : Assign how many replica nodes need to be synchronized when a write request(insert, update, etc) is executed
              ShardingKey  : Assign the sharding key
              ShardingType : Assign the sharding type
              Partition    : When the ShardingType is "hash", need to assign Partition, it's the bucket number for hash, the range is [2^3,2^20]
              CompressionType : The compression type of data, could be "snappy" or "lzw"
              EnsureShardingIndex : Assign to true to build sharding index
              StrictDataMode : Using strict date mode in numeric operations or not
                             e.g. {RepliSize:0, ShardingKey:{a:1}, ShardingType:"hash", Partition:1024}
              AutoIncrement: Assign attributes of an autoincrement field or batch autoincrement fields.
                             e.g. {AutoIncrement:{Field:"a",MaxValue:2000}},
                             {AutoIncrement:[{Field:"a",MaxValue:2000},{Field:"a",MaxValue:4000}]}
          \note Can't alter attributes about split in partition collection; After altering a collection to
                be a partition collection, need to split this collection manually
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t setAttributes ( const bson::BSONObj &options );

      int32_t history  ( ldbCursor **cursor, const std::string &id );
      int32_t history  ( ldbCursor **cursor, const std::string &id,
                         std::string &startTP,
                         std::string &endTP);
   private :
      std::mutex           _mutex ;
      _ldb                *_connection ;
      char                *_pSendBuffer ;
      int32_t              _sendBufferSize ;
      char                *_pReceiveBuffer ;
      int32_t              _receiveBufferSize ;
      std::set<uintptr_t>  _cursors ;

      char _collectionSpaceName [ CLIENT_CS_NAMESZ+1 ] ;
      char _collectionName      [ CLIENT_COLLECTION_NAMESZ+1 ] ;
      char _collectionFullName  [ CLIENT_CL_FULLNAME_SZ + 1 ] ;

   private:
      void*    _getConnection () ;
      void     _dropConnection() ;
      void     _regCursor ( ldbCursor *cursor ) ;
      void     _unregCursor ( ldbCursor * cursor ) ;

      int32_t    _queryAndModify( ldbCursor **cursor,
                                const bson::BSONObj &condition,
                                const bson::BSONObj &selected,
                                const bson::BSONObj &orderBy,
                                const bson::BSONObj &hint,
                                const bson::BSONObj &update,
                                uint64_t numToSkip,
                                uint64_t numToReturn,
                                int32_t flag,
                                bool isUpdate,
                                bool returnNew ) ;


      int32_t _update ( const bson::BSONObj &rule,
                      const bson::BSONObj &condition,
                      const bson::BSONObj &hint,
                      int32_t flag,
                      bson::BSONObj *pResult ) ;

      void lock ()
      {
         _mutex.lock () ;
      }
      void unlock ()
      {
         _mutex.unlock () ;
      }

      friend class _ldbCollectionSpace ;
      friend class _ldb ;
      friend class ldbCursor ;

      int32_t _createIndex ( const bson::BSONObj &indexDef, const char *pName,
                           bool isUnique, bool isEnforced,
                           int32_t sortBufferSize ) ;
      int32_t _createIndex ( const bson::BSONObj &indexDef, const char *pIndexName,
                           const bson::BSONObj &options ) ;

      int32_t _alterInternal ( const char * taskName,
                             const bson::BSONObj * argument,
                             bool allowNullArgs ) ;

      int32_t _insert ( const bson::BSONObj &obj,
                      int32_t flags,
                      bson::BSONObj *pResult = nullptr ) ;

      int32_t _query ( ldbCursor **cursor,
                     const bson::BSONObj &condition = _ldbStaticObject,
                     const bson::BSONObj &selected  = _ldbStaticObject,
                     const bson::BSONObj &orderBy   = _ldbStaticObject,
                     const bson::BSONObj &hint      = _ldbStaticObject,
                     uint64_t numToSkip          = 0,
                     uint64_t numToReturn        = -1,
                     int32_t flag               = 0
                   ) ;
   } ;

   /** \enum ldbNodeStatus
       \breif The status of the node.
   */
   enum ldbNodeStatus
   {
      LDB_NODE_ALL = 0,
      LDB_NODE_ACTIVE,
      LDB_NODE_INACTIVE,
      LDB_NODE_UNKNOWN
   } ;

   #define LDB_NODE_INVALID_NODEID     -1

   class DLLEXPORT _ldbNode
   {
   private :
      _ldbNode ( const _ldbNode& other ) ;
      _ldbNode& operator=( const _ldbNode& ) ;

      _ldb                *_connection ;
      char                     _hostName [ OSS_MAX_HOSTNAME + 1 ] ;
      char                     _serviceName [ OSS_MAX_SERVICENAME + 1 ] ;
      char                     _nodeName [ OSS_MAX_HOSTNAME +
                                           OSS_MAX_SERVICENAME + 2 ] ;
      int32_t                    _replicaGroupID ;
      int32_t                    _nodeID ;
      void _dropConnection()
      {
         _connection = nullptr ;
      }
      int32_t _stopStart ( bool start ) ;

      friend class _ldb;

   public :
      _ldbNode ();
      ~_ldbNode ();
      int32_t connect ( _ldb **dbConn );
      int32_t connect ( ldb &dbConn );

      ldbNodeStatus getStatus ();
      const char *getHostName () { return _hostName; }
      const char *getServiceName () { return _serviceName; }
      const char *getNodeName () { return _nodeName; }
      int32_t getNodeID( int32_t &nodeID ) const
      {
         nodeID = _nodeID; return LDB_OK;
      }
      int32_t stop () { return _stopStart(false); }
      int32_t start () { return _stopStart(true); }

      // modify config for the current node
/*      virtual int32_t modifyConfig ( std::map<std::string,std::string>
                                   &config ) = 0 ; */
   } ;

   /** \class ldbNode
       \brief Database operation interfaces of node. This class takes the place of class "ldbReplicaNode".
       \note We use concept "node" instead of "replica node",
               and change the class name "ldbReplicaNode" to "ldbNode".
               class "ldbReplicaNode" will be deprecated in version 2.x.
   */
   class DLLEXPORT ldbNode
   {
   private :
      /** \fn ldbNode ( const ldbNode& other )
          \brief Copy Constructor
          \param[in] A const object reference  of class ldbNode.
      */
      ldbNode ( const ldbNode& other ) ;

      /** \fn ldbNode& operator=( const ldbNode& )
          \brief Assignment constructor
          \param[in] A const reference  of class ldbNode.
          \retval A object const reference  of class ldbNode.
      */
      ldbNode& operator=( const ldbNode& ) ;
   public :
      /** \var pNode
          \breif A pointer of base class _ldbNode

          Class ldbNode is a shell for _ldbNode. We use pNode to
          call the methods in class _ldbNode.
      */
      _ldbNode *pNode ;

      /** \fn ldbNode ()
          \brief Default constructor.
      */
      ldbNode ()
      {
         pNode = nullptr ;
      }

      /** \fn ~ldbNode ()
          \brief Destructor.
      */
      ~ldbNode ()
      {
         if ( pNode )
            delete pNode ;
      }
      /* \fn connect ( _ldb **dbConn )
          \brief Connect to the current node.
          \param [out] dbConn The database obj of current connection
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( _ldb **dbConn )
      {
         if ( !pNode )
            return LDB_NOT_CONNECTED ;
         return pNode->connect ( dbConn ) ;
      }

      /** \fn connect ( ldb &dbConn )
          \brief Connect to the current node.
          \param [out] dbConn The database obj of current connection
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( ldb &dbConn )
      {
         if ( !pNode )
         {
            return LDB_NOT_CONNECTED ;
         }
         // we can not use dbConn.pLDB here,
         // for ldb had not define yet.
         // RELEASE_INNER_HANDLE( dbConn.pLDB ) ;
         return pNode->connect ( dbConn ) ;
      }

      /** \fn const char *getHostName ()
          \brief Get host name of the current node.
          \return The host name.
      */
      const char *getHostName ()
      {
         if ( !pNode )
            return nullptr ;
         return pNode->getHostName () ;
      }

      /** \fn char *getServiceName ()
          \brief Get service name of the current node.
          \return The service name.
      */
      const char *getServiceName ()
      {
         if ( !pNode )
            return nullptr ;
         return pNode->getServiceName () ;
      }

      /** \fn const char *getNodeName ()
          \brief Get node name of the current node.
          \return The node name.
      */
      const char *getNodeName ()
      {
         if ( !pNode )
            return nullptr ;
         return pNode->getNodeName () ;
      }

      /** \fn int32_t getNodeID( int32_t &nodeID )
          \brief Get node id of the current node.
          \return The node id.
      */
      int32_t getNodeID( int32_t &nodeID ) const
      {
         if ( !pNode )
            return LDB_NOT_CONNECTED ;
         return pNode->getNodeID( nodeID ) ;
      }

      /** \fn int32_t  stop ()
          \brief Stop the node.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t  stop ()
      {
         if ( !pNode )
            return LDB_NOT_CONNECTED ;
         return pNode->stop () ;
      }

      /** \fn int32_t start ()
          \brief Start the node.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t start ()
      {
         if ( !pNode )
            return LDB_NOT_CONNECTED ;
         return pNode->start () ;
      }
/*      int32_t modifyConfig ( std::map<std::string,std::string> &config )
      {
         if ( !pNode )
            return nullptr ;
         return pNode->modifyConfig ( config ) ;
      }*/
   } ;

   class DLLEXPORT _ldbCollectionSpace
   {
   private :
      _ldbCollectionSpace ( const _ldbCollectionSpace& other ) ;
      _ldbCollectionSpace& operator=( const _ldbCollectionSpace& ) ;

      std::mutex  _mutex ;

      _ldb                *_connection ;
      char                *_pSendBuffer ;
      int32_t              _sendBufferSize ;
      char                *_pReceiveBuffer ;
      int32_t              _receiveBufferSize ;
      char _collectionSpaceName [ CLIENT_CS_NAMESZ+1 ] ;
      void _setConnection ( _ldb *connection ) ;
      int32_t _setName ( const char *pCollectionSpaceName ) ;
      void _dropConnection()
      {
         _connection = nullptr ;
      }

      friend class _ldb;

   public :
      _ldbCollectionSpace ();
      _ldbCollectionSpace (char *pCSName);
      ~_ldbCollectionSpace ();
      // get a collection object
       int32_t getCollection ( const char *pCollectionName,
                             ldbCollection **collection )  ;


      int32_t createCollection ( const char *pCollection,
                               const bson::BSONObj &options,
                               ldbCollection **collection )  ;

      int32_t createCollection ( const char *pCollection,
                               ldbCollection **collection )  ;
      int32_t dropCollection ( const char *pCollection )  ;
      const char *getCSName ()
      {
         return &_collectionSpaceName[0] ;
      }
      int32_t renameCollection( const char* oldName, const char* newName,
                 const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t alterCollectionSpace ( const bson::BSONObj & options )  ;
      int32_t setAttributes ( const bson::BSONObj & options )  ;
   private:
      int32_t _alterInternal ( const char * taskName,
                             const bson::BSONObj * arguments,
                             bool allowNullArgs ) ;

   } ;
   /** \class ldbCollectionSpace
       \brief Database operation interfaces of collection space
   */
   class DLLEXPORT ldbCollectionSpace
   {
   private :
      /** \fn ldbCollectionSpace ( const ldbCollectionSpace& other )
          \brief Copy constructor.
          \param[in] A const object reference of class ldbCollectionSpace .
      */
      ldbCollectionSpace ( const ldbCollectionSpace& other ) ;

      /** \fn ldbCollectionSpace& operator=( const ldbCollectionSpace& )
          \brief Assignment constructor.
          \param[in] A const object reference of class ldb.
          \retval A const object reference  of class ldb.
      */
      ldbCollectionSpace& operator=( const ldbCollectionSpace& ) ;
   public :
      /** \var pCollectionSpace
          \breif A pointer of base class _ldbCollectionSpace

           Class ldbCollectionSpace is a shell for _ldbCollectionSpace. We use
           pCollectionSpace to call the methods in class _ldbCollectionSpace.
      */
      _ldbCollectionSpace *pCollectionSpace ;

      /** \fn ldbCollectionSpace ()
          \brief Default constructor.
      */
      ldbCollectionSpace ()
      {
         pCollectionSpace = nullptr ;
      }

      /** \fn ~ldbCollectionSpace ()
          \brief Destructor.
      */
      ~ldbCollectionSpace ()
      {
         if ( pCollectionSpace )
            delete pCollectionSpace ;
      }
      /** \fn int32_t getCollection ( const char *pCollectionName,
                                    ldbCollection **collection )
          \brief Get the named collection.
          \param [in] pCollectionName The full name of the collection.
          \param [out] collection The return collection object.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getCollection ( const char *pCollectionName,
                            ldbCollection **collection )
      {
         if ( !pCollectionSpace )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pCollectionSpace->getCollection ( pCollectionName,
                                                  collection ) ;
      }

      /** \fn int32_t createCollection ( const char *pCollection,
                                       const bson::BSONObj &options,
                                       ldbCollection &collection )
          \brief Create the specified collection in current collection space with options
          \param [in] pCollection The collection name
          \param [in] options The options for creating collection or nullptr for not specified any options.
          \param [out] collection The return collection object .
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createCollection ( const char *pCollection,
                               const bson::BSONObj &options,
                               ldbCollection **collection )
      {
         if ( !pCollectionSpace )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pCollectionSpace->createCollection ( pCollection,
                                                     options,
                                                     collection ) ;
      }

      /** \fn int32_t createCollection ( const char *pCollection,
                                       ldbCollection &collection )
          \brief Create the specified collection in current collection space without
                 sharding key and default ReplSize.
          \param [in] pCollection The collection name.
          \param [out] collection The return collection object.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createCollection ( const char *pCollection,
                               ldbCollection **collection )
      {
         if ( !pCollectionSpace )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pCollectionSpace->createCollection ( pCollection,
                                                     collection ) ;
      }

      /** \fn int32_t dropCollection ( const char *pCollection )
          \brief Drop the specified collection in current collection space.
          \param [in] pCollection  The collection name.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t dropCollection ( const char *pCollection )
      {
         if ( !pCollectionSpace )
            return LDB_NOT_CONNECTED ;
         return pCollectionSpace->dropCollection ( pCollection ) ;
      }

      /** \fn const char *getCSName ()
          \brief Get the current collection space name.
          \return The name of current collection space.
      */
      const char *getCSName ()
      {
         if ( !pCollectionSpace )
            return nullptr ;
         return pCollectionSpace->getCSName () ;
      }

      /** \fn int32_t renameCollection(const char* oldName,
                                     const char* newName,
                                     const bson::BSONObj &options )
          \brief Rename collection
          \param [in] oldName The old name of collectionSpace.
          \param [in] newName The new name of collectionSpace.
          \param [in] options Reserved argument
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t renameCollection( const char* oldName, const char* newName,
                              const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pCollectionSpace )
            return LDB_NOT_CONNECTED ;
         return pCollectionSpace->renameCollection( oldName, newName, options ) ;
      }

      /** \fn int32_t alterCollectionSpace ( const bson::BSONObj & options )
          \brief Alter collection space.
          \param [in] options The options of collection space to be changed, e.g. { "PageSize": 4096, "Domain": "mydomain" }.

              PageSize     : The page size of the collection space
              Domain       : The domain which the collection space belongs to

          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t alterCollectionSpace ( const bson::BSONObj & options )
      {
         if ( nullptr == pCollectionSpace )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pCollectionSpace->alterCollectionSpace( options ) ;
      }

      /** \fn int32_t setAttributes ( const bson::BSONObj & options )
          \brief Alter collection space.
          \param [in] options The options of collection space to be changed, e.g. { "PageSize": 4096 }.

              PageSize     : The page size of the collection space

          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t setAttributes ( const bson::BSONObj & options )
      {
         if ( nullptr == pCollectionSpace )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pCollectionSpace->setAttributes( options ) ;
      }
   } ;

   class _ldb
   {
   private :
      _ldb ( const _ldb& other ) ; // non construction-copyable
      _ldb& operator=( const _ldb& ) ; // non copyable

      std::mutex            _mutex ;

      ossSocket             *_sock ;
      char                   _hostName [ OSS_MAX_HOSTNAME + 1 ] ;
      uint16_t               _port ;
      char                  *_pSendBuffer ;
      int32_t                _sendBufferSize ;
      char                  *_pReceiveBuffer ;
      int32_t                _receiveBufferSize ;
      bool                   _endianConvert ;
      bool                   _useSSL ;
      std::set<uintptr_t>    _cursors ;
      std::set<uintptr_t>    _collections ;
      std::set<uintptr_t>    _collectionspaces ;
      std::set<uintptr_t>    _nodes ;
      bson::BSONObj          _attributeCache ;

      const char*            _pErrorBuf ;
      int32_t                _errorBufSize ;
      const char*            _pResultBuf ;
      int32_t                _resultBufSize ;

      // last send or receive time
      ossTimestamp           _lastAliveTime ;

      void _disconnect () ;
      void _setErrorBuffer( const char *pBuf, int32_t bufSize ) ;
      void _setResultBuffer( const char *pBuf, int32_t bufSize ) ;
      int32_t _send ( char *pBuffer ) ;
      int32_t _recv ( char **ppBuffer, int32_t *size ) ;

      int32_t _recvExtract ( char **ppBuffer,
                           int32_t *size,
                           int64_t &contextID,
                           bool *pRemoteErr = nullptr,
                           bool *pHasRecv = nullptr ) ;

      int32_t _reallocBuffer ( char **ppBuffer,
                             int32_t *size,
                             int32_t newSize ) ;

      int32_t _getRetInfo ( char **ppBuffer,
                          int32_t *size,
                          int64_t contextID,
                          ldbCursor **ppCursor ) ;
      int32_t _runCommand ( uint32_t commandID,
                          const bson::BSONObj *arg1 = nullptr,
                          ldbCursor **ppCursor = nullptr ) ;
      int32_t _runQuery ( const char *pString,
                          const bson::BSONObj *arg1 = nullptr,
                          const bson::BSONObj *arg2 = nullptr,
                          const bson::BSONObj *arg3 = nullptr,
                          const bson::BSONObj *arg4 = nullptr,
                          int32_t flag = 0,
                          uint64_t reqID = 0,
                          int64_t numToSkip = -1,
                          int64_t numToReturn = -1,
                          ldbCursor **ppCursor = nullptr ) ;

      int32_t _sendAndRecv( const char *pSendBuf,
                          char **ppRecvBuf,
                          int32_t *recvBufSize,
                          ldbCursor **ppCursor = nullptr,
                          bool needLock = true ) ;

      int32_t _buildEmptyCursor( ldbCursor **ppCursor ) ;
      int32_t _requestSysInfo () ;
      void _regCursor ( ldbCursor *cursor ) ;
      void _regCollection ( ldbCollection *collection ) ;
      void _regCollectionSpace ( _ldbCollectionSpace *collectionspace ) ;
      void _regNode ( _ldbNode *node ) ;
      void _unregCursor ( ldbCursor *cursor ) ;
      void _unregCollection ( ldbCollection *collection ) ;
      void _unregCollectionSpace ( _ldbCollectionSpace *collectionspace ) ;
      void _unregNode ( _ldbNode *node ) ;

      hashTable* _getCachedContainer() const ;

      int32_t _connect( const char *pHostName, uint16_t port ) ;

      int32_t _traceStrtok( bson::BSONArrayBuilder &arrayBuilder, const char* pLine ) ;

      void _clearSessionAttrCache ( bool needLock ) ;
      void _setSessionAttrCache ( const bson::BSONObj & attribute ) ;

      void _getSessionAttrCache ( bson::BSONObj & attribute ) ;

      friend class _ldbCollectionSpace ;
      friend class ldbCollection ;
      friend class ldbCursor ;
      friend class _ldbNode ;
      friend class _ldbReplicaGroup ;
      friend class _ldbDomain ;
      friend class _ldbDataCenter ;

   public :
      _ldb (){}
      _ldb (bool useSSL);
      ~_ldb ();
      int32_t connect ( const char *pHostName, uint16_t port,
                        const char *pUsrName = nullptr,
                        const char *pPasswd = nullptr);
      int32_t connect ( const char *pHostName,
                        const char *pServiceName,
                        const char *pUsrName = nullptr,
                        const char *pPasswd = nullptr);
      int32_t connect ( const char **pConnAddrs,
                        int32_t arrSize,
                        const char *pUsrName,
                        const char *pPasswd );

      void disconnect ()  ;
      bool isConnected ()
      { return nullptr != _sock ; }

      int32_t createUsr( const char *pUsrName,
                       const char *pPasswd,
                       const bson::BSONObj &options = _ldbStaticObject
                      )  ;

      int32_t removeUsr( const char *pUsrName,
                       const char *pPasswd )  ;

      int32_t alterUsr( const char *pUsrName,
                      const char *pAction,
                      const bson::BSONObj &options )  ;

      int32_t changeUsrPasswd( const char *pUsrName,
                             const char *pOldPasswd,
                             const char *pNewPasswd )  ;

      void lock ()
      {
      //   _mutex.lock () ;
      }
      void unlock ()
      {
       //  _mutex.unlock () ;
      }

      int32_t getCollection ( const char *pCollectionFullName,
                            ldbCollection **collection);
      int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                 _ldbCollectionSpace **cs
                               )  ;

      int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                 ldbCollectionSpace &cs)
      {
         RELEASE_INNER_HANDLE( cs.pCollectionSpace ) ;
         return getCollectionSpace ( pCollectionSpaceName,
                                     &cs.pCollectionSpace ) ;
      }

      int32_t createCollectionSpace ( const char *pCollectionSpaceName,
                                    const bson::BSONObj &options,
                                    _ldbCollectionSpace **cs
                                  )  ;

      int32_t createCollectionSpace ( const char *pCollectionSpaceName,
                                    const bson::BSONObj &options,
                                    ldbCollectionSpace &cs)
      {
         RELEASE_INNER_HANDLE( cs.pCollectionSpace ) ;
         return createCollectionSpace ( pCollectionSpaceName, options,
                                        &cs.pCollectionSpace ) ;
      }

      int32_t dropCollectionSpace ( const char *pCollectionSpaceName );

      int32_t execUpdate( const char *sql,
                        bson::BSONObj *pResult = nullptr )  ;

      int32_t exec( const char *sql,
                  ldbCursor **result )  ;

      int32_t transactionBegin()  ;
      int32_t transactionCommit()  ;
      int32_t transactionRollback()  ;
      int32_t flushConfigure( const bson::BSONObj &options )  ;

      // set session attribute
      int32_t setSessionAttr ( const bson::BSONObj &options =
                              _ldbStaticObject )  ;
      // get session attribute
      int32_t getSessionAttr ( bson::BSONObj &result,
                             bool useCache = true )  ;

      // close all cursor
      int32_t closeAllCursors ()  ;

      // interrupt
      int32_t interrupt()  ;
      int32_t interruptOperation()  ;

      // connection is valid
      int32_t isValid( bool *result )  ;
      bool isValid()  ;

      bool isClosed()  ;

      static _ldb *getObj ( bool useSSL = false ) ;

      // get last alive time
      uint64_t getLastAliveTime() const { return _lastAliveTime.time; }

      int32_t forceSession(
         int64_t sessionID,
         const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t forceStepUp(
         const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t reloadConfig(
         const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t updateConfig ( const bson::BSONObj &configs = _ldbStaticObject,
                            const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t deleteConfig ( const bson::BSONObj &configs = _ldbStaticObject,
                           const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t setPDLevel( int32_t level,
         const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t msg( const char* msg )  ;

      int32_t loadCS( const char* csName,
         const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t unloadCS( const char* csName,
         const bson::BSONObj &options = _ldbStaticObject )  ;


      int32_t renameCollectionSpace( const char* oldName,
                                   const char* newName,
                const bson::BSONObj &options = _ldbStaticObject )  ;

      int32_t getLastErrorObj( bson::BSONObj &result )  ;
      void  cleanLastErrorObj()  ;

      int32_t getLastResultObj( bson::BSONObj &result,
                              bool getOwned = false ) const  ;

      /// LedgerDB specific API ///
      int32_t transHistory(ldbCursor **cursor, uint32_t trxID);
      int32_t transHistory(ldbCursor **cursor, uint32_t trxID,
                           std::string &startTS, std::string &endTS);
      int32_t getDigest(bson::BSONObj &result,
                        const bson::BSONObj &address = _ldbStaticObject);
      int32_t getEntry(bson::BSONObj &result, const bson::BSONObj &address);
      int32_t verifyJournal(bson::BSONObj &result, const bson::BSONObj &digest);
      int32_t verifyDocument(bson::BSONObj &result,
                             const bson::BSONObj &address, const std::string &id,
                             const std::string &revision_hash);
      int32_t verifyEntry(bson::BSONObj &result, const bson::BSONObj &entry);
      /// End LedgerDB specific API ///
   } ;
   /** \class ldb
       \brief Database operation interfaces of admin.
   */
   class ldb
   {
   private:
      ldb ( const ldb& other ) ;
      ldb& operator=( const ldb& ) ;
   public :
      /** \var pLDB
          \breif A pointer of virtual base class _ldb

          Class ldb is a shell for _ldb. We use pLDB to
          call the methods in class _ldb.
      */
      _ldb *pLDB ;

      /** \fn ldb ( bool useSSL = false )
          \brief Default constructor.
          \param [in] useSSL Set whether use the SSL or not, default is false.
      */
      ldb ( bool useSSL = false ) :
      pLDB ( _ldb::getObj( useSSL ) )
      {
      }

      /** \fn ~ldb()
          \brief Destructor.
      */
      ~ldb ()
      {
         if ( pLDB )
            delete pLDB ;
      }

      /** \fn int32_t connect ( const char *pHostName,
                            uint16_t port
                          )
          \brief Connect to remote Database Server.
          \param [in] pHostName The Host Name or IP Address of Database Server.
          \param [in] port The Port of Database Server.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( const char *pHostName,
                      uint16_t port
                    )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->connect ( pHostName, port ) ;
      }

      /** \fn int32_t connect ( const char *pHostName,
                           uint16_t port,
                           const char *pUsrName,
                           const char *pPasswd
                           )
          \brief Connect to remote Database Server.
          \param [in] pHostName The Host Name or IP Address of Database Server.
          \param [in] port The Port of Database Server.
          \param [in] pUsrName The connection user name.
          \param [in] pPasswd The connection password.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( const char *pHostName,
                      uint16_t port,
                      const char *pUsrName,
                      const char *pPasswd
                      )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->connect ( pHostName, port,
                                pUsrName, pPasswd ) ;
      }

      /** \fn int32_t connect ( const char *pHostName,
                            const char *pServiceName
                          )
          \brief Connect to remote Database Server.
          \param [in] pHostName The Host Name or IP Address of Database Server.
          \param [in] pServiceName The Service Name of Database Server.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( const char *pHostName,
                      const char *pServiceName
                    )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->connect ( pHostName, pServiceName ) ;
      }

      /** \fn int32_t connect ( const char *pHostName,
                           const char *pServiceName,
                           const char *pUsrName,
                           const char *pPasswd
                           )
          \brief Connect to remote Database Server.
          \param [in] pHostName The Host Name or IP Address of Database Server.
          \param [in] pServiceName The Service Name of Database Server.
          \param [in] pUsrName The connection user name.
          \param [in] pPasswd The connection password.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( const char *pHostName,
                      const char *pServiceName,
                      const char *pUsrName,
                      const char *pPasswd )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->connect ( pHostName, pServiceName,
                                 pUsrName, pPasswd ) ;
      }

      /** \fn int32_t connect ( const char **pConnAddrs,
                              int32_t arrSize,
                              const char *pUsrName,
                              const char *pPasswd
                            )
          \brief Connect to database used  a random  valid address in the array.
          \param [in] pConnAddrs The array of the coord's address
          \param [in] arrSize The size of the array
          \param [in] pUsrName The connection user name.
          \param [in] pPasswd The connection password.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t connect ( const char **pConnAddrs,
                      int32_t arrSize,
                      const char *pUsrName,
                      const char *pPasswd,
                      const char *pCipher = nullptr )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->connect ( pConnAddrs, arrSize,
                                 pUsrName, pPasswd ) ;
      }

      /** \fn int32_t createUsr( const char *pUsrName,
                               const char *pPasswd,
                               const bson::BSONObj &options = _ldbStaticObject )
          \brief Add an user in current database.
          \param [in] pUsrName The connection user name.
          \param [in] pPasswd The connection password.
          \param [in] options The options for user, such as: { AuditMask:"DDL|DML" }

              AuditMask : User audit log mask, value list:
                          ACCESS,CLUSTER,SYSTEM,DML,DDL,DCL,DQL,INSERT,DELETE,
                          UPDATE,OTHER.
                          You can combine multiple values with '|'. 'ALL' means
                          that all mask items are turned on, and 'NONE' means
                          that no mask items are turned on.
                          If an item in the user audit log is not configured, the
                          configuration of the corresponding mask item on the node
                          is inherited. You can also use '!' to disable inheritance
                          of this mask( e.g. "!DDL|DML" ).

          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createUsr( const char *pUsrName,
                       const char *pPasswd,
                       const bson::BSONObj &options = _ldbStaticObject )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->createUsr( pUsrName, pPasswd, options ) ;
      }

      /** \fn int32_t removeUsr( const char *pUsrName,
                                 const char *pPasswd )
          \brief Remove the spacified user from current database.
          \param [in] pUsrName The connection user name.
          \param [in] pPasswd The connection password.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t removeUsr( const char *pUsrName,
                       const char *pPasswd )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->removeUsr( pUsrName, pPasswd ) ;
      }

      /** \fn int32_t alterUsr( const char *pUsrName,
                              const char *pAction,
                              const bson::BSONObj &options )
          \brief Alter the spacified user's information.
          \param [in] pUsrName The username needed to alter information.
          \param [in] pAction The alter action.
          \param [in] options The action corresponding options.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
          \note Alter action and options list:
                "set attributes" : { AuditMask : ... }
      */
      int32_t alterUsr( const char *pUsrName,
                      const char *pAction,
                      const bson::BSONObj &options )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->alterUsr( pUsrName, pAction, options ) ;
      }

      /** \fn int32_t changeUsrPasswd( const char *pUsrName,
                                     const char *pOldPasswd,
                                     const char *pNewPasswd )
          \brief Change the spacified user's password.
          \param [in] pUsrName The username needed to change password.
          \param [in] pOldPasswd The old password.
          \param [in] pNewPasswd The new password.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t changeUsrPasswd( const char *pUsrName,
                             const char *pOldPasswd,
                             const char *pNewPasswd )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->changeUsrPasswd( pUsrName, pOldPasswd, pNewPasswd ) ;
      }

      /** \fn void disconnect ()
          \brief Disconnect the remote Database Server.
      */
      void disconnect ()
      {
         if ( !pLDB )
            return ;
         pLDB->disconnect () ;
      }

      /** \fn int32_t getCollection ( const char *pCollectionFullName,
                                  ldbCollection &collection
                                )
          \biref Get the specified collection.
          \param [in] pCollectionFullName The full name of collection.
          \param [out] collection The return collection object of query.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getCollection ( const char *pCollectionFullName,
                            ldbCollection **collection
                          )
      {
         if ( !pLDB )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pLDB->getCollection ( pCollectionFullName,
                                      collection ) ;
      }

      /* \fn int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                       _ldbCollectionSpace **cs)
           \brief Get the specified collection space.
           \param [in] pCollectionSpaceName The name of collection space.
          \param [out] cs The return collection space handle of query.
           \retval LDB_OK Operation Success
           \retval Others Operation Fail
      */
      int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                 _ldbCollectionSpace **cs
                               )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->getCollectionSpace ( pCollectionSpaceName,
                                           cs ) ;
      }

      /** \fn int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                        ldbCollectionSpace &cs)
           \brief Get the specified collection space.
           \param [in] pCollectionSpaceName The name of collection space.
           \param [out] cs The return collection space object of query.
           \retval LDB_OK Operation Success
           \retval Others Operation Fail
      */
      int32_t getCollectionSpace ( const char *pCollectionSpaceName,
                                 ldbCollectionSpace &cs
                               )
      {
         if ( !pLDB )
         {
            return LDB_NOT_CONNECTED ;
         }
         RELEASE_INNER_HANDLE( cs.pCollectionSpace ) ;
         return pLDB->getCollectionSpace ( pCollectionSpaceName,
                                           cs ) ;
      }

      int32_t createCollectionSpace ( const char *pCollectionSpaceName,
                                    const bson::BSONObj &options,
                                    _ldbCollectionSpace **cs
                                  )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->createCollectionSpace ( pCollectionSpaceName,
                                              options, cs ) ;
      }

      /** \fn int32_t createCollectionSpace ( const char *pCollectionSpaceName,
                                            const bson::BSONObj &options,
                                            ldbCollectionSpace &cs
                                           )
          \brief Create collection space with specified pagesize.
          \param [in] pCollectionSpaceName The name of collection space.
          \param [in] options The options specified by user, e.g. {"PageSize": 4096}

              PageSize   : Assign the pagesize of the collection space
          \param [out] cs The return collection space object of creation.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t createCollectionSpace ( const char *pCollectionSpaceName,
                                    const bson::BSONObj &options,
                                    ldbCollectionSpace &cs
                                  )
      {
         if ( !pLDB )
         {
            return LDB_NOT_CONNECTED ;
         }
         RELEASE_INNER_HANDLE( cs.pCollectionSpace ) ;
         return pLDB->createCollectionSpace ( pCollectionSpaceName,
                                              options, cs ) ;
      }

      /** \fn int32_t dropCollectionSpace ( const char *pCollectionSpaceName )
          \brief Remove the specified collection space.
          \param [in] pCollectionSpaceName The name of collection space.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t dropCollectionSpace ( const char *pCollectionSpaceName )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->dropCollectionSpace ( pCollectionSpaceName ) ;
      }

      /** \fn int32_t execUpdate( const char *sql )
          \brief Executing SQL command for updating.
          \param [in] sql The SQL command.
          \param [out] pResult The detail result info
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t execUpdate( const char *sql, bson::BSONObj *pResult = nullptr )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->execUpdate( sql, pResult ) ;
      }

      /* \fn int32_t exec( const char *sql,
                        ldbCursor **result )
          \brief Executing SQL command.
          \param [in] sql The SQL command.
          \param [out] result The return cursor handle of matching documents.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t exec( const char *sql,
                  ldbCursor **result )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->exec( sql, result ) ;
      }

      /** \fn int32_t transactionBegin()
          \brief Transaction commit.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t transactionBegin()
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->transactionBegin() ;
      }

      /** \fn int32_t transactionCommit()
          \brief Transaction commit.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t transactionCommit()
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->transactionCommit() ;
      }

      /** \fn int32_t transactionRollback()
          \brief Transaction rollback.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t transactionRollback()
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->transactionRollback() ;
      }
      /** \fn int32_t flushConfigure( BSONObj &options )
          \brief flush the options to configure file.
          \param [in] options The configure infomation, pass {"Global":true} or {"Global":false}
                          In cluster environment, passing {"Global":true} will flush data's and catalog's configuration file,
                          while passing {"Global":false} will flush coord's configuration file.
                          In stand-alone environment, both them have the same behaviour.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t flushConfigure( const bson::BSONObj &options )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->flushConfigure( options ) ;
      }

      /** \fn int32_t setSessionAttr ( const bson::BSONObj &options ) ;
          \brief Set the attributes of the session.
          \param [in] options The options for setting session attributes. Can not be
                      nullptr. While it's a empty options, the local session attributes
                      cache will be cleanup. Please reference
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t setSessionAttr ( const bson::BSONObj &options = _ldbStaticObject )
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->setSessionAttr ( options ) ;
      }

      /** \fn int32_t getSessionAttr ( bson::BSONObj & result,
                                     bool useCache = true) ;
          \brief Get the attributes of the current session.
          \param [out] result The return bson object.
          \param [in] useCache Whether to use cache in local, default to be true.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t getSessionAttr ( bson::BSONObj & result,
                             bool useCache = true )
      {
         if ( !pLDB )
         {
            return LDB_NOT_CONNECTED ;
         }
         return pLDB->getSessionAttr( result, useCache ) ;
      }

      /** \fn int32_t closeAllCursors () ;
          \brief Send a "Interrpt" message to engine, as a result, all the cursors
                 created by current connection will be closed.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t closeAllCursors ()
      {
         return interrupt() ;
      }

      /** \fn int32_t interrupt () ;
          \brief Send "INTERRUPT" message to engine, as a result, all the cursors
                 created by current connection will be closed.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t interrupt()
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->interrupt () ;
      }

      /** \fn int32_t interruptOperation () ;
          \brief Send "INTERRUPT_SELF" message to engine to stop the current
                 operation. When the current operation had finish, nothing
                 happend, Otherwise, the current operation will be stop, and
                 return error.
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t interruptOperation()
      {
         if ( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->interruptOperation () ;
      }

      /** \fn bool isValid ()
          \brief Judge whether the connection is valid.
          \retval true for the connection is valid while false for not
      */
      bool isValid ()
      {
         if ( !pLDB )
            return false ;
         return pLDB->isValid () ;
      }

      /** \fn bool isClosed()
          \brief Judge whether the connection has been closed.
          \retval true or false
      */
      bool isClosed()
      {
         if (!pLDB)
            return true ;
         return pLDB->isClosed() ;
      }

      /** \fn uint64_t getLastAliveTime()
          \brief Get the number of seconds from the standard time point
          (usually in the midnight of January 1, 1970) to the last alive time
          \retval uint64_t time difference, unit for seconds
      */
      uint64_t getLastAliveTime() const { return pLDB->getLastAliveTime(); }

      /** \fn int32_t forceSession(int64_t sessionID,
                                 const bson::BSONObj &options)
          \brief Stop the specified session's current operation and terminate it
          \param [in] sessionID The ID of the session.
          \param [in] options The control options:(Only take effect in coordinate nodes)

              GroupID:int32_t,
              GroupName:String,
              NodeID:int32_t,
              HostName:String,
              svcname:String,
              ...
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t forceSession( int64_t sessionID,
                          const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->forceSession( sessionID, options ) ;
      }

      /** \fn int32_t reloadConfig(const bson::BSONObj &options)
          \brief Force the node to reload config from file and take effect.
          \param [in] options The control options:(Only take effect in coordinate nodes)

              GroupID:int32_t,
              GroupName:String,
              NodeID:int32_t,
              HostName:String,
              svcname:String,
              ...
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t reloadConfig( const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->reloadConfig( options ) ;
      }

      /** \fn int32_t updateConfig(const bson::BSONObj &configs,
                                 const bson::BSONObj &options)
          \brief Update node config and take effect.
          \param [in] configs the specific configuration parameters to update.
                { diaglevel:3 } Modify diaglevel as 3.
          \param [in] options The control options:(Only take effect in coordinate nodes)

              GroupID:int32_t,
              GroupName:String,
              NodeID:int32_t,
              HostName:String,
              svcname:String,
              ...
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t updateConfig( const bson::BSONObj &configs = _ldbStaticObject,
                          const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->updateConfig( configs, options ) ;
      }

      /** \fn int32_t deleteConfig(const bson::BSONObj &configs,
                                 const bson::BSONObj &options)
          \brief Delete node config and take effect.
          \param [in] configs the specific configuration parameters to delete.
                { diaglevel:1 } Delete diaglevel config and restore to default value.
          \param [in] options The control options:(Only take effect in coordinate nodes)

              GroupID:int32_t,
              GroupName:String,
              NodeID:int32_t,
              HostName:String,
              svcname:String,
              ...
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t deleteConfig( const bson::BSONObj &configs = _ldbStaticObject,
                          const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->deleteConfig( configs, options ) ;
      }

      /** \fn int32_t setPDLevel(int32_t level,
                               const bson::BSONObj &options)
          \brief Set the node's diagnostic level and take effect.
          \param [in] level The diagnostic level, value 0~5. value means:

              0: SEVERE
              1: ERROR
              2: EVENT
              3: WARNING
              4: INFO
              5: DEBUG
          \param [in] options The control options:(Only take effect in coordinate nodes)

              GroupID:int32_t,
              GroupName:String,
              NodeID:int32_t,
              HostName:String,
              svcname:String,
              ...
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t setPDLevel( int32_t level,
                        const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->setPDLevel( level, options ) ;
      }

      int32_t msg( const char* msg )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->msg( msg ) ;
      }

      /** \fn int32_t renameCollectionSpace(const char* oldName,
                                          const char* newName,
                                          const bson::BSONObj &options )
          \brief Rename collectionSpace
          \param [in] oldName The old name of collectionSpace.
          \param [in] newName The new name of collectionSpace.
          \param [in] options Reserved argument
          \retval LDB_OK Operation Success
          \retval Others Operation Fail
      */
      int32_t renameCollectionSpace( const char* oldName,
                                   const char* newName,
                                   const bson::BSONObj &options = _ldbStaticObject )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->renameCollectionSpace( oldName, newName, options ) ;
      }

      /** \fn int32_t getLastErrorObj( bson::BSONObj &errObj )
          \brief Get the error object(only return by engine) of the last operation.
                 The error object will not be clean up automatically until the next
                 error object cover it.
          \param [out] errObj The return error bson object. Please reference
          \retval LDB_OK Operation Success.
          \retval LDB_DMS_EOC There is no error object.
          \retval Others Operation Fail.
      */
      int32_t getLastErrorObj( bson::BSONObj &errObj )
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->getLastErrorObj( errObj ) ;
      }

      /** \fn void cleanLastErrorObj()
          \brief Clean the last error object(returned by engine) of current connection.
      */
      void cleanLastErrorObj()
      {
         if( !pLDB ) return  ;
         return pLDB->cleanLastErrorObj() ;
      }

      /** \fn int32_t getLastResultObj( bson::BSONObj &result ) const
          \brief Get the result object(only return by engine) of the last operation.
                 The result object will not be clean up automatically until the next
                 result object cover it.
          \param [out] result The return result bson object.
          \param [in]  getOwned Wether the return result bson object should get
                       owned memory or not.
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t getLastResultObj( bson::BSONObj &result,
                              bool getOwned = false ) const
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->getLastResultObj( result, getOwned ) ;
      }

      int32_t transHistory(ldbCursor **cursor, uint32_t trxID){
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->transHistory(cursor, trxID);
      }
      int32_t transHistory(ldbCursor **cursor, uint32_t trxID,
                           std::string &startTS, std::string &endTS){
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->transHistory(cursor, trxID, startTS, endTS);
      }
      /** \fn int32_t getDigest( bson::BSONObj &result )
          \brief Retrieve the database ledger digest.
          \param [out] result The ledger digest
          \param [in] address An address object describing the log head to digest
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t getDigest (bson::BSONObj &result,
                         const bson::BSONObj &address = _ldbStaticObject)
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->getDigest(result, address);
      }

      /** \fn int32_t getEntry( bson::BSONObj &result )
          \brief Retrieve an entry from the journal
          \param [out] result The ledger entry
          \param [in] address An address object describing the entry to get
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t getEntry (bson::BSONObj &result, const bson::BSONObj &address)
      {
         if( !pLDB )
            return LDB_NOT_CONNECTED ;
         return pLDB->getEntry(result, address);
      }
      /** \fn int32_t verifyJournal( bson::BSONObj &result )
          \brief Check that the journal is consistent
          \param [out] A list of hash strings representing the consistency proof
          \param [in] digest A digest object describing the previous digest to verify
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t verifyJournal(bson::BSONObj &result,
                            const bson::BSONObj &digest)
      {
          if (!pLDB)
              return LDB_NOT_CONNECTED;
          return pLDB->verifyJournal(result, digest);
      }

      /** \fn int32_t verifyDocument( bson::BSONObj &result )
          \brief Check that a document revision is valid
          \param [out] result A list of hash strings representing the audit path.
          \param [in] address An address object describing the position of the entry
          \param [in] id      ID string describing the document in the entry
          \param [in] revision_hash Hash of the document's revision to verify
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t verifyDocument(bson::BSONObj &result,
                             const bson::BSONObj &address,
                             const std::string &id,
                             const std::string &revision_hash)
      {
          if (!pLDB)
              return LDB_NOT_CONNECTED;
          return pLDB->verifyDocument(result, address, id, revision_hash);
      }

      /** \fn int32_t verifyEntry( bson::BSONObj &result )
          \brief Check that an entry is valid
          \param [out] result A list of hash strings representing the audit path.
          \param [in] entry An entry object, output from a getEntry() call
          \retval LDB_OK Operation Success.
          \retval Others Operation Fail.
      */
      int32_t verifyEntry(bson::BSONObj &result, const bson::BSONObj &entry)
      {
          if (!pLDB)
              return LDB_NOT_CONNECTED;
          return pLDB->verifyEntry(result, entry);
      }
   } ;
}

#endif

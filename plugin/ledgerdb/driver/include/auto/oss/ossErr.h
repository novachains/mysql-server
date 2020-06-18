/******************************************************************************


   Copyright (C) 2011-2020 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.


******************************************************************************/

// This Header File is automatically generated, you MUST NOT modify this file anyway!
// On the contrary, you can modify the file "novachains/misc/resource/rclist.xml" if necessary!

#ifndef OSSERR_H_
#define OSSERR_H_

#include "oss/core.h"
#include "oss/ossFeat.h"

#define LDB_MAX_ERROR                         1024
#define LDB_MAX_WARNING                       1024
#define LDB_OK                                0

/** \fn char* getErrDesp ( int errCode )
    \brief Error Code.
    \param [in] errCode The number of the error code
    \returns The meaning of the error code
 */
const char* getErrDesp ( int errCode ) ;

#define LDB_IO                                -1    /**< IO Exception */
#define LDB_OOM                               -2    /**< Out of Memory */
#define LDB_PERM                              -3    /**< Permission Error */
#define LDB_FNE                               -4    /**< File Not Exist */
#define LDB_FE                                -5    /**< File Exist */
#define LDB_INVALIDARG                        -6    /**< Invalid Argument */
#define LDB_INVALIDSIZE                       -7    /**< Invalid size */
#define LDB_INTERRUPT                         -8    /**< Interrupt */
#define LDB_EOF                               -9    /**< Hit end of file */
#define LDB_SYS                               -10   /**< System error */
#define LDB_NOSPC                             -11   /**< No space is left on disk */
#define LDB_EDU_INVAL_STATUS                  -12   /**< EDU status is not valid */
#define LDB_TIMEOUT                           -13   /**< Timeout error */
#define LDB_QUIESCED                          -14   /**< Database is quiesced */
#define LDB_NETWORK                           -15   /**< Network error */
#define LDB_NETWORK_CLOSE                     -16   /**< Network is closed from remote */
#define LDB_DATABASE_DOWN                     -17   /**< Database is in shutdown status */
#define LDB_APP_FORCED                        -18   /**< Application is forced */
#define LDB_INVALIDPATH                       -19   /**< Given path is not valid */
#define LDB_INVALID_FILE_TYPE                 -20   /**< Unexpected file type specified */
#define LDB_DMS_NOSPC                         -21   /**< There's no space for DMS */
#define LDB_DMS_EXIST                         -22   /**< Collection already exists */
#define LDB_DMS_NOTEXIST                      -23   /**< Collection does not exist */
#define LDB_DMS_RECORD_TOO_BIG                -24   /**< User record is too large */
#define LDB_DMS_RECORD_NOTEXIST               -25   /**< Record does not exist */
#define LDB_DMS_OVF_EXIST                     -26   /**< Remote overflow record exists */
#define LDB_DMS_RECORD_INVALID                -27   /**< Invalid record */
#define LDB_DMS_SU_NEED_REORG                 -28   /**< Storage unit need reorg */
#define LDB_DMS_EOC                           -29   /**< End of collection */
#define LDB_DMS_CONTEXT_IS_OPEN               -30   /**< Context is already opened */
#define LDB_DMS_CONTEXT_IS_CLOSE              -31   /**< Context is closed */
#define LDB_OPTION_NOT_SUPPORT                -32   /**< Option is not supported yet */
#define LDB_DMS_CS_EXIST                      -33   /**< Collection space already exists */
#define LDB_DMS_CS_NOTEXIST                   -34   /**< Collection space does not exist */
#define LDB_DMS_INVALID_SU                    -35   /**< Storage unit file is invalid */
#define LDB_RTN_CONTEXT_NOTEXIST              -36   /**< Context does not exist */
#define LDB_IXM_MULTIPLE_ARRAY                -37   /**< More than one fields are array type */
#define LDB_IXM_DUP_KEY                       -38   /**< Duplicate key exist */
#define LDB_IXM_KEY_TOO_LARGE                 -39   /**< Index key is too large */
#define LDB_IXM_NOSPC                         -40   /**< No space can be found for index extent */
#define LDB_IXM_KEY_NOTEXIST                  -41   /**< Index key does not exist */
#define LDB_DMS_MAX_INDEX                     -42   /**< Hit max number of index */
#define LDB_DMS_INIT_INDEX                    -43   /**< Failed to initialize index */
#define LDB_DMS_COL_DROPPED                   -44   /**< Collection is dropped */
#define LDB_IXM_IDENTICAL_KEY                 -45   /**< Two records get same key and rid */
#define LDB_IXM_EXIST                         -46   /**< Duplicate index name */
#define LDB_IXM_NOTEXIST                      -47   /**< Index name does not exist */
#define LDB_IXM_UNEXPECTED_STATUS             -48   /**< Unexpected index flag */
#define LDB_IXM_EOC                           -49   /**< Hit end of index */
#define LDB_IXM_DEDUP_BUF_MAX                 -50   /**< Hit the max of dedup buffer */
#define LDB_RTN_INVALID_PREDICATES            -51   /**< Invalid predicates */
#define LDB_RTN_INDEX_NOTEXIST                -52   /**< Index does not exist */
#define LDB_RTN_INVALID_HINT                  -53   /**< Invalid hint */
#define LDB_DMS_NO_MORE_TEMP                  -54   /**< No more temp collections are avaliable */
#define LDB_DMS_SU_OUTRANGE                   -55   /**< Exceed max number of storage unit */
#define LDB_IXM_DROP_ID                       -56   /**< $id index can't be dropped */
#define LDB_DPS_LOG_NOT_IN_BUF                -57   /**< Log was not found in log buf */
#define LDB_DPS_LOG_NOT_IN_FILE               -58   /**< Log was not found in log file */
#define LDB_PMD_RG_NOT_EXIST                  -59   /**< Replication group does not exist */
#define LDB_PMD_RG_EXIST                      -60   /**< Replication group exists */
#define LDB_INVALID_REQID                     -61   /**< Invalid request id is received */
#define LDB_PMD_SESSION_NOT_EXIST             -62   /**< Session ID does not exist */
#define LDB_PMD_FORCE_SYSTEM_EDU              -63   /**< System EDU cannot be forced */
#define LDB_NOT_CONNECTED                     -64   /**< Database is not connected */
#define LDB_UNEXPECTED_RESULT                 -65   /**< Unexpected result received */
#define LDB_CORRUPTED_RECORD                  -66   /**< Corrupted record */
#define LDB_BACKUP_HAS_ALREADY_START          -67   /**< Backup has already been started */
#define LDB_BACKUP_NOT_COMPLETE               -68   /**< Backup is not completed */
#define LDB_RTN_IN_BACKUP                     -69   /**< Backup is in progress */
#define LDB_BAR_DAMAGED_BK_FILE               -70   /**< Backup is corrupted */
#define LDB_RTN_NO_PRIMARY_FOUND              -71   /**< No primary node was found */
#define LDB_ERROR_RESERVED_1                  -72   /**< Reserved */
#define LDB_PMD_HELP_ONLY                     -73   /**< Engine help argument is specified */
#define LDB_PMD_CON_INVALID_STATE             -74   /**< Invalid connection state */
#define LDB_CLT_INVALID_HANDLE                -75   /**< Invalid handle */
#define LDB_CLT_OBJ_NOT_EXIST                 -76   /**< Object does not exist */
#define LDB_NET_ALREADY_LISTENED              -77   /**< Listening port is already occupied */
#define LDB_NET_CANNOT_LISTEN                 -78   /**< Unable to listen the specified address */
#define LDB_NET_CANNOT_CONNECT                -79   /**< Unable to connect to the specified address */
#define LDB_NET_NOT_CONNECT                   -80   /**< Connection does not exist */
#define LDB_NET_SEND_ERR                      -81   /**< Failed to send */
#define LDB_NET_TIMER_ID_NOT_FOUND            -82   /**< Timer does not exist */
#define LDB_NET_ROUTE_NOT_FOUND               -83   /**< Route info does not exist */
#define LDB_NET_BROKEN_MSG                    -84   /**< Broken msg */
#define LDB_NET_INVALID_HANDLE                -85   /**< Invalid net handle */
#define LDB_DMS_INVALID_REORG_FILE            -86   /**< Invalid reorg file */
#define LDB_DMS_REORG_FILE_READONLY           -87   /**< Reorg file is in read only mode */
#define LDB_DMS_INVALID_COLLECTION_S          -88   /**< Collection status is not valid */
#define LDB_DMS_NOT_IN_REORG                  -89   /**< Collection is not in reorg state */
#define LDB_REPL_GROUP_NOT_ACTIVE             -90   /**< Replication group is not activated */
#define LDB_REPL_INVALID_GROUP_MEMBER         -91   /**< Node does not belong to the group */
#define LDB_DMS_INCOMPATIBLE_MODE             -92   /**< Collection status is not compatible */
#define LDB_DMS_INCOMPATIBLE_VERSION          -93   /**< Incompatible version for storage unit */
#define LDB_REPL_LOCAL_G_V_EXPIRED            -94   /**< Version is expired for local group */
#define LDB_DMS_INVALID_PAGESIZE              -95   /**< Invalid page size */
#define LDB_REPL_REMOTE_G_V_EXPIRED           -96   /**< Version is expired for remote group */
#define LDB_CLS_VOTE_FAILED                   -97   /**< Failed to vote for primary */
#define LDB_DPS_CORRUPTED_LOG                 -98   /**< Log record is corrupted */
#define LDB_DPS_LSN_OUTOFRANGE                -99   /**< LSN is out of boundary */
#define LDB_UNKNOWN_MESSAGE                   -100  /**< Unknown message is received */
#define LDB_NET_UPDATE_EXISTING_NODE          -101  /**< Updated information is same as old one */
#define LDB_CLS_UNKNOW_MSG                    -102  /**< Unknown message */
#define LDB_CLS_EMPTY_HEAP                    -103  /**< Empty heap */
#define LDB_CLS_NOT_PRIMARY                   -104  /**< Node is not primary */
#define LDB_CLS_NODE_NOT_ENOUGH               -105  /**< Not enough number of data nodes */
#define LDB_CLS_NO_CATALOG_INFO               -106  /**< Catalog information does not exist on data node */
#define LDB_CLS_DATA_NODE_CAT_VER_OLD         -107  /**< Catalog version is expired on data node */
#define LDB_CLS_COORD_NODE_CAT_VER_OLD        -108  /**< Catalog version is expired on coordinator node */
#define LDB_CLS_INVALID_GROUP_NUM             -109  /**< Exceeds the max group size */
#define LDB_CLS_SYNC_FAILED                   -110  /**< Failed to sync log */
#define LDB_CLS_REPLAY_LOG_FAILED             -111  /**< Failed to replay log */
#define LDB_REST_EHS                          -112  /**< Invalid HTTP header */
#define LDB_CLS_CONSULT_FAILED                -113  /**< Failed to negotiate */
#define LDB_DPS_MOVE_FAILED                   -114  /**< Failed to change DPS metadata */
#define LDB_DMS_CORRUPTED_SME                 -115  /**< SME is corrupted */
#define LDB_APP_INTERRUPT                     -116  /**< Application is interrupted */
#define LDB_APP_DISCONNECT                    -117  /**< Application is disconnected */
#define LDB_OSS_CCE                           -118  /**< Character encoding errors */
#define LDB_COORD_QUERY_FAILED                -119  /**< Failed to query on coord node */
#define LDB_CLS_BUFFER_FULL                   -120  /**< Buffer array is full */
#define LDB_RTN_SUBCONTEXT_CONFLICT           -121  /**< Sub context is conflict */
#define LDB_COORD_QUERY_EOC                   -122  /**< EOC message is received by coordinator node */
#define LDB_DPS_FILE_SIZE_NOT_SAME            -123  /**< Size of DPS files are not the same */
#define LDB_DPS_FILE_NOT_RECOGNISE            -124  /**< Invalid DPS log file */
#define LDB_OSS_NORES                         -125  /**< No resource is avaliable */
#define LDB_DPS_INVALID_LSN                   -126  /**< Invalid LSN */
#define LDB_OSS_NPIPE_DATA_TOO_BIG            -127  /**< Pipe buffer size is too small */
#define LDB_CAT_AUTH_FAILED                   -128  /**< Catalog authentication failed */
#define LDB_CLS_FULL_SYNC                     -129  /**< Full sync is in progress */
#define LDB_CAT_ASSIGN_NODE_FAILED            -130  /**< Failed to assign data node from coordinator node */
#define LDB_PHP_DRIVER_INTERNAL_ERROR         -131  /**< PHP driver internal error */
#define LDB_COORD_SEND_MSG_FAILED             -132  /**< Failed to send the message */
#define LDB_CAT_NO_NODEGROUP_INFO             -133  /**< No activated group information on catalog */
#define LDB_COORD_REMOTE_DISC                 -134  /**< Remote-node is disconnected */
#define LDB_CAT_NO_MATCH_CATALOG              -135  /**< Unable to find the matched catalog information */
#define LDB_CLS_UPDATE_CAT_FAILED             -136  /**< Failed to update catalog */
#define LDB_COORD_UNKNOWN_OP_REQ              -137  /**< Unknown request operation code */
#define LDB_COOR_NO_NODEGROUP_INFO            -138  /**< Group information cannot be found on coordinator node */
#define LDB_DMS_CORRUPTED_EXTENT              -139  /**< DMS extent is corrupted */
#define LDBCM_FAIL                            -140  /**< Remote cluster manager failed */
#define LDBCM_STOP_PART                       -141  /**< Remote database services have been stopped */
#define LDBCM_SVC_STARTING                    -142  /**< Service is starting */
#define LDBCM_SVC_STARTED                     -143  /**< Service has been started */
#define LDBCM_SVC_RESTARTING                  -144  /**< Service is restarting */
#define LDBCM_NODE_EXISTED                    -145  /**< Node already exists */
#define LDBCM_NODE_NOTEXISTED                 -146  /**< Node does not exist */
#define LDB_LOCK_FAILED                       -147  /**< Unable to lock */
#define LDB_DMS_STATE_NOT_COMPATIBLE          -148  /**< DMS state is not compatible with current command */
#define LDB_REBUILD_HAS_ALREADY_START         -149  /**< Database rebuild is already started */
#define LDB_RTN_IN_REBUILD                    -150  /**< Database rebuild is in progress */
#define LDB_RTN_COORD_CACHE_EMPTY             -151  /**< Cache is empty on coordinator node */
#define LDB_SPT_EVAL_FAIL                     -152  /**< Evalution failed with error */
#define LDB_CAT_GRP_EXIST                     -153  /**< Group already exist */
#define LDB_CLS_GRP_NOT_EXIST                 -154  /**< Group does not exist */
#define LDB_CLS_NODE_NOT_EXIST                -155  /**< Node does not exist */
#define LDB_CM_RUN_NODE_FAILED                -156  /**< Failed to start the node */
#define LDB_CM_CONFIG_CONFLICTS               -157  /**< Invalid node configuration */
#define LDB_CLS_EMPTY_GROUP                   -158  /**< Group is empty */
#define LDB_RTN_COORD_ONLY                    -159  /**< The operation is for coord node only */
#define LDB_CM_OP_NODE_FAILED                 -160  /**< Failed to operate on node */
#define LDB_RTN_MUTEX_JOB_EXIST               -161  /**< The mutex job already exist */
#define LDB_RTN_JOB_NOT_EXIST                 -162  /**< The specified job does not exist */
#define LDB_CAT_CORRUPTION                    -163  /**< The catalog information is corrupted */
#define LDB_IXM_DROP_SHARD                    -164  /**< $shard index can't be dropped */
#define LDB_RTN_CMD_NO_NODE_AUTH              -165  /**< The command can't be run in the node */
#define LDB_RTN_CMD_NO_SERVICE_AUTH           -166  /**< The command can't be run in the service plane */
#define LDB_CLS_NO_GROUP_INFO                 -167  /**< The group info not exist */
#define LDB_CLS_GROUP_NAME_CONFLICT           -168  /**< Group name is conflict */
#define LDB_COLLECTION_NOTSHARD               -169  /**< The collection is not sharded */
#define LDB_INVALID_SHARDINGKEY               -170  /**< The record does not contains valid sharding key */
#define LDB_TASK_EXIST                        -171  /**< A task that already exists does not compatible with the new task */
#define LDB_CL_NOT_EXIST_ON_GROUP             -172  /**< The collection does not exists on the specified group */
#define LDB_CAT_TASK_NOTFOUND                 -173  /**< The specified task does not exist */
#define LDB_MULTI_SHARDING_KEY                -174  /**< The record contains more than one sharding key */
#define LDB_CLS_MUTEX_TASK_EXIST              -175  /**< The mutex task already exist */
#define LDB_CLS_BAD_SPLIT_KEY                 -176  /**< The split key is not valid or not in the source group */
#define LDB_SHARD_KEY_NOT_IN_UNIQUE_KEY       -177  /**< The unique index must include all fields in sharding key */
#define LDB_UPDATE_SHARD_KEY                  -178  /**< Sharding key cannot be updated */
#define LDB_AUTH_AUTHORITY_FORBIDDEN          -179  /**< Authority is forbidden */
#define LDB_CAT_NO_ADDR_LIST                  -180  /**< There is no catalog address specified by user */
#define LDB_CURRENT_RECORD_DELETED            -181  /**< Current record has been removed */
#define LDB_QGM_MATCH_NONE                    -182  /**< No records can be matched for the search condition */
#define LDB_IXM_REORG_DONE                    -183  /**< Index page is reorged and the pos got different lchild */
#define LDB_RTN_DUPLICATE_FIELDNAME           -184  /**< Duplicate field name exists in the record */
#define LDB_QGM_MAX_NUM_RECORD                -185  /**< Too many records to be inserted at once */
#define LDB_QGM_MERGE_JOIN_EQONLY             -186  /**< Sort-Merge Join only supports equal predicates */
#define LDB_PD_TRACE_IS_STARTED               -187  /**< Trace is already started */
#define LDB_PD_TRACE_HAS_NO_BUFFER            -188  /**< Trace buffer does not exist */
#define LDB_PD_TRACE_FILE_INVALID             -189  /**< Trace file is not valid */
#define LDB_DPS_TRANS_LOCK_INCOMPATIBLE       -190  /**< Incompatible lock */
#define LDB_DPS_TRANS_DOING_ROLLBACK          -191  /**< Rollback operation is in progress */
#define LDB_MIG_IMP_BAD_RECORD                -192  /**< Invalid record is found during import */
#define LDB_QGM_REPEAT_VAR_NAME               -193  /**< Repeated variable name */
#define LDB_QGM_AMBIGUOUS_FIELD               -194  /**< Column name is ambiguous */
#define LDB_SQL_SYNTAX_ERROR                  -195  /**< SQL syntax error */
#define LDB_DPS_TRANS_NO_TRANS                -196  /**< Invalid transactional operation */
#define LDB_DPS_TRANS_APPEND_TO_WAIT          -197  /**< Append to lock-wait-queue */
#define LDB_DMS_DELETING                      -198  /**< Record is deleted */
#define LDB_DMS_INVALID_INDEXCB               -199  /**< Index is dropped or invalid */
#define LDB_COORD_RECREATE_CATALOG            -200  /**< Unable to create new catalog when there's already one exists */
#define LDB_UTIL_PARSE_JSON_INVALID           -201  /**< Failed to parse JSON file */
#define LDB_UTIL_PARSE_CSV_INVALID            -202  /**< Failed to parse CSV file */
#define LDB_DPS_LOG_FILE_OUT_OF_SIZE          -203  /**< Log file size is too large */
#define LDB_CATA_RM_NODE_FORBIDDEN            -204  /**< Unable to remove the last node or primary in a group */
#define LDB_CATA_FAILED_TO_CLEANUP            -205  /**< Unable to clean up catalog, manual cleanup may be required */
#define LDB_CATA_RM_CATA_FORBIDDEN            -206  /**< Unable to remove primary catalog or catalog group for non-empty database */
#define LDB_ERROR_RESERVED_2                  -207  /**< Reserved */
#define LDB_CAT_RM_GRP_FORBIDDEN              -208  /**< Unable to remove non-empty group */
#define LDB_MIG_END_OF_QUEUE                  -209  /**< End of queue */
#define LDB_COORD_SPLIT_NO_SHDIDX             -210  /**< Unable to split because of no sharding index exists */
#define LDB_FIELD_NOT_EXIST                   -211  /**< The parameter field does not exist */
#define LDB_TOO_MANY_TRACE_BP                 -212  /**< Too many break points are specified */
#define LDB_BUSY_PREFETCHER                   -213  /**< All prefetchers are busy */
#define LDB_CAT_DOMAIN_NOT_EXIST              -214  /**< Domain does not exist */
#define LDB_CAT_DOMAIN_EXIST                  -215  /**< Domain already exists */
#define LDB_CAT_GROUP_NOT_IN_DOMAIN           -216  /**< Group is not in domain */
#define LDB_CLS_SHARDING_NOT_HASH             -217  /**< Sharding type is not hash */
#define LDB_CLS_SPLIT_PERCENT_LOWER           -218  /**< split percentage is lower then expected */
#define LDB_TASK_ALREADY_FINISHED             -219  /**< Task is already finished */
#define LDB_COLLECTION_LOAD                   -220  /**< Collection is in loading status */
#define LDB_LOAD_ROLLBACK                     -221  /**< Rolling back load operation */
#define LDB_INVALID_ROUTEID                   -222  /**< RouteID is different from the local */
#define LDB_DUPLICATED_SERVICE                -223  /**< Service already exists */
#define LDB_UTIL_NOT_FIND_FIELD               -224  /**< Field is not found */
#define LDB_UTIL_CSV_FIELD_END                -225  /**< csv field line end */
#define LDB_MIG_UNKNOW_FILE_TYPE              -226  /**< Unknown file type */
#define LDB_RTN_EXPORTCONF_NOT_COMPLETE       -227  /**< Exporting configuration does not complete in all nodes */
#define LDB_CLS_NOTP_AND_NODATA               -228  /**< Empty non-primary node */
#define LDB_DMS_SECRETVALUE_NOT_SAME          -229  /**< Secret value for index file does not match with data file */
#define LDB_PMD_VERSION_ONLY                  -230  /**< Engine version argument is specified */
#define LDB_LDB_HELP_ONLY                     -231  /**< Help argument is specified */
#define LDB_LDB_VERSION_ONLY                  -232  /**< Version argument is specified */
#define LDB_FMP_FUNC_NOT_EXIST                -233  /**< Stored procedure does not exist */
#define LDB_ILL_RM_SUB_CL                     -234  /**< Unable to remove collection partition */
#define LDB_RELINK_SUB_CL                     -235  /**< Duplicated attach collection partition */
#define LDB_INVALID_MAIN_CL                   -236  /**< Invalid partitioned-collection */
#define LDB_BOUND_CONFLICT                    -237  /**< New boundary is conflict with the existing boundary */
#define LDB_BOUND_INVALID                     -238  /**< Invalid boundary for the shard */
#define LDB_HIT_HIGH_WATERMARK                -239  /**< Hit the high water mark */
#define LDB_BAR_BACKUP_EXIST                  -240  /**< Backup already exists */
#define LDB_BAR_BACKUP_NOTEXIST               -241  /**< Backup does not exist */
#define LDB_INVALID_SUB_CL                    -242  /**< Invalid collection partition */
#define LDB_TASK_HAS_CANCELED                 -243  /**< Task is canceled */
#define LDB_INVALID_MAIN_CL_TYPE              -244  /**< Sharding type must be ranged partition for partitioned-collection */
#define LDB_NO_SHARDINGKEY                    -245  /**< There is no valid sharding-key defined */
#define LDB_MAIN_CL_OP_ERR                    -246  /**< Operation is not supported on partitioned-collection */
#define LDB_IXM_REDEF                         -247  /**< Redefine index */
#define LDB_DMS_CS_DELETING                   -248  /**< Dropping the collection space is in progress */
#define LDB_DMS_REACHED_MAX_NODES             -249  /**< Hit the limit of maximum number of nodes in the cluster */
#define LDB_CLS_NODE_BSFAULT                  -250  /**< The node is not in normal status */
#define LDB_CLS_NODE_INFO_EXPIRED             -251  /**< Node information is expired */
#define LDB_CLS_WAIT_SYNC_FAILED              -252  /**< Failed to wait for the sync operation from secondary nodes */
#define LDB_DPS_TRANS_DIABLED                 -253  /**< Transaction is disabled */
#define LDB_DRIVER_DS_RUNOUT                  -254  /**< Data source is running out of connection pool */
#define LDB_TOO_MANY_OPEN_FD                  -255  /**< Too many opened file description */
#define LDB_DOMAIN_IS_OCCUPIED                -256  /**< Domain is not empty */
#define LDB_REST_RECV_SIZE                    -257  /**< The data received by REST is larger than the max size */
#define LDB_DRIVER_BSON_ERROR                 -258  /**< Failed to build bson object */
#define LDB_OUT_OF_BOUND                      -259  /**< Arguments are out of bound */
#define LDB_REST_COMMON_UNKNOWN               -260  /**< Unknown REST command */
#define LDB_BUT_FAILED_ON_DATA                -261  /**< Failed to execute command on data node */
#define LDB_CAT_NO_GROUP_IN_DOMAIN            -262  /**< The domain is empty */
#define LDB_OM_PASSWD_CHANGE_SUGGEST          -263  /**< Changing password is required */
#define LDB_COORD_NOT_ALL_DONE                -264  /**< One or more nodes did not complete successfully */
#define LDB_OMA_DIFF_VER_AGT_IS_RUNNING       -265  /**< There is another OM Agent running with different version */
#define LDB_OM_TASK_NOT_EXIST                 -266  /**< Task does not exist */
#define LDB_OM_TASK_ROLLBACK                  -267  /**< Task is rolling back */
#define LDB_LOB_SEQUENCE_NOT_EXIST            -268  /**< LOB sequence does not exist */
#define LDB_LOB_IS_NOT_AVAILABLE              -269  /**< LOB is not useable */
#define LDB_MIG_DATA_NON_UTF                  -270  /**< Data is not in UTF-8 format */
#define LDB_OMA_TASK_FAIL                     -271  /**< Task failed */
#define LDB_LOB_NOT_OPEN                      -272  /**< Lob does not open */
#define LDB_LOB_HAS_OPEN                      -273  /**< Lob has been open */
#define LDBCM_NODE_IS_IN_RESTORING            -274  /**< Node is in restoring */
#define LDB_DMS_CS_NOT_EMPTY                  -275  /**< There are some collections in the collection space */
#define LDB_CAT_LOCALHOST_CONFLICT            -276  /**< 'localhost' and '127.0.0.1' cannot be used mixed with other hostname and IP address */
#define LDB_CAT_NOT_LOCALCONN                 -277  /**< If use 'localhost' and '127.0.0.1' for hostname, coord and catalog must in the same host  */
#define LDB_CAT_IS_NOT_DATAGROUP              -278  /**< The special group is not data group */
#define LDB_RTN_AUTOINDEXID_IS_FALSE          -279  /**< can not update/delete records when $id index does not exist */
#define LDB_CLS_CAN_NOT_STEP_UP               -280  /**< can not step up when primary node exists or LSN is not the biggest */
#define LDB_CAT_IMAGE_ADDR_CONFLICT           -281  /**< Image address is conflict with the self cluster */
#define LDB_CAT_GROUP_HASNOT_IMAGE            -282  /**< The data group does not have image group */
#define LDB_CAT_GROUP_HAS_IMAGE               -283  /**< The data group has image group */
#define LDB_CAT_IMAGE_IS_ENABLED              -284  /**< The image is in enabled status */
#define LDB_CAT_IMAGE_NOT_CONFIG              -285  /**< The cluster's image does not configured */
#define LDB_CAT_DUAL_WRITABLE                 -286  /**< This cluster and image cluster is both writable */
#define LDB_CAT_CLUSTER_IS_READONLY           -287  /**< This cluster is readonly */
#define LDB_RTN_QUERYMODIFY_SORT_NO_IDX       -288  /**< Sorting of 'query and modify' must use index */
#define LDB_RTN_QUERYMODIFY_MULTI_NODES       -289  /**< 'query and modify' can't use skip and limit in multiple nodes or sub-collections */
#define LDB_DIR_NOT_EMPTY                     -290  /**< Given path is not empty */
#define LDB_IXM_EXIST_COVERD_ONE              -291  /**< Exist one index which can cover this scene */
#define LDB_CAT_IMAGE_IS_CONFIGURED           -292  /**< The cluster's image has already configured */
#define LDB_RTN_CMD_IN_LOCAL_MODE             -293  /**< The command is in local mode */
#define LDB_SPT_NOT_SPECIAL_JSON              -294  /**< The object is not a special object in sdb shell */
#define LDB_AUTH_USER_ALREADY_EXIST           -295  /**< The specified user already exist */
#define LDB_DMS_EMPTY_COLLECTION              -296  /**< The collection is empty */
#define LDB_LOB_SEQUENCE_EXISTS               -297  /**< LOB sequence exists */
#define LDB_OM_CLUSTER_NOT_EXIST              -298  /**< cluster do not exist */
#define LDB_OM_BUSINESS_NOT_EXIST             -299  /**< business do not exist */
#define LDB_AUTH_USER_NOT_EXIST               -300  /**< user specified is not exist or password is invalid */
#define LDB_UTIL_COMPRESS_INIT_FAIL           -301  /**< Compression initialization failed */
#define LDB_UTIL_COMPRESS_FAIL                -302  /**< Compression failed */
#define LDB_UTIL_DECOMPRESS_FAIL              -303  /**< Decompression failed */
#define LDB_UTIL_COMPRESS_ABORT               -304  /**< Compression abort */
#define LDB_UTIL_COMPRESS_BUFF_SMALL          -305  /**< Buffer for compression is too small */
#define LDB_UTIL_DECOMPRESS_BUFF_SMALL        -306  /**< Buffer for decompression is too small */
#define LDB_OSS_UP_TO_LIMIT                   -307  /**< Up to the limit */
#define LDB_DS_NOT_ENABLE                     -308  /**< data source is not enabled yet */
#define LDB_DS_NO_REACHABLE_COORD             -309  /**< No reachable coord notes */
#define LDB_RULE_ID_IS_NOT_EXIST              -310  /**< the record which exclusive ruleID is not exist */
#define LDB_STRTGY_TASK_NAME_CONFLICT         -311  /**< Task name conflict */
#define LDB_STRTGY_TASK_NOT_EXISTED           -312  /**< The task is not existed */
#define LDB_DPS_LOG_NOT_ARCHIVED              -313  /**< Replica log is not archived */
#define LDB_DS_NOT_INIT                       -314  /**< Data source has not been initialized */
#define LDB_OPERATION_INCOMPATIBLE            -315  /**< Operation is incompatible with the object */
#define LDB_CAT_CLUSTER_IS_DEACTIVED          -316  /**< This cluster is deactived */
#define LDB_LOB_IS_IN_USE                     -317  /**< LOB is in use */
#define LDB_VALUE_OVERFLOW                    -318  /**< Data operation is overflowed */
#define LDB_LOB_PIECESINFO_OVERFLOW           -319  /**< LOB's pieces info is overflowed */
#define LDB_LOB_LOCK_CONFLICTED               -320  /**< LOB lock is conflicted */
#define LDB_DMS_TRUNCATED                     -321  /**< Collection is truncated */
#define LDB_RTN_CONF_NOT_TAKE_EFFECT          -322  /**< Some configuration changes didn't take effect */
#define LDB_SEQUENCE_EXIST                    -323  /**< Sequence already exists */
#define LDB_SEQUENCE_NOT_EXIST                -324  /**< Sequence not exists */
#define LDB_SEQUENCE_EXCEEDED                 -325  /**< Sequence is exceeded */
#define LDB_DMS_CS_UNIQUEID_CONFLICT          -326  /**< CS uniqueID conflict */
#define LDB_DMS_UNIQUEID_CONFLICT             -327  /**< CL uniqueID conflict */
#define LDB_DMS_CS_REMAIN                     -328  /**< CS uniqueID conflict with remain cs */
#define LDB_DMS_REMAIN                        -329  /**< CL uniqueID conflict with remain cl */
#define LDB_CAT_CS_UNIQUEID_EXCEEDED          -330  /**< CS uniqueID exceeds the maximum */
#define LDB_CAT_CL_UNIQUEID_EXCEEDED          -331  /**< CL uniqueID exceeds the maximum */
#define LDB_AUTOINCREMENT_FIELD_CONFLICT      -332  /**< Autoincrement field conflicts on collection */
#define LDB_AUTOINCREMENT_FIELD_NOT_EXIST     -333  /**< Autoincrement field does not exist on collection */
#define LDB_OPERATION_CONFLICT                -334  /**< Operations conflict */
#define LDB_CAT_GLOBALID_EXCEEDED             -335  /**< GlobalID exceeds the maximum */
#define LDB_DPS_INVALID_LOCK_UPGRADE_REQUEST  -336  /**< Invalid lock upgrade request */
#define LDB_DMS_CS_RENAMING                   -337  /**< Renaming the collection space is in progress */
#define LDB_CLS_DATA_NOT_SYNC                 -338  /**< Data not yet synchronized */
#define LDB_IXM_KEY_NOTNULL                   -339  /**< Any field of index key should exist and cannot be null */
#define LDB_RTN_EXIST_INDOUBT_TRANS           -340  /**< Exists in-doubt transaction */
#define LDB_LOCK_ALREADY_HELD                 -341  /**< Lock has been previously acquired */
#endif /* OSSERR_H_ */

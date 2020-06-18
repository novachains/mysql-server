/*******************************************************************************


   Copyright (C) 2019-2020 PlanetRover Ltd.

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

   Source File Name = msgDef.h

   Descriptive Name = Message Defines Header

   When/how to use: this program may be used on binary and text-formatted
   versions of msg component. This file contains definition for global keywords
   that used in client/server communication.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/
#ifndef MSGDEF_H__
#define MSGDEF_H__

#define SYS_PREFIX                           "SYS"

#define FIELD_NAME_ROLE                      "Role"
#define FIELD_NAME_HOST                      "HostName"
#define FIELD_NAME_SERVICE                   "Service"
#define FIELD_NAME_NODE_NAME                 "NodeName"
#define FIELD_NAME_SERVICE_TYPE              "Type"
#define FIELD_NAME_SERVICE_NAME              "ServiceName"
#define FIELD_NAME_NAME                      "Name"
#define FIELD_NAME_CSNAME                    "CSName"
#define FIELD_NAME_CLNAME                    "CLName"
#define FIELD_NAME_GROUPID                   "GroupID"
#define FIELD_NAME_GROUPNAME                 "GroupName"
#define FIELD_NAME_DOMAIN                    "Domain"
#define FIELD_NAME_NODEID                    "NodeID"
#define FIELD_NAME_INSTANCEID                "InstanceID"
#define FIELD_NAME_IS_PRIMARY                "IsPrimary"
#define FIELD_NAME_CURRENT_LSN               "CurrentLSN"
#define FIELD_NAME_BEGIN_LSN                 "BeginLSN"
#define FIELD_NAME_COMMIT_LSN                "CommittedLSN"
#define FIELD_NAME_COMPLETE_LSN              "CompleteLSN"
#define FIELD_NAME_LOW_TRANS_LSN             "LowTranLSN"
#define FIELD_NAME_LOW_TRANSID_NODEID        "LowTransNodeID"
#define FIELD_NAME_LOW_TRANSID_SN            "LowTransSN"
#define FIELD_NAME_IDX_TREE_LOW_TRAN         "IdxTreeLowTran"
#define FIELD_NAME_LSN_QUE_SIZE              "LSNQueSize"
#define FIELD_NAME_LSN_OFFSET                "Offset"
#define FIELD_NAME_LSN_VERSION               "Version"
#define FIELD_NAME_TRANS_INFO                "TransInfo"
#define FIELD_NAME_TOTAL_COUNT               "TotalCount"
#define FIELD_NAME_SERVICE_STATUS            "ServiceStatus"
#define FIELD_NAME_GROUP                     "Group"
#define FIELD_NAME_GROUPS                    "Groups"
#define FIELD_NAME_VERSION                   "Version"
#define FIELD_NAME_SECRETID                  "SecretID"
#define FIELD_NAME_EDITION                   "Edition"
#define FIELD_NAME_W                         "ReplSize"
#define FIELD_NAME_PRIMARY                   "PrimaryNode"
#define FIELD_NAME_GROUP_STATUS              "Status"
#define FIELD_NAME_DATA_STATUS               "DataStatus"
#define FIELD_NAME_SYNC_CONTROL              "SyncControl"
#define FIELD_NAME_PAGE_SIZE                 "PageSize"
#define FIELD_NAME_MAX_CAPACITY_SIZE         "MaxCapacitySize"
#define FIELD_NAME_MAX_DATA_CAP_SIZE         "MaxDataCapSize"
#define FIELD_NAME_MAX_INDEX_CAP_SIZE        "MaxIndexCapSize"
#define FIELD_NAME_TOTAL_SIZE                "TotalSize"
#define FIELD_NAME_FREE_SIZE                 "FreeSize"
#define FIELD_NAME_TOTAL_DATA_SIZE           "TotalDataSize"
#define FIELD_NAME_TOTAL_IDX_SIZE            "TotalIndexSize"
#define FIELD_NAME_FREE_DATA_SIZE            "FreeDataSize"
#define FIELD_NAME_FREE_IDX_SIZE             "FreeIndexSize"
#define FIELD_NAME_COLLECTION                "Collection"
#define FIELD_NAME_COLLECTIONSPACE           "CollectionSpace"
#define FIELD_NAME_CATALOGINFO               "CataInfo"
#define FIELD_NAME_SHARDINGKEY               "ShardingKey"
#define FIELD_NAME_STRICTDATAMODE            "StrictDataMode"
#define VALUE_NAME_DATABASE                  "database"
#define VALUE_NAME_SESSIONS                  "sessions"
#define VALUE_NAME_SESSIONS_CURRENT          "sessions current"
#define VALUE_NAME_HEALTH                    "health"
#define VALUE_NAME_SVCTASKS                  "svctasks"
#define VALIE_NAME_COLLECTIONS               "collections"
#define VALUE_NAME_ALL                       "all"
#define FIELD_NAME_ISMAINCL                  "IsMainCL"
#define FIELD_NAME_MAINCLNAME                "MainCLName"
#define FIELD_NAME_SUBCLNAME                 "SubCLName"
#define FIELD_NAME_SUBOBJSNUM                "SubObjsNum"
#define FIELD_NAME_SUBOBJSSIZE               "SubObjsSize"
#define FIELD_NAME_ENSURE_SHDINDEX           "EnsureShardingIndex"
#define FIELD_NAME_SHARDTYPE                 "ShardingType"
#define FIELD_NAME_SHARDTYPE_RANGE           "range"
#define FIELD_NAME_SHARDTYPE_HASH            "hash"
#define FIELD_NAME_PARTITION                 "Partition"
#define FIELD_NAME_AUTOINCREMENT             "AutoIncrement"
#define FIELD_NAME_AUTOINC_FIELD             "Field"
#define FIELD_NAME_AUTOINC_SEQ               "SequenceName"
#define FIELD_NAME_AUTOINC_SEQ_ID            "SequenceID"
#define FIELD_NAME_GENERATED                 "Generated"
#define VALUE_NAME_ALWAYS                    "always"
#define VALUE_NAME_STRICT                    "strict"
#define VALUE_NAME_DEFAULT                   "default"
#define FIELD_NAME_CURRENT_VALUE             "CurrentValue"
#define FIELD_NAME_INCREMENT                 "Increment"
#define FIELD_NAME_START_VALUE               "StartValue"
#define FIELD_NAME_MIN_VALUE                 "MinValue"
#define FIELD_NAME_MAX_VALUE                 "MaxValue"
#define FIELD_NAME_CACHE_SIZE                "CacheSize"
#define FIELD_NAME_ACQUIRE_SIZE              "AcquireSize"
#define FIELD_NAME_CYCLED                    "Cycled"
#define FIELD_NAME_INTERNAL                  "Internal"
#define FIELD_NAME_INITIAL                   "Initial"
#define FIELD_NAME_NEXT_VALUE                "NextValue"
#define FIELD_NAME_EXPECT_VALUE              "ExpectValue"
#define FIELD_NAME_MAJOR                     "Major"
#define FIELD_NAME_MINOR                     "Minor"
#define FIELD_NAME_FIX                       "Fix"
#define FIELD_NAME_RELEASE                   "Release"
#define FIELD_NAME_GITVERSION                "GitVersion"
#define FIELD_NAME_BUILD                     "Build"
#define FIELD_NAME_SESSIONID                 "SessionID"
#define FIELD_NAME_TID                       "TID"
#define FIELD_NAME_CONTEXTS                  "Contexts"
#define FIELD_NAME_CONTEXTID                 "ContextID"
#define FIELD_NAME_ACCESSPLAN_ID             "AccessPlanID"
#define FIELD_NAME_DATAREAD                  "DataRead"
#define FIELD_NAME_DATAWRITE                 "DataWrite"
#define FIELD_NAME_INDEXREAD                 "IndexRead"
#define FIELD_NAME_INDEXWRITE                "IndexWrite"
#define FIELD_NAME_QUERYTIMESPENT            "QueryTimeSpent"
#define FIELD_NAME_NODEWAITTIME              "RemoteNodeWaitTime"
#define FIELD_NAME_STARTTIMESTAMP            "StartTimestamp"
#define FIELD_NAME_ENDTIMESTAMP              "EndTimestamp"
#define FIELD_NAME_TOTALNUMCONNECTS          "TotalNumConnects"
#define FIELD_NAME_TOTALDATAREAD             "TotalDataRead"
#define FIELD_NAME_TOTALINDEXREAD            "TotalIndexRead"
#define FIELD_NAME_TOTALDATAWRITE            "TotalDataWrite"
#define FIELD_NAME_TOTALINDEXWRITE           "TotalIndexWrite"
#define FIELD_NAME_TOTALUPDATE               "TotalUpdate"
#define FIELD_NAME_TOTALDELETE               "TotalDelete"
#define FIELD_NAME_TOTALINSERT               "TotalInsert"
#define FIELD_NAME_TOTALSELECT               "TotalSelect"
#define FIELD_NAME_TOTALREAD                 "TotalRead"
#define FIELD_NAME_TOTALWRITE                "TotalWrite"
#define FIELD_NAME_TOTALTBSCAN               "TotalTbScan"
#define FIELD_NAME_TOTALIXSCAN               "TotalIxScan"
#define FIELD_NAME_TOTALREADTIME             "TotalReadTime"
#define FIELD_NAME_TOTALWRITETIME            "TotalWriteTime"
#define FIELD_NAME_TOTALTIME                 "Time"
#define FIELD_NAME_TOTALCONTEXTS             "TotalContexts"
#define FIELD_NAME_READTIMESPENT             "ReadTimeSpent"
#define FIELD_NAME_WRITETIMESPENT            "WriteTimeSpent"
#define FIELD_NAME_LASTOPBEGIN               "LastOpBegin"
#define FIELD_NAME_LASTOPEND                 "LastOpEnd"
#define FIELD_NAME_LASTOPTYPE                "LastOpType"
#define FIELD_NAME_LASTOPINFO                "LastOpInfo"
#define FIELD_NAME_OPTYPE                    "OpType"
#define FIELD_NAME_TOTALMAPPED               "TotalMapped"
#define FIELD_NAME_REPLINSERT                "ReplInsert"
#define FIELD_NAME_REPLUPDATE                "ReplUpdate"
#define FIELD_NAME_REPLDELETE                "ReplDelete"
#define FIELD_NAME_ACTIVETIMESTAMP           "ActivateTimestamp"
#define FIELD_NAME_RESETTIMESTAMP            "ResetTimestamp"
#define FIELD_NAME_USERCPU                   "UserCPU"
#define FIELD_NAME_SYSCPU                    "SysCPU"
#define FIELD_NAME_CONNECTTIMESTAMP          "ConnectTimestamp"
#define FIELD_NAME_USER                      "User"
#define FIELD_NAME_SYS                       "Sys"
#define FIELD_NAME_IDLE                      "Idle"
#define FIELD_NAME_OTHER                     "Other"
#define FIELD_NAME_CPU                       "CPU"
#define FIELD_NAME_LOADPERCENT               "LoadPercent"
#define FIELD_NAME_TOTALRAM                  "TotalRAM"
#define FIELD_NAME_FREERAM                   "FreeRAM"
#define FIELD_NAME_TOTALSWAP                 "TotalSwap"
#define FIELD_NAME_FREESWAP                  "FreeSwap"
#define FIELD_NAME_TOTALVIRTUAL              "TotalVirtual"
#define FIELD_NAME_FREEVIRTUAL               "FreeVirtual"
#define FIELD_NAME_MEMORY                    "Memory"
#define FIELD_NAME_RSSSIZE                   "RssSize"
#define FIELD_NAME_LOADPERCENTVM             "LoadPercentVM"
#define FIELD_NAME_VMLIMIT                   "VMLimit"
#define FIELD_NAME_VMSIZE                    "VMSize"
#define FIELD_NAME_CORESZ                    "CoreFileSize"
#define FIELD_NAME_VM                        "VirtualMemory"
#define FIELD_NAME_OPENFL                    "OpenFiles"
#define FIELD_NAME_NPROC                     "NumProc"
#define FIELD_NAME_FILESZ                    "FileSize"
#define FIELD_NAME_ULIMIT                    "Ulimit"
#define FIELD_NAME_OOM                       "LDB_OOM"
#define FIELD_NAME_NOSPC                     "LDB_NOSPC"
#define FIELD_NAME_TOOMANY_OF                "LDB_TOO_MANY_OPEN_FD"
#define FIELD_NAME_ERRNUM                    "ErrNum"
#define FIELD_NAME_TOTALNUM                  "TotalNum"
#define FIELD_NAME_FREENUM                   "FreeNum"
#define FIELD_NAME_FILEDESP                  "FileDesp"
#define FIELD_NAME_ABNORMALHST               "AbnormalHistory"
#define FIELD_NAME_STARTHST                  "StartHistory"
#define FIELD_NAME_DIFFLSNPRIMARY            "DiffLSNWithPrimary"
#define FIELD_NAME_DATABASEPATH              "DatabasePath"
#define FIELD_NAME_TOTALSPACE                "TotalSpace"
#define FIELD_NAME_FREESPACE                 "FreeSpace"
#define FIELD_NAME_DISK                      "Disk"
#define FIELD_NAME_CURRENTACTIVESESSIONS     "CurrentActiveSessions"
#define FIELD_NAME_CURRENTIDLESESSIONS       "CurrentIdleSessions"
#define FIELD_NAME_CURRENTSYSTEMSESSIONS     "CurrentSystemSessions"
#define FIELD_NAME_CURRENTTASKSESSIONS       "CurrentTaskSessions"
#define FIELD_NAME_CURRENTCONTEXTS           "CurrentContexts"
#define FIELD_NAME_SESSIONS                  "Sessions"
#define FIELD_NAME_STATUS                    "Status"
#define FIELD_NAME_NUM_MSG_SENT              "TotalMsgSent"
#define FIELD_NAME_DICT_CREATED              "DictionaryCreated"
#define FIELD_NAME_DICT_VERSION              "DictionaryVersion"
#define FIELD_NAME_DICT_CREATE_TIME          "DictionaryCreateTime"
#define FIELD_NAME_TYPE                      "Type"
#define FIELD_NAME_EXT_DATA_NAME             "ExtDataName"
#define FIELD_NAME_TOTAL_RECORDS             "TotalRecords"
#define FIELD_NAME_TOTAL_DATA_PAGES          "TotalDataPages"
#define FIELD_NAME_TOTAL_INDEX_PAGES         "TotalIndexPages"
#define FIELD_NAME_TOTAL_DATA_FREESPACE      "TotalDataFreeSpace"
#define FIELD_NAME_TOTAL_INDEX_FREESPACE     "TotalIndexFreeSpace"
#define FIELD_NAME_EDUNAME                   "Name"
#define FIELD_NAME_QUEUE_SIZE                "QueueSize"
#define FIELD_NAME_PROCESS_EVENT_COUNT       "ProcessEventCount"
#define FIELD_NAME_RELATED_ID                "RelatedID"
#define FIELD_NAME_RELATED_NODE              "RelatedNode"
#define FIELD_NAME_RELATED_NID               "RelatedNID"
#define FIELD_NAME_RELATED_TID               "RelatedTID"
#define FIELD_NAME_ID                        "ID"
#define FIELD_NAME_UNIQUEID                  "UniqueID"
#define FIELD_NAME_LOGICAL_ID                "LogicalID"
#define FIELD_NAME_SEQUENCE                  "Sequence"
#define FIELD_NAME_INDEXES                   "Indexes"
#define FIELD_NAME_DETAILS                   "Details"
#define FIELD_NAME_NUMCOLLECTIONS            "NumCollections"
#define FIELD_NAME_COLLECTIONHWM             "CollectionHWM"
#define FIELD_NAME_SIZE                      "Size"
#define FIELD_NAME_MAX                       "Max"
#define FIELD_NAME_TRACE                     "trace"
#define FIELD_NAME_TO                        "To"
#define FIELD_NAME_OLDNAME                   "OldName"
#define FIELD_NAME_NEWNAME                   "NewName"
#define FIELD_NAME_INDEX                     "Index"
#define FIELD_NAME_TOTAL                     "Total"
#define FIELD_NAME_ERROR_NO                  "ErrNo"
#define FIELD_NAME_LOWBOUND                  "LowBound"
#define FIELD_NAME_UPBOUND                   "UpBound"
#define FIELD_NAME_SOURCE                    "Source"
#define FIELD_NAME_TARGET                    "Target"
#define FIELD_NAME_SPLITQUERY                "SplitQuery"
#define FIELD_NAME_SPLITENDQUERY             "SplitEndQuery"
#define FIELD_NAME_SPLITPERCENT              "SplitPercent"
#define FIELD_NAME_SPLITVALUE                "SplitValue"
#define FIELD_NAME_SPLITENDVALUE             "SplitEndValue"
#define FIELD_NAME_RECEIVECOUNT              "ReceivedEvents"
#define FIELD_NAME_TASKTYPE                  "TaskType"
#define FIELD_NAME_TASKID                    "TaskID"
#define FIELD_NAME_RULEID                    "RuleID"
#define FIELD_NAME_SOURCEID                  "SourceID"
#define FIELD_NAME_TARGETID                  "TargetID"
#define FIELD_NAME_ASYNC                     "Async"
#define FIELD_NAME_OPTIONS                   "Options"
#define FIELD_NAME_CONDITION                 "Condition"
#define FIELD_NAME_RULE                      "Rule"
#define FIELD_NAME_SORT                      "Sort"
#define FIELD_NAME_HINT                      "Hint"
#define FIELD_NAME_SELECTOR                  "Selector"
#define FIELD_NAME_SKIP                      "Skip"
#define FIELD_NAME_RETURN                    "Return"
#define FIELD_NAME_COMPONENTS                "Components"
#define FIELD_NAME_BREAKPOINTS               "BreakPoint"
#define FIELD_NAME_THREADS                   "Threads"
#define FIELD_NAME_FUNCTIONNAMES             "FunctionNames"
#define FIELD_NAME_THREADTYPES               "ThreadTypes"
#define FIELD_NAME_FILENAME                  "FileName"
#define FIELD_NAME_TRACESTARTED              "TraceStarted"
#define FIELD_NAME_WRAPPED                   "Wrapped"
#define FIELD_NAME_MASK                      "Mask"
#define FIELD_NAME_AGGR                      "Aggr"
#define FIELD_NAME_CMD                       "CMD"
#define FIELD_NAME_DATABLOCKS                "Datablocks"
#define FIELD_NAME_SCANTYPE                  "ScanType"
#define VALUE_NAME_TBSCAN                    "tbscan"
#define VALUE_NAME_IXSCAN                    "ixscan"
#define FIELD_NAME_INDEXNAME                 "IndexName"
#define FIELD_NAME_INDEXLID                  "IndexLID"
#define FIELD_NAME_DIRECTION                 "Direction"
#define FIELD_NAME_INDEXBLOCKS               "Indexblocks"
#define FIELD_NAME_STARTKEY                  "StartKey"
#define FIELD_NAME_ENDKEY                    "EndKey"
#define FIELD_NAME_STARTRID                  "StartRID"
#define FIELD_NAME_ENDRID                    "EndRID"
#define FIELD_NAME_META                      "$Meta"
#define FIELD_NAME_SET_ON_INSERT             "$SetOnInsert"
#define FIELD_NAME_PATH                      "Path"
#define FIELD_NAME_DESP                      "Description"
#define FIELD_NAME_ENSURE_INC                "EnsureInc"
#define FIELD_NAME_OVERWRITE                 "OverWrite"
#define FIELD_NAME_DETAIL                    "Detail"
#define FIELD_NAME_ESTIMATE                  "Estimate"
#define FIELD_NAME_SEARCH                    "Search"
#define FIELD_NAME_EVALUATE                  "Evaluate"
#define FIELD_NAME_EXPAND                    "Expand"
#define FIELD_NAME_LOCATION                  "Location"
#define FIELD_NAME_FLATTEN                   "Flatten"
#define FIELD_NAME_ISSUBDIR                  "IsSubDir"
#define FIELD_NAME_ENABLE_DATEDIR            "EnableDateDir"
#define FIELD_NAME_PREFIX                    "Prefix"
#define FIELD_NAME_MAX_DATAFILE_SIZE         "MaxDataFileSize"
#define FIELD_NAME_BACKUP_LOG                "BackupLog"
#define FIELD_NAME_USE_EXT_SORT              "UseExtSort"
#define FIELD_NAME_SUB_COLLECTIONS           "SubCollections"
#define FIELD_NAME_ELAPSED_TIME              "ElapsedTime"
#define FIELD_NAME_RETURN_NUM                "ReturnNum"
#define FIELD_NAME_RUN                       "Run"
#define FIELD_NAME_WAIT                      "Wait"
#define FIELD_NAME_CLUSTERNAME               "ClusterName"
#define FIELD_NAME_BUSINESSNAME              "BusinessName"
#define FIELD_NAME_DATACENTER                "DataCenter"
#define FIELD_NAME_ADDRESS                   "Address"
#define FIELD_NAME_IMAGE                     "Image"
#define FIELD_NAME_ACTIVATED                 "Activated"
#define FIELD_NAME_READONLY                  "Readonly"
#define FIELD_NAME_CSUNIQUEHWM               "CSUniqueHWM"
#define FIELD_NAME_ENABLE                    "Enable"
#define FIELD_NAME_ACTION                    "Action"
#define FIELD_NAME_DATA                      "Data"
#define FIELD_NAME_DATALEN                   "DataLen"
#define FIELD_NAME_ORG_LSNOFFSET             "OrgOffset"
#define FIELD_NAME_TRANSACTION_ID            "TransactionID"
#define FIELD_NAME_TRANSACTION_ID_NODEID     "TransactionIDNodeID"
#define FIELD_NAME_TRANSACTION_ID_SN         "TransactionIDSN"
#define FIELD_NAME_TRANS_LSN_CUR             "CurrentTransLSN"
#define FIELD_NAME_TRANS_LSN_BEGIN           "BeginTransLSN"
#define FIELD_NAME_TRANS_BEGIN_TIME          "TransBeginTime"
#define FIELD_NAME_IS_ROLLBACK               "IsRollback"
#define FIELD_NAME_TRANS_LOCKS_NUM           "TransactionLocksNum"
#define FIELD_NAME_TRANS_LOCKS               "GotLocks"
#define FIELD_NAME_TRANS_WAIT_LOCK           "WaitLock"
#define FIELD_NAME_SLICE                     "Slice"
#define FIELD_NAME_REMOTE_IP                 "RemoteIP"
#define FIELD_NAME_REMOTE_PORT               "RemotePort"
#define FIELD_NAME_MODE                      "Mode"
#define VALUE_NAME_LOCAL                     "local"

#define FIELD_NAME_MODIFY                    "$Modify"
#define FIELD_NAME_OP                        "OP"
#define FIELD_NAME_OP_UPDATE                 "Update"
#define FIELD_NAME_OP_REMOVE                 "Remove"
#define FIELD_NAME_RETURNNEW                 "ReturnNew"
#define FIELD_NAME_KEEP_SHARDING_KEY         "KeepShardingKey"

#define FIELD_NAME_INSERT                    "Insert"
#define FIELD_NAME_UPDATE                    "Update"
#define FIELD_NAME_DELETE                    "Delete"
#define FIELD_NAME_NLJOIN                    "NLJoin"
#define FIELD_NAME_HASHJOIN                  "HashJoin"
#define FIELD_NAME_SCAN                      "Scan"
#define FIELD_NAME_FILTER                    "Filter"
#define FIELD_NAME_SPLITBY                   "SPLITBY"
#define FIELD_NAME_MAX_GTID                  "MaxGlobTransID"
#define FIELD_NAME_DATA_COMMIT_LSN           "DataCommitLSN"
#define FIELD_NAME_IDX_COMMIT_LSN            "IndexCommitLSN"
#define FIELD_NAME_DATA_COMMITTED            "DataCommitted"
#define FIELD_NAME_IDX_COMMITTED             "IndexCommitted"
#define FIELD_NAME_DIRTY_PAGE                "DirtyPage"

#define FIELD_NAME_PDLEVEL                   "PDLevel"
#define FIELD_NAME_ASYNCHRONOUS              "asynchronous"
#define FIELD_NAME_THREADNUM                 "threadNum"
#define FIELD_NAME_BUCKETNUM                 "bucketNum"
#define FIELD_NAME_PARSEBUFFERSIZE           "parseBufferSize"
#define FIELD_NAME_ATTRIBUTE                 "Attribute"
#define FIELD_NAME_ATTRIBUTE_DESC            "AttributeDesc"
#define FIELD_NAME_RCFLAG                    "Flag"
#define FIELD_NAME_GROUPBY_ID                "_id"
#define FIELD_NAME_FIELDS                    "fields"
#define FIELD_NAME_HEADERLINE                "headerline"
#define FIELD_NAME_LTYPE                     "type"
#define FIELD_NAME_charACTER                 "character"
#define FIELD_NAME_GLOBAL                    "Global"
#define FIELD_NAME_ERROR_NODES               "ErrNodes"
#define FIELD_NAME_ERROR_IINFO               "ErrInfo"
#define FIELD_NAME_FUNC                      "func"
#define FIELD_NAME_FUNCTYPE                  "funcType"
#define FIELD_NAME_PREFERED_INSTANCE         "PreferedInstance"
#define FIELD_NAME_PREFERED_INSTANCE_V1      "PreferedInstanceV1"
#define FIELD_NAME_PREFERED_INSTANCE_MODE    "PreferedInstanceMode"
#define FIELD_NAME_PREFERED_STRICT           "PreferedStrict"
#define FIELD_NAME_TIMEOUT                   "Timeout"
#define FIELD_NAME_NODE_SELECT               "NodeSelect"
#define FIELD_NAME_RAWDATA                   "RawData"
#define FIELD_NAME_SYS_AGGR                  "$Aggr"

#define FIELD_NAME_FREELOGSPACE              "freeLogSpace"
#define FIELD_NAME_VSIZE                     "vsize"
#define FIELD_NAME_RSS                       "rss"
#define FIELD_NAME_FAULT                     "fault"
#define FIELD_NAME_SVC_NETIN                 "svcNetIn"
#define FIELD_NAME_SVC_NETOUT                "svcNetOut"
#define FIELD_NAME_REPL_NETIN                "replNetIn"
#define FIELD_NAME_REPL_NETOUT               "replNetOut"
#define FIELD_NAME_SHARD_NETIN               "shardNetIn"
#define FIELD_NAME_SHARD_NETOUT              "shardNetOut"
#define FIELD_NAME_DOMAIN_AUTO_SPLIT         "AutoSplit"
#define FIELD_NAME_DOMAIN_AUTO_REBALANCE     "AutoRebalance"
#define FIELD_NAME_AUTO_INDEX_ID             "AutoIndexId"
#define FIELD_NAME_REELECTION_TIMEOUT        "Seconds"
#define FIELD_NAME_REELECTION_LEVEL          "Level"
#define FIELD_NAME_FORCE_STEP_UP_TIME        FIELD_NAME_REELECTION_TIMEOUT
#define FIELD_NAME_INTERNAL_VERSION          "InternalV"
#define FIELD_NAME_RTYPE                     "ReturnType"
#define FIELD_NAME_IX_BOUND                  "IXBound"
#define FIELD_NAME_QUERY                     "Query"
#define FIELD_NAME_NEED_MATCH                "NeedMatch"
#define FIELD_NAME_RTYPE                     "ReturnType"
#define FIELD_NAME_ONLY_DETACH               "OnlyDetach"
#define FIELD_NAME_ONLY_ATTACH               "OnlyAttach"
#define FIELD_NAME_ALTER_TYPE                "AlterType"
#define FIELD_NAME_ARGS                      "Args"
#define FIELD_NAME_ALTER                     "Alter"
#define FIELD_NAME_IGNORE_EXCEPTION          "IgnoreException"
#define FIELD_NAME_KEEP_DATA                 "KeepData"
#define FIELD_NAME_ENFORCED                  "enforced"
#define FIELD_NAME_ENFORCED1                 "Enforced"
#define FIELD_NAME_DEEP                      "Deep"
#define FIELD_NAME_BLOCK                     "Block"
#define FIELD_NAME_CAPPED                    "Capped"
#define FIELD_NAME_TEXT                      "$Text"
#define FIELD_NAME_CONFIGS                   "Configs"
#define FIELD_NAME_SEQUENCE_NAME             "Name"
#define FIELD_NAME_SEQUENCE_OID              "_id"
#define FIELD_NAME_SEQUENCE_ID               "ID"
#define FIELD_NAME_CONTONDUP                 "ContOnDup"
#define FIELD_NAME_REPLACEONDUP              "ReplaceOnDup"
#define FIELD_NAME_ROLLBACK                  "Rollback"
#define FIELD_NAME_TRANSISOLATION            "TransIsolation"
#define FIELD_NAME_TRANS_TIMEOUT             "TransTimeout"
#define FIELD_NAME_TRANS_WAITLOCK            "TransLockWait"
#define FIELD_NAME_TRANS_WAITLOCKTIME        "TransLockWaitTime"
#define FIELD_NAME_TRANS_USE_RBS             "TransUseRBS"
#define FIELD_NAME_TRANS_AUTOCOMMIT          "TransAutoCommit"
#define FIELD_NAME_TRANS_AUTOROLLBACK        "TransAutoRollback"
#define FIELD_NAME_TRANS_RCCOUNT             "TransRCCount"
#define FIELD_NAME_TRANS_GLOBTRANSON         "GlobTransOn"
#define FIELD_NAME_LAST_GENERATE_ID          "LastGenerateID"
#define FIELD_NAME_INSERT_NUM                "InsertedNum"
#define FIELD_NAME_DUPLICATE_NUM             "DuplicatedNum"
#define FIELD_NAME_UPDATE_NUM                "UpdatedNum"
#define FIELD_NAME_MODIFIED_NUM              "ModifiedNum"
#define FIELD_NAME_DELETE_NUM                "DeletedNum"
#define FIELD_NAME_MEMPOOL_SIZE              "MemPoolSize"
#define FIELD_NAME_CUR_RBS_CL                "CurRBSCL"
#define FIELD_NAME_LAST_FREE_RBS_CL          "LastFreeRBSCL"
#define FIELD_NAME_NUM_ACTIVE_RBS_GC         "NumActiveRBSGC"
#define FIELD_NAME_RBS_RECORD_KEY            "RBSRECORDKEY"
#define FIELD_NAME_RBS_RECORD_DATA           "RBSRECORDDATA"
#define FIELD_NAME_RBS_RECORD_TRANSID        "RBSRECORDTRANSID"
#define FIELD_NAME_RBS_RECORD_LSN_OFFSET     "RBSRECORDLSNOFFSET"
#define FIELD_NAME_RBS_PRERECORD_CL          "RBSPRERECORDCL"
#define FIELD_NAME_RBS_PRERECORD_OFFSET      "RBSPRERECORDOFFSET"
#define FIELD_NAME_RBS_HASH_BKT              "RBSHASHBKT"
#define FIELD_NAME_LATCH_WAIT_TIME           "LatchWaitTime"
#define FIELD_NAME_MSG_SENT_TIME             "MsgSentTime"
#define FIELD_NAME_XOWNER_TID                "XOwnerTID"
#define FIELD_NAME_LAST_S_OWNER              "LastSOwner"
#define FIELD_NAME_NUM_OWNER                 "NumOwner"
#define FIELD_NAME_LATCH_NAME                "LatchName"
#define FIELD_NAME_LATCH_DESC                "LatchDesc"
#define FIELD_NAME_VIEW_HISTORY              "ViewHistory"
#define FIELD_NAME_INDEXVALUE                "IndexValue"
#define FIELD_NAME_CURRENTID                 "CurrentID"
#define FIELD_NAME_PEERID                    "PeerID"
#define FIELD_NAME_CURRENT_FIELD             "CurrentField"

/// strategy field begin
#define FIELD_NAME_NICE                      "Nice"
#define FIELD_NAME_TASK_NAME                 "TaskName"
#define FIELD_NAME_CONTAINER_NAME            "ContainerName"
#define FIELD_NAME_IP                        "IP"
#define FIELD_NAME_TASK_ID                   "TaskID"
#define FIELD_NAME_SCHDLR_TYPE               "SchdlrType"
#define FIELD_NAME_SCHDLR_TYPE_DESP          "SchdlrTypeDesp"
#define FIELD_NAME_SCHDLR_TIMES              "SchdlrTimes"
#define FIELD_NAME_SCHDLR_MGR_EVT_NUM        "SchdlrMgrEvtNum"
/// strategy field end

#define FIELD_NAME_ANALYZE_MODE              "Mode"
#define FIELD_NAME_ANALYZE_NUM               "SampleNum"
#define FIELD_NAME_ANALYZE_PERCENT           "SamplePercent"

#define FIELD_OP_VALUE_UPDATE                "update"
#define FIELD_OP_VALUE_REMOVE                "remove"

#define FIELD_OP_VALUE_KEEP                  "keep"
#define FIELD_OP_VALUE_REPLACE               "replace"

// For parameters
// Used internal: { $param : paramIndex, $ctype : canonicalType }
#define FIELD_NAME_PARAM                     "$param"
#define FIELD_NAME_CTYPE                     "$ctype"
#define FIELD_NAME_PARAMETERS                "Parameters"

#define IXM_FIELD_NAME_KEY                   "key"
#define IXM_FIELD_NAME_NAME                  "name"
#define IXM_FIELD_NAME_UNIQUE                "unique"
#define IXM_FIELD_NAME_UNIQUE1               "Unique"
#define IXM_FIELD_NAME_V                     "v"
#define IXM_FIELD_NAME_ENFORCED              "enforced"
#define IXM_FIELD_NAME_ENFORCED1             "Enforced"
#define IXM_FIELD_NAME_DROPDUPS              "dropDups"
#define IXM_FIELD_NAME_2DRANGE               "2drange"
#define IXM_FIELD_NAME_INDEX_DEF             "IndexDef"
#define IXM_FIELD_NAME_INDEX_FLAG            "IndexFlag"
#define IXM_FIELD_NAME_SCAN_EXTLID           "ScanExtentLID"
#define IXM_FIELD_NAME_SORT_BUFFER_SIZE      "SortBufferSize"
#define IXM_FIELD_NAME_NOTNULL               "NotNull"


#define CMD_ADMIN_PREFIX                     "$"

#define SYS_VIRTUAL_CS                       "SYS_VCS"
#define SYS_VIRTUAL_CS_LEN                   sizeof( SYS_VIRTUAL_CS )
#define SYS_CL_SESSION_INFO                  SYS_VIRTUAL_CS".SYS_SESSION_INFO"

#define CMD_VALUE_NAME_CREATE                "create image"
#define CMD_VALUE_NAME_REMOVE                "remove image"
#define CMD_VALUE_NAME_ATTACH                "attach groups"
#define CMD_VALUE_NAME_DETACH                "detach groups"
#define CMD_VALUE_NAME_ENABLE                "enable image"
#define CMD_VALUE_NAME_DISABLE               "disable image"
#define CMD_VALUE_NAME_ACTIVATE              "activate"
#define CMD_VALUE_NAME_DEACTIVATE            "deactivate"
#define CMD_VALUE_NAME_ENABLE_READONLY       "enable readonly"
#define CMD_VALUE_NAME_DISABLE_READONLY      "disable readonly"

/*
   alter user
*/
#define CMD_VALUE_NAME_CHANGEPASSWD          "change passwd"
#define CMD_VALUE_NAME_SETATTR               "set attributes"


#define CLS_REPLSET_MAX_NODE_SIZE            7
#define LDB_MAX_MSG_LENGTH                   ( 512 * 1024 * 1024 )

#define LDB_MAX_USERNAME_LENGTH              256
#define LDB_MAX_PASSWORD_LENGTH              256

#define INVALID_GROUPID                      0
#define CATALOG_GROUPID                      1
#define COORD_GROUPID                        2
#define OM_GROUPID                           3
#define OMAGENT_GROUPID                      4
#define SPARE_GROUPID                        5
#define CATALOG_GROUPNAME                    SYS_PREFIX"CatalogGroup"
#define COORD_GROUPNAME                      SYS_PREFIX"Coord"
#define SPARE_GROUPNAME                      SYS_PREFIX"Spare"
#define OM_GROUPNAME                         SYS_PREFIX"OM"
#define NODE_NAME_SERVICE_SEP                ":"
#define NODE_NAME_SERVICE_SEPCHAR            (((char*)NODE_NAME_SERVICE_SEP)[0])
#define INVALID_NODEID                       0
#define CURRENT_NODEID                       -1
#define SYS_NODE_ID_BEGIN                    1
#define SYS_NODE_ID_END                      ( OM_NODE_ID_BEGIN - 1 )
#define OM_NODE_ID_BEGIN                     800
#define OM_NODE_ID_END                       ( RESERVED_NODE_ID_BEGIN - 1 )
#define RESERVED_NODE_ID_BEGIN               810
#define RESERVED_NODE_ID_END                 ( DATA_NODE_ID_BEGIN - 1 )
#define DATA_NODE_ID_BEGIN                   1000
#define DATA_NODE_ID_END                     ( 60000 + DATA_NODE_ID_BEGIN )
#define DATA_GROUP_ID_BEGIN                  1000
#define DATA_GROUP_ID_END                    ( 60000 + DATA_GROUP_ID_BEGIN )
#define CATA_NODE_MAX_NUM                    CLS_REPLSET_MAX_NODE_SIZE

#define LDB_INDEX_SORT_BUFFER_DEFAULT_SIZE   64

#define LDB_ROLE_DATA_STR                    "data"
#define LDB_ROLE_COORD_STR                   "coord"
#define LDB_ROLE_CATALOG_STR                 "catalog"
#define LDB_ROLE_STANDALONE_STR              "standalone"
#define LDB_ROLE_OM_STR                      "om"
#define LDB_ROLE_OMA_STR                     "cm"
#define LDB_ROLE_STP_STR                     "stp"

#define LDB_AUTH_USER                        "User"
#define LDB_AUTH_PASSWD                      "Passwd"
#define LDB_AUTH_OLDPASSWD                   "OldPasswd"
#define LDB_AUTH_SOURCE                      "Source"

#define LDB_LOB_OID_LEN                      16

#define LDB_SHARDING_PARTITION_DEFAULT    4096       // 2^12
#define LDB_SHARDING_PARTITION_MIN        8          // 2^3
#define LDB_SHARDING_PARTITION_MAX        1048576    // 2^20

enum LDB_ROLE
{
   LDB_ROLE_DATA = 0,
   LDB_ROLE_COORD,
   LDB_ROLE_CATALOG,
   LDB_ROLE_STANDALONE,
   LDB_ROLE_MAX
} ;

enum LDB_LOB_MODE
{
   LDB_LOB_MODE_CREATEONLY = 0x00000001,
   LDB_LOB_MODE_READ       = 0x00000004,
   LDB_LOB_MODE_WRITE      = 0x00000008,
   LDB_LOB_MODE_REMOVE     = 0x00000010,
   LDB_LOB_MODE_TRUNCATE   = 0x00000020,
   LDB_LOB_MODE_SHAREREAD  = 0x00000040,
} ;

#define LDB_ALTER_VERSION 1

/// alter collection
#define CMD_NAME_ALTER_COLLECTION      "alter collection"

/// alter collection space
#define CMD_NAME_ALTER_COLLECTION_SPACE   "alter collectionspace"

/// set attributes
#define LDB_ALTER_CS_SET_ATTR             LDB_ALTER_ACTION_SET_ATTR

/// add groups
#define LDB_ALTER_DOMAIN_ADD_GROUPS    LDB_ALTER_ACTION_ADD \
                                       LDB_ALTER_DELIMITER \
                                       LDB_CATALOG_DOMAIN_GROUPS

/// set groups
#define LDB_ALTER_DOMAIN_SET_GROUPS    LDB_ALTER_ACTION_SET \
                                       LDB_ALTER_DELIMITER \
                                       LDB_CATALOG_DOMAIN_GROUPS

/// remove groups
#define LDB_ALTER_DOMAIN_REMOVE_GROUPS LDB_ALTER_ACTION_REMOVE \
                                       LDB_ALTER_DELIMITER \
                                       LDB_CATALOG_DOMAIN_GROUPS

/// set attributes
#define LDB_ALTER_DOMAIN_SET_ATTR      LDB_ALTER_ACTION_SET_ATTR

enum LDB_COMMAND_TYPE
{
   CMD_CREATE_COLLECTION                  = 10,
   CMD_CREATE_COLLECTIONSPACE             = 11,
   CMD_CREATE_INDEX                       = 12,
   CMD_ALTER_COLLECTION                   = 13,
   CMD_ALTER_COLLECTIONSPACE              = 14,

   CMD_DROP_COLLECTION                    = 20,
   CMD_DROP_COLLECTIONSPACE               = 21,
   CMD_DROP_INDEX                         = 22,

   CMD_LOAD_COLLECTIONSPACE               = 25,
   CMD_UNLOAD_COLLECTIONSPACE             = 26,


   CMD_SET_PDLEVEL                        = 120,
   CMD_SET_SESSIONATTR                    = 121,
   CMD_GET_SESSIONATTR                    = 122,

   CMD_GET_TRANS_HISTORY                  = 132,
   CMD_GET_HISTORY                        = 133,
   CMD_GET_CONFIG                         = 134,
   CMD_GET_DIGEST                         = 135,
   CMD_GET_ENTRY                          = 136,
   CMD_VERIFY_JOURNAL                     = 137,
   CMD_VERIFY_DOCUMENT                    = 138,
   CMD_VERIFY_ENTRY                       = 139
};

#endif // MSGDEF_H__

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

#ifndef NETWORK_H__
#define NETWORK_H__

#include "oss/core.h"
LDB_EXTERN_C_START

typedef struct Socket Socket ;

int32_t clientConnect ( const char *pHostName,
                      const char *pServiceName,
                      int useSSL,
                      Socket** sock ) ;

void clientDisconnect ( Socket** sock ) ;

// timeout for microsecond (1/1000000 sec )
int32_t clientSend ( Socket* sock, const char *pMsg, int32_t len,
                   int32_t *pSentLen, int32_t timeout ) ;
// timeout for microsecond ( 1/1000000 sec )
int32_t clientRecv ( Socket* sock, char *pMsg, int32_t len, 
                   int32_t *pReceivedLen, int32_t timeout ) ;

int32_t disableNagle( Socket* sock ) ;
SOCKET clientGetRawSocket( Socket* sock ) ;
int32_t setKeepAlive( SOCKET sock, int32_t keepAlive, int32_t keepIdle,
                    int32_t keepInterval, int32_t keepCount ) ;

LDB_EXTERN_C_END
#endif

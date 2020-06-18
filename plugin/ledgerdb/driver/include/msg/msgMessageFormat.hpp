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

   Source File Name = msgMessageFormat.hpp

   Descriptive Name = Message Client Header

   When/how to use: this program may be used on binary and text-formatted
   versions of Messaging component. This file contains message structure for
   client-server communication.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/
#ifndef MSGMESSAGE_FORMAT_HPP_
#define MSGMESSAGE_FORMAT_HPP_

#include "msg/msg.h"
#include "net/netDef.hpp"
#include "bson/bson.h"
#include "bson/oid.h"
#include <string>

using namespace std;

const char* serviceID2String( UINT32 serviceID ) ;

string routeID2String( MsgRouteID routeID ) ;
string routeID2String( UINT64 nodeID ) ;

const char* msgType2String( MSG_TYPE msgType );

void msgExpandComSessionInit2String( stringstream &ss,
                                     const MsgHeader *pMsg,
                                     UINT32 expandMask ) ;

#define MSG_EXP_MASK_CLNAME            0x00000001
#define MSG_EXP_MASK_MATCHER           0x00000002
#define MSG_EXP_MASK_SELECTOR          0x00000004
#define MSG_EXP_MASK_ORDERBY           0x00000008
#define MSG_EXP_MASK_HINT              0x00000010
#define MSG_EXP_MASK_OTHER             0x00000020

#endif // MSGMESSAGE_FORMAT_HPP_


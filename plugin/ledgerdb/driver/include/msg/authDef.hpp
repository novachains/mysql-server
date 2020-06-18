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

   Source File Name = authDef.hpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#ifndef AUTHDEF_HPP_
#define AUTHDEF_HPP_

#include "oss/core.hpp"

namespace engine
{
   #define AUTH_SPACE                     "SYSAUTH"
   #define AUTH_USR_COLLECTION            AUTH_SPACE".SYSUSRS"
   /// AUTH_USR_COLLECTION SCHEMA
   /// {User:"", Passwd:""}

   #define AUTH_USR_INDEX_NAME            "usrindex"

}

#endif


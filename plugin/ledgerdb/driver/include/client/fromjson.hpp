/*******************************************************************************

   Copyright (C) 2011-2018 SequoiaDB Ltd.

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

   Source File Name = fromjson.hpp

*******************************************************************************/

/** \file fromjson.hpp
    \brief Convert from json to BSONObj
*/
#ifndef FROMJSON_HPP__
#define FROMJSON_HPP__

#include <string>
//#include "bson/bson.hpp"
/*
#include "bson/util/builder.h"
#include "bson/util/optime.h"
#include "bson/bsontypes.h"
#include "bson/oid.h"
#include "bson/bsonelement.h"
#include "bson/bsonmisc.h"
#include "bson/bsonobjbuilder.h"
#include "bson/bsonobjiterator.h"
#include "bson/bson-inl.h"
#include "bson/ordering.h"
#include "bson/stringdata.h"
#include "bson/bson_db.h"
#include "oss/ossTypes.h"
*/
/** \namespace bson
    \brief Include files for C++ BSON module
*/
namespace bson
{
    class BSONObj;
/** \fn int32_t fromjson ( const string &str, BSONObj &out ) ;
    \brief Convert from json to BSONObj.
    \param [in] str The json string to be converted
    \param [out] out The CPP BSONObj object of first json in "str"
    \param [in] isBatch Ignore the unnecessary things behind the first json or not
    \retval SDB_OK Connection Success
    \retval Others Connection Fail
*/
   extern int32_t fromjson ( const std::string &str, BSONObj &out,
                               bool isBatch = true ) ;

/** \fn int32_t fromjson ( const char *pStr, BSONObj &out ) ;
    \brief Convert from json to BSONObj.
    \param [in] pStr The C-style json charactor string to be converted
    \param [out] out The CPP BSONObj object of first json in "str"
    \param [in] isBatch Ignore the unnecessary things behind the first json or not 
    \retval SDB_OK Connection Success
    \retval Others Connection Fail
*/
   extern int32_t fromjson ( const char *pStr, BSONObj &out,
                               bool isBatch = true ) ;

}
#endif

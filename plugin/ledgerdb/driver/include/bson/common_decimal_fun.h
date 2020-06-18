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
*******************************************************************************/
#ifndef BSON_COMMON_DECIMAL_FUN_H_
#define BSON_COMMON_DECIMAL_FUN_H_

#include "bson/common_decimal_type.h"

LDB_EXTERN_C_START

LDB_EXPORT void ldb_decimal_init( bson_decimal *decimal );
LDB_EXPORT int  ldb_decimal_init1( bson_decimal *decimal,
                                   int precision, 
                                   int scale ) ;

LDB_EXPORT void ldb_decimal_free( bson_decimal *decimal ) ;

LDB_EXPORT void ldb_decimal_set_zero( bson_decimal *decimal ) ;
LDB_EXPORT int  ldb_decimal_is_zero( const bson_decimal *decimal ) ;

LDB_EXPORT int  ldb_decimal_is_special( const bson_decimal *decimal ) ;

LDB_EXPORT void ldb_decimal_set_nan( bson_decimal *decimal ) ;
LDB_EXPORT int  ldb_decimal_is_nan( const bson_decimal *decimal ) ;

LDB_EXPORT void ldb_decimal_set_min( bson_decimal *decimal ) ;
LDB_EXPORT int  ldb_decimal_is_min( const bson_decimal *decimal ) ;

LDB_EXPORT void ldb_decimal_set_max( bson_decimal *decimal ) ;
LDB_EXPORT int  ldb_decimal_is_max( const bson_decimal *decimal ) ;

LDB_EXPORT int  ldb_decimal_round( bson_decimal *decimal, int rscale ) ;

LDB_EXPORT int     ldb_decimal_to_int( const bson_decimal *decimal ) ;
LDB_EXPORT double  ldb_decimal_to_double( const bson_decimal *decimal ) ;
LDB_EXPORT int64_t ldb_decimal_to_long( const bson_decimal *decimal ) ;

LDB_EXPORT int     ldb_decimal_to_str_get_len( const bson_decimal *decimal, 
                                               int *size ) ;
LDB_EXPORT int     ldb_decimal_to_str( const bson_decimal *decimal,
                                       char *value, 
                                       int value_size ) ;

// the caller is responsible for freeing this decimal( ldb_decimal_free )
LDB_EXPORT int  ldb_decimal_from_int( int value, bson_decimal *decimal ) ;

// the caller is responsible for freeing this decimal( ldb_decimal_free )
LDB_EXPORT int  ldb_decimal_from_long( int64_t value, bson_decimal *decimal) ;

// the caller is responsible for freeing this decimal( ldb_decimal_free )
LDB_EXPORT int  ldb_decimal_from_double( double value, bson_decimal *decimal ) ;

// the caller is responsible for freeing this decimal( ldb_decimal_free )
LDB_EXPORT int  ldb_decimal_from_str( const char *value, bson_decimal *decimal ) ;

LDB_EXPORT int  ldb_decimal_get_typemod( const bson_decimal *decimal,
                                         int *precision, 
                                         int *scale ) ;
LDB_EXPORT int  ldb_decimal_get_typemod2( const bson_decimal *decimal ) ;
LDB_EXPORT int  ldb_decimal_copy( const bson_decimal *source, 
                                  bson_decimal *target ) ;

int ldb_decimal_from_bsonvalue( const char *value, bson_decimal *decimal ) ;

int ldb_decimal_to_jsonstr( const bson_decimal *decimal,
                            char *value, 
                            int value_size ) ;

int ldb_decimal_to_jsonstr_len( int sign, int weight, int dscale, 
                                int typemod, int *size ) ;

LDB_EXPORT int ldb_decimal_cmp( const bson_decimal *left, 
                                const bson_decimal *right ) ;

LDB_EXPORT int ldb_decimal_add( const bson_decimal *left, 
                                const bson_decimal *right,
                                bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_sub( const bson_decimal *left, 
                                const bson_decimal *right,
                                bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_mul( const bson_decimal *left, 
                                const bson_decimal *right,
                                bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_div( const bson_decimal *left, 
                                const bson_decimal *right,
                                bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_abs( bson_decimal *decimal ) ;

LDB_EXPORT int ldb_decimal_ceil( const bson_decimal *decimal, 
                                 bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_floor( const bson_decimal *decimal, 
                                  bson_decimal *result ) ;

LDB_EXPORT int ldb_decimal_mod( const bson_decimal *left,
                                const bson_decimal *right, 
                                bson_decimal *result ) ;

int ldb_decimal_update_typemod( bson_decimal *decimal, int typemod ) ;

int ldb_decimal_is_out_of_precision( bson_decimal *decimal, int typemod ) ;


LDB_EXTERN_C_END

/*
   For compatialbe with old version
*/

#define decimal_init             ldb_decimal_init
#define decimal_init1            ldb_decimal_init1
#define decimal_free             ldb_decimal_free
#define decimal_set_zero         ldb_decimal_set_zero
#define decimal_is_zero          ldb_decimal_is_zero
#define decimal_is_special       ldb_decimal_is_special
#define decimal_set_nan          ldb_decimal_set_nan
#define decimal_is_nan           ldb_decimal_is_nan
#define decimal_set_min          ldb_decimal_set_min
#define decimal_is_min           ldb_decimal_is_min
#define decimal_set_max          ldb_decimal_set_max
#define decimal_is_max           ldb_decimal_is_max
#define decimal_round            ldb_decimal_round
#define decimal_to_int           ldb_decimal_to_int
#define decimal_to_double        ldb_decimal_to_double
#define decimal_to_long          ldb_decimal_to_long
#define decimal_to_str_get_len   ldb_decimal_to_str_get_len
#define decimal_to_str           ldb_decimal_to_str

#define decimal_from_int         ldb_decimal_from_int
#define decimal_from_long        ldb_decimal_from_long
#define decimal_from_double      ldb_decimal_from_double
#define decimal_from_str         ldb_decimal_from_str

#define decimal_get_typemod      ldb_decimal_get_typemod
#define decimal_get_typemod2     ldb_decimal_get_typemod2
#define decimal_copy             ldb_decimal_copy
#define decimal_from_bsonvalue   ldb_decimal_from_bsonvalue
#define decimal_to_jsonstr       ldb_decimal_to_jsonstr
#define decimal_to_jsonstr_len   ldb_decimal_to_jsonstr_len

#define decimal_cmp              ldb_decimal_cmp
#define decimal_add              ldb_decimal_add
#define decimal_sub              ldb_decimal_sub
#define decimal_mul              ldb_decimal_mul
#define decimal_div              ldb_decimal_div
#define decimal_abs              ldb_decimal_abs
#define decimal_ceil             ldb_decimal_ceil
#define decimal_floor            ldb_decimal_floor
#define decimal_mod              ldb_decimal_mod
#define decimal_update_typemod   ldb_decimal_update_typemod
#define decimal_is_out_of_precision ldb_decimal_is_out_of_precision

#endif // BSON_COMMON_DECIMAL_FUN_H_

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
#ifndef BSON_COMMON_DECIMAL_TYPE_H_
#define BSON_COMMON_DECIMAL_TYPE_H_

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#if defined(__GNUC__) || defined(__xlC__)
    #define LDB_EXPORT
#else
    #ifdef LDB_STATIC_BUILD
        #define LDB_EXPORT
    #elif defined(LDB_DLL_BUILD)
        #define LDB_EXPORT __declspec(dllexport)
    #else
        #define LDB_EXPORT __declspec(dllimport)
    #endif
#endif

#if defined (__linux__) || defined (_AIX)
#include <sys/types.h>
#include <unistd.h>
#elif defined (_WIN32)
#include <Windows.h>
#include <WinBase.h>
#endif

#include <stdint.h>

#ifdef __cplusplus
#define LDB_EXTERN_C_START extern "C" {
#define LDB_EXTERN_C_END }
#else
#define LDB_EXTERN_C_START
#define LDB_EXTERN_C_END
#endif


#define LDB_DECIMAL_SIGN_MASK       0xC000
#define LDB_DECIMAL_POS             0x0000
#define LDB_DECIMAL_NEG             0x4000
#define LDB_DECIMAL_SPECIAL_SIGN    0xC000

#define LDB_DECIMAL_SPECIAL_NAN     0x0000
#define LDB_DECIMAL_SPECIAL_MIN     0x0001
#define LDB_DECIMAL_SPECIAL_MAX     0x0002

#define LDB_DECIMAL_DSCALE_MASK     0x3FFF


#define LDB_DECIMAL_DBL_DIG        ( DBL_DIG )

//sign + dscale
//   sign  = dscale & 0xC000
//   scale = dscale & 0x3FFF


/*
 * Hardcoded precision limit - arbitrary, but must be small enough that
 * dscale values will fit in 14 bits.
 */
#define LDB_DECIMAL_MAX_PRECISION         1000
#define LDB_DECIMAL_MAX_DISPLAY_SCALE     LDB_DECIMAL_MAX_PRECISION
#define LDB_DECIMAL_MIN_DISPLAY_SCALE     0
#define LDB_DECIMAL_MIN_SIG_DIGITS        16

#define LDB_DECIMAL_NBASE                 10000
#define LDB_DECIMAL_HALF_NBASE            5000
#define LDB_DECIMAL_DEC_DIGITS            4     /* decimal digits per NBASE digit */
#define LDB_DECIMAL_MUL_GUARD_DIGITS      2     /* these are measured in NBASE digits */
#define LDB_DECIMAL_DIV_GUARD_DIGITS      4

typedef struct {
   int typemod;    /* precision & scale define:  
                         precision = (typmod >> 16) & 0xffff
                         scale     = typmod & 0xffff */
   int ndigits;    /* length of digits */
   int sign;       /* the decimal's sign */
   int dscale;     /* display scale */
   int weight;     /* weight of first digit */
   int isOwn;      /* is digits allocated self */
   short *buff ;   /* start of palloc'd space for digits[] */
   short *digits;  /* real decimal data */
} bson_decimal ;

#define LDB_DECIMAL_DEFAULT_VALUE \
   { \
      -1,               /* typemode */ \
      0,                /* ndigits */ \
      LDB_DECIMAL_POS,  /* sign */ \
      0,                /* dscale */ \
      0,                /* weight */ \
      0,                /* isOwn */ \
      NULL,             /* buff */ \
      NULL              /* digits */\
   }

//storage detail define in bson.h  (BSON_DECIMAL)
#define LDB_DECIMAL_HEADER_SIZE     12  /*size + typemod + dscale + weight*/

#pragma pack(1)
typedef struct 
{
  int    size;    //total size of this value

  int    typemod; //precision + scale
                  //   precision = (typmod >> 16) & 0xffff
                  //   scale     = typmod & 0xffff

  short  dscale;  //sign + dscale
                  //   sign  = dscale & 0xC000
                  //   scale = dscale & 0x3FFF

  short  weight;  //weight of this decimal (NBASE=10000)

  //short  digitis[0]; //real data
} __sdb_decimal ;

#pragma pack()


#endif // BSON_COMMON_DECIMAL_TYPE_H_




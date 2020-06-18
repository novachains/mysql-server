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

   Source File Name = ossTypes.h

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#ifndef OSSTYPES_H_
#define OSSTYPES_H_

#include "oss/ossFeat.h"

#define LDB_UNUSED(x)      (x)=(x)

typedef int ossSystemError ;

typedef unsigned char        UINT8;
typedef unsigned char         BYTE;
typedef signed char          SINT8;
#if defined (_LINUX) || defined ( _AIX )
typedef signed char          INT8 ;
#define LDB_DEV_NULL         "/dev/null"
#endif

typedef unsigned short       UINT16;
typedef short                SINT16;
typedef short                INT16 ;

typedef unsigned int         UINT32;
typedef int                  SINT32;
typedef int                  INT32 ;

typedef unsigned long long   UINT64;
typedef long long            SINT64;
typedef long long            INT64 ;

#define OSS_UINT64_MAX 0xFFFFFFFFFFFFFFFFuLL
#define OSS_SINT64_MAX 0x7FFFFFFFFFFFFFFFLL
#define OSS_SINT64_MIN (-9223372036854775807LL-1)
#define OSS_SINT64_MAX_D (9223372036854775807.0)
#define OSS_SINT64_MIN_D (-9223372036854775808.0)
#define OSS_SINT64_JS_MAX  (9007199254740991LL)
#define OSS_SINT64_JS_MIN (-9007199254740991LL)
#define OSS_SINT32_MAX_LL (2147483647LL)
#define OSS_SINT32_MIN_LL (-2147483648LL)
#define OSS_SINT32_MAX_D  (2147483647.0)
#define OSS_SINT32_MIN_D  (-2147483648.0)

   typedef float                FLOAT32;
   typedef double               FLOAT64;

#if defined OSS_ARCH_64
   typedef UINT64               UINT32_64 ;
   typedef SINT64               SINT32_64 ;
#elif defined OSS_ARCH_32
   typedef UINT32               UINT32_64 ;
   typedef SINT32               SINT32_64 ;
#endif

#define LDB_PAGE_SIZE           4096

#if defined (_LINUX) || defined ( _AIX ) || defined ( _MACOS )
typedef INT32 SOCKET ;
#endif

#if defined _LINUX || defined _AIX || defined _MACOS
   #include "pthread.h"
   typedef pid_t           	OSSPID ;
   typedef pthread_t       	OSSTID ;
   typedef INT32           	OSSHANDLE ;
   #define OSS_INVALID_TID      ( ( OSSTID )NULL )
   typedef uid_t           	OSSUID ;
   typedef gid_t           	OSSGID ;
   // any attempt to get TLS variable should use OSS_FORCE_INLINE
   // It may avoid calling __tls_get_addr (x86 only)instruction repeatedly if
   // there's any for loop
   // eg:
   // static OSS_THREAD_LOCAL pmdEDUCB *_tlsEDUCB ;
   // OSS_FORCE_INLINE pmdEDUCB *getEDUCB ()
   // {
   //    return _tlsEDUCB ;
   // }
   #define OSS_FORCE_INLINE __attribute__((always_inline))
   #define OSS_THREAD_LOCAL __thread
#else
   #error Unsupported platform
#endif

#if defined OSS_HAVE_KERNEL_THREAD_ID
   typedef pid_t      OSS_KERNEL_TID
#endif

typedef UINT64 EDUID ;
#define OSS_INVALID_PID       ( ( OSSPID )-1 )
#define OSS_INVALID_UID       ( ( OSSUID )-1 )
#define OSS_INVALID_GID       ( ( OSSGID )-1 )


// return the minimum of two values
#define OSS_MIN(a, b) (((a) < (b)) ? (a) : (b))
//
// return the maximum of two values
#define OSS_MAX(a, b) (((a) > (b)) ? (a) : (b))

#define ossRoundDownToMultipleX(x,y) (((x)/(y))*(y))
#define ossRoundUpToMultipleX(x,y) (((x)+((y)-1))-(((x)+((y)-1))%(y)))
// check if an address is aligned on a 4 or 8 bytes boundary on its
// platform ( currently it works for 32bit or 64bit only )
#define ossIsAlignedNative(x) (0==(((uintptr_t)(x))&(sizeof(void*)-1)))
#define ossIsAligned4(x) (0==(((uintptr_t)(x))&(4-1)))
#define ossIsAligned8(x) (0==(((uintptr_t)(x))&(8-1)))

#define ossEndianConvert1(in,out)        \
do {                                     \
   out = in ;                            \
} while ( false )                        \

#define ossEndianConvert2(in,out)        \
do {                                     \
   const char *pin = (const char *)&in ; \
   char *pout = (char*)&out ;            \
   pout[0] = pin[1] ;                    \
   pout[1] = pin[0] ;                    \
} while ( false )                        \

#define ossEndianConvert4(in,out)        \
do {                                     \
   const char *pin = (const char *)&in ; \
   char *pout = (char*)&out ;            \
   pout[0] = pin[3] ;                    \
   pout[1] = pin[2] ;                    \
   pout[2] = pin[1] ;                    \
   pout[3] = pin[0] ;                    \
} while ( false )                        \

#define ossEndianConvert8(in,out)        \
do {                                     \
   const char *pin = (const char *)&in ; \
   char *pout = (char*)&out ;            \
   pout[0] = pin[7] ;                    \
   pout[1] = pin[6] ;                    \
   pout[2] = pin[5] ;                    \
   pout[3] = pin[4] ;                    \
   pout[4] = pin[3] ;                    \
   pout[5] = pin[2] ;                    \
   pout[6] = pin[1] ;                    \
   pout[7] = pin[0] ;                    \
} while ( false )                        \

#define ossEndianConvertIf1(in,out,condition) \
do {                                          \
   ossEndianConvert1 ( in, out ) ;            \
} while ( false )                             \

#define ossEndianConvertIf2(in,out,condition) \
do {                                          \
   if ( (condition) )                         \
   {                                          \
      ossEndianConvert2(in,out) ;             \
   }                                          \
   else                                       \
   {                                          \
      out=in ;                                \
   }                                          \
} while ( false )                             \

#define ossEndianConvertIf4(in,out,condition) \
do {                                          \
   if ( (condition) )                         \
   {                                          \
      ossEndianConvert4(in,out) ;             \
   }                                          \
   else                                       \
   {                                          \
      out=in ;                                \
   }                                          \
} while ( false )                             \

#define ossEndianConvertIf8(in,out,condition) \
do {                                          \
   if ( (condition) )                         \
   {                                          \
      ossEndianConvert8(in,out) ;             \
   }                                          \
   else                                       \
   {                                          \
      out=in ;                                \
   }                                          \
} while ( false )                             \

#define ossEndianConvertIf(in,out,condition)       \
do {                                               \
   if ( sizeof(in) != sizeof(out) ) {              \
      break ;                                      \
   }                                               \
   if ( sizeof(in) == 1 ) {                        \
      ossEndianConvertIf1(in,out,condition) ;      \
   } else if ( sizeof(in) == 2 ) {                 \
      ossEndianConvertIf2(in,out,condition) ;      \
   } else if ( sizeof(in) == 4 ) {                 \
      ossEndianConvertIf4(in,out,condition) ;      \
   } else if ( sizeof(in) == 8 ) {                 \
      ossEndianConvertIf8(in,out,condition) ;      \
   } else {                                        \
   }                                               \
} while ( false )                                  \

/*
   Shell return code
*/
enum LDB_SHELL_RETURN_CODE
{
   LDB_SRC_SUC                = 0,     // succeed
   LDB_SRC_EMPTY              = 1,     // empty out
   LDB_SRC_WARNING            = 2,     // warning
   LDB_SRC_ERROR              = 4,     // error
   LDB_SRC_SYS                = 8,     // System error

   LDB_SRC_INVALIDARG         = 127,   // invalid argment

   // user define, begin from 128
   LDB_SRC_IO                 = 128,   // IO Exception
   LDB_SRC_PERM               = 129,   // Permission Error
   LDB_SRC_OOM                = 130,   // Out of Memory
   LDB_SRC_INTERRUPT          = 131,   // Interrupt
   LDB_SRC_NOSPC              = 133,   // No space is left on disk
   LDB_SRC_TIMEOUT            = 134,   // Timeout error
   LDB_SRC_NETWORK            = 135,   // Network error
   LDB_SRC_INVALIDPATH        = 136,   // Given path is not valid
   LDB_SRC_CANNOT_LISTEN      = 137,   // Unable to listen the specified address
   LDB_SRC_CAT_AUTH_FAILED    = 138,   // Catalog authentication failed

   LDB_SRC_MAX                = 255    // max value
} ;

// define the client return code
// should always between 0 to 255
#define LDB_RETURNCODE_OK      LDB_SRC_SUC
#define LDB_RETURNCODE_EMPTY   LDB_SRC_EMPTY
#define LDB_RETURNCODE_WARNING LDB_SRC_WARNING
#define LDB_RETURNCODE_ERROR   LDB_SRC_ERROR
#define LDB_RETURNCODE_SYSTEM  LDB_SRC_SYS

#endif /* OSSTYPES_H_ */


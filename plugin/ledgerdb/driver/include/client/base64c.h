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

/** \file Base64c.h
    \brief Encode binary data using printable characters.
*/
#ifndef ALAN_BASE64_H
#define ALAN_BASE64_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__) || defined(__xlC__)
    #ifndef LDB_INLINE
       #define LDB_INLINE static __inline__
    #endif
    #define LDB_EXPORT
#else
    #ifndef LDB_INLINE
       #define LDB_INLINE static
    #endif
    #ifdef LDB_STATIC_BUILD
        #define LDB_EXPORT
    #elif defined(LDB_DLL_BUILD)
        #define LDB_EXPORT __declspec(dllexport)
    #else
        #define LDB_EXPORT __declspec(dllimport)
    #endif
#endif

#ifdef __cplusplus
#define LDB_EXTERN_C_START extern "C" {
#define LDB_EXTERN_C_END }
#else
#define LDB_EXTERN_C_START
#define LDB_EXTERN_C_END
#endif

LDB_EXTERN_C_START


/** \fn int base64Encode ( const char *s, int in_size, char *out, int out_size )
    \brief String convert base64
    \param [in] s Input buffer
    \param [in] in_size Input buffer size
    \param [in] out_size The size of the buffer that 'out' pointing to ,
                                   make sure it's large enough
    \param [out] out Output string
    \return If successful return the length of base64,else return 0
    \code
     char *str = "hello world" ;
     int strLen = strlen( str ) ;
     int len = getEnBase64Size ( strLen ) ;
     char *out = (char *)malloc( len ) ;
     memset( out, 0, len ) ;
     base64Encode( str, strLen, out, len ) ;
     printf( "out is: %s\n", out ) ;
     free( out ) ;
    \endcode
*/
LDB_EXPORT int base64Encode ( const char *s, int in_size, char *out, int out_size ) ;

/** \fn int base64Decode ( const char *s, char *out, int out_size ) 
    \brief Base64 convert string
    \param [in] s Input string
    \param [in] out_size The size of the buffer that 'out' pointing to ,
                                   make sure it's large enough
    \param [out] out Output string
    \return If successful return the length of string,else return 0
    \code
     char *str = "aGVsbG8gd29ybGQ=" ;
     int len = getDeBase64Size ( str ) ;
     char *out = (char *)malloc( len ) ;
     memset( out, 0, len ) ;
     base64Decode( str, out, len ) ;
     printf( "out is: %s\n", out ) ;
     free( out ) ;
   \endcode
*/
LDB_EXPORT int base64Decode ( const char *s, char *out, int out_size ) ;

/** \fn int getEnBase64Size ( int size )
    \brief Get string convert base64 need size
    \param [in] size original data size
    \return Base64 length + 1
*/
LDB_EXPORT int getEnBase64Size ( int size ) ;

/** \fn int getDeBase64Size ( const char *s )
    \brief Get base64 convert string need size
    \param [in] s Base64
    \return String length + 1
*/
LDB_EXPORT int getDeBase64Size ( const char *s ) ;

LDB_EXTERN_C_END

#endif

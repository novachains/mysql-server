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

   Source File Name = ossFeat.h

   Descriptive Name =

   When/how to use:

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#ifndef OSSFEAT_H_
#define OSSFEAT_H_

/**
 *   \file ossfeat.h
 *   \brief Operating system specific features
 *
 */

#if defined(__linux__)  && defined(__i386__)
   #define _LIN32
   #define _LINUX
#elif defined(__linux__) && (defined(__ia64__)||defined(__x86_64__))
   #define _LIN64
   #define _LINUX
#elif defined(__linux__) && (defined(__PPC64__))
   #define _PPCLIN64
   #define _LINUX
#endif

// architecture
#if defined ( _LIN32 )
   #define OSS_ARCH_32
#elif defined ( _LIN64 ) || defined ( _PPCLIN64 )
   #define OSS_ARCH_64
#endif

#define OSS_OSTYPE_LIN32               1
#define OSS_OSTYPE_LIN64               2
#define OSS_OSTYPE_PPCLIN64            3

#if defined (_LIN32)
#define OSS_OSTYPE                     OSS_OSTYPE_LIN32
#elif defined (_LIN64)
#define OSS_OSTYPE                     OSS_OSTYPE_LIN64
#elif defined (_PPCLIN64)
#define OSS_OSTYPE                     OSS_OSTYPE_PPCLIN64
#endif


#if defined _LINUX
   #include <errno.h>
   #define OSS_HAS_KERNEL_THREAD_ID
   #define __FUNC__ __func__
   #define LDB_EXPORT

   // max fd size is 65528 on linux
   #define OSS_FD_SETSIZE  65528
   // this header must be included BEFORE __FD_SETSIZE declaration
   #include <bits/types.h>
   #include <linux/posix_types.h>


   // __FD_SETSIZE is only for Linux and HPUX
   #undef __FD_SETSIZE
   #define __FD_SETSIZE    OSS_FD_SETSIZE
   // FD_SETSIZE is for all other unix
   #undef FD_SETSIZE
   #define FD_SETSIZE      __FD_SETSIZE
   // sys/types.h must be included AFTER __FD_SETSIZE declaration
   #include <sys/types.h>
#endif

#endif /* OSSFEAT_H_ */


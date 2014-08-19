/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef HBASE_COMMON_MACROS_H__
#define HBASE_COMMON_MACROS_H__

#include <hbase/log.h>

#ifdef LIKELY 
#undef LIKELY
#endif

#ifdef UNLIKELY 
#undef UNLIKELY
#endif

#if __GNUC__ >= 3
  #define EXPECT(expr, value)   __builtin_expect((expr), (value))
#else
  #define EXPECT(expr, value)   (expr)
#endif

#ifndef TRUE
  #define TRUE 1
#endif
#ifndef FALSE
  #define FALSE 0
#endif

#define LIKELY(expr)    EXPECT((expr), TRUE)
#define UNLIKELY(expr)  EXPECT((expr), FALSE)

#define RETURN_IF_INVALID_PARAM(stmt, ...) \
  if (UNLIKELY(stmt)){ \
    HBASE_LOG_ERROR(__VA_ARGS__); \
    return EINVAL; \
  }

#define CALLBACK_IF_INVALID_PARAM(stmt, call_back, \
    client, operation, result, extra, ...) \
  if (UNLIKELY(stmt)){ \
    HBASE_LOG_ERROR(__VA_ARGS__); \
    call_back(EINVAL, client, operation, result, extra); \
    return 0; \
  }

#define RETURN_IF_ERROR(stmt) \
  do { \
    Status __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) return __status__; \
  } while (false)

#endif /* HBASE_COMMON_MACROS_H__ */

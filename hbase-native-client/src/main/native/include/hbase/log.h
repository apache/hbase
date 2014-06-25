/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef LIBHBASE_LOG_H_
#define LIBHBASE_LOG_H_

#include <stdio.h>

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Set the log output level
 */
HBASE_API void
hb_log_set_level(HBaseLogLevel level);

/**
 * By default, the log messages are sent to stderr. Use this function to
 * redirect it to another file stream. Setting it to NULL disables logging.
 */
HBASE_API void
hb_log_set_stream(FILE *stream);

/**
 * An application using the HBase APIs could use these macros to direct
 * the log messages to the same stream used by HBase libraries.
 */
#define HBASE_LOG_FATAL(...) if(hb_log_get_level() >= HBASE_LOG_LEVEL_FATAL) \
    hb_log_message(HBASE_LOG_LEVEL_FATAL, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_ERROR(...) if(hb_log_get_level() >= HBASE_LOG_LEVEL_ERROR) \
    hb_log_message(HBASE_LOG_LEVEL_ERROR, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_WARN(...) if(hb_log_get_level() >= HBASE_LOG_LEVEL_WARN) \
    hb_log_message(HBASE_LOG_LEVEL_WARN, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_INFO(...) if(hb_log_get_level() >= HBASE_LOG_LEVEL_INFO) \
    hb_log_message(HBASE_LOG_LEVEL_INFO, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_DEBUG(...) if(hb_log_get_level() == HBASE_LOG_LEVEL_DEBUG) \
    hb_log_message(HBASE_LOG_LEVEL_DEBUG, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_TRACE(...) if(hb_log_get_level() == HBASE_LOG_LEVEL_TRACE) \
    hb_log_message(HBASE_LOG_LEVEL_TRACE, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

#define HBASE_LOG_MSG(level, ...) if(hb_log_get_level() >= level) \
    hb_log_message(level, __LINE__, __FILE__, __func__, \
        hb_format_log_message(__VA_ARGS__))

/**
 * For internal use. Use one of the macros defined later in this file.
 */
HBASE_API void
hb_log_message(
    HBaseLogLevel curLevel,
    int line,
    const char *fileName,
    const char *funcName,
    const char *message);

/**
 * For internal use. Use one of the macros defined later in this file.
 */
HBASE_API const char *
hb_format_log_message(const char *format, ...);

/**
 * For internal use. Get the log output level.
 */
HBASE_API HBaseLogLevel
hb_log_get_level();

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* LIBHBASE_LOG_H_ */

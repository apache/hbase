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
#line 19 "hbase_log.cc" // ensures short filename in logs.

#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

#include <hbase/log.h>

#line __LINE__ "hbase_log.cc"

#define TIME_NOW_BUF_SIZE   1024

#define FORMAT_LOG_BUF_SIZE 4096

#define LOGSTREAM getLogStream()

#ifdef THREADED

static pthread_key_t time_now_buffer;

static pthread_key_t format_log_msg_buffer;

void
freeBuffer(void *p) {
  if(p) free(p);
}

__attribute__((constructor)) void prepareTSDKeys() {
  pthread_key_create(&time_now_buffer, freeBuffer);
  pthread_key_create(&format_log_msg_buffer, freeBuffer);
}

char*
getTSData(pthread_key_t key, int size) {
  char *p = (char*)pthread_getspecific(key);
  if(p == 0) {
    int res;
    p = (char*)calloc(1, size);
    res = pthread_setspecific(key, p);
    if(res != 0) {
      fprintf(stderr,"Failed to set TSD key: %d", res);
    }
  }
  return p;
}

char *
get_time_buffer() {
  return getTSData(time_now_buffer, TIME_NOW_BUF_SIZE);
}

char *
get_format_log_buffer() {
  return getTSData(format_log_msg_buffer, FORMAT_LOG_BUF_SIZE);
}

#else // #ifdef THREADED

char *
get_time_buffer() {
  static char buf[TIME_NOW_BUF_SIZE];
  return buf;
}

char *
get_format_log_buffer() {
  static char buf[FORMAT_LOG_BUF_SIZE];
  return buf;
}

#endif // #ifdef THREADED

static HBaseLogLevel hbLogLevel = HBASE_LOG_LEVEL_INFO;

static FILE *logStream = 0;
FILE *getLogStream() {
  if(logStream == 0)
    logStream = stderr;
  return logStream;
}

static const char *
time_now(char *now_str) {
  struct timeval tv;
  struct tm lt;
  time_t now = 0;
  size_t len = 0;

  gettimeofday(&tv, 0);
  now = tv.tv_sec;
  localtime_r(&now, &lt);

  // clone the format used by log4j ISO8601DateFormat
  // specifically: "yyyy-MM-dd HH:mm:ss,SSS"
  len = strftime(now_str, TIME_NOW_BUF_SIZE,
                 "%Y-%m-%d %H:%M:%S", &lt);
  len += snprintf(now_str + len,
                  TIME_NOW_BUF_SIZE - len,
                  ",%03d", (int)(tv.tv_usec/1000));
  return now_str;
}

#ifdef __cplusplus
extern "C" {
#endif

static const char *DBG_LEVEL_STR[] = {
    "?????", "FATAL", "ERROR",
    "WARN ", "INFO ", "DEBUG", "TRACE"
};

HBASE_API void
hb_log_message(
    HBaseLogLevel curLevel,
    int line,
    const char *fileName,
    const char *funcName,
    const char *message) {
  if (curLevel < HBASE_LOG_LEVEL_FATAL
      || curLevel > HBASE_LOG_LEVEL_TRACE) {
    curLevel = HBASE_LOG_LEVEL_INVALID;
  }
  static pid_t pid = 0;
  if (pid==0) {
    pid = getpid();
  }
#ifndef THREADED
  fprintf(LOGSTREAM, "%s %s %d@%s(%s#%d): %s\n",
      time_now(get_time_buffer()), DBG_LEVEL_STR[curLevel],
      pid, fileName, funcName, line, message);
#else
  fprintf(LOGSTREAM, "%s %s %d(0x%lx)@%s(%s#%d): %s\n",
      time_now(get_time_buffer()), DBG_LEVEL_STR[curLevel], pid,
      (unsigned long int)pthread_self(), fileName, funcName, line, message);
#endif
  fflush(LOGSTREAM);
}

HBASE_API const char*
hb_format_log_message(const char *format, ...) {
  va_list va;
  char *buf = get_format_log_buffer();
  if(!buf) {
    return "format_log_message: Unable to allocate memory buffer";
  }

  va_start(va, format);
  int msgLen = vsnprintf(buf, FORMAT_LOG_BUF_SIZE, format, va);
  if (msgLen >= FORMAT_LOG_BUF_SIZE) {
    // indicate that the log was truncated.
    snprintf(buf+(FORMAT_LOG_BUF_SIZE-11), 11, "...(%05d)",
        (msgLen-FORMAT_LOG_BUF_SIZE+10));
  }
  va_end(va);
  return buf;
}

HBASE_API void
hb_log_set_stream(FILE *stream) {
  logStream = stream;
}

HBASE_API HBaseLogLevel
hb_log_get_level() {
  return hbLogLevel;
}

HBASE_API void
hb_log_set_level(HBaseLogLevel level) {
  if(level == 0) {
    // disable logging (unit tests do this)
    hbLogLevel = (HBaseLogLevel)0;
    return;
  }
  if(level >= HBASE_LOG_LEVEL_FATAL
      && level <= HBASE_LOG_LEVEL_TRACE) {
    hbLogLevel = level;
  } else {
    HBASE_LOG_ERROR("Invalid log level: %d", (int32_t)level);
  }
}

#ifdef __cplusplus
}
#endif

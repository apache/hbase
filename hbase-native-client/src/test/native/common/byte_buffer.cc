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
#line 19 "byte_buffer.cc" // ensures short filename in logs.

#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>

#include "byte_buffer.h"
#include "common_utils.h"

#ifdef __cplusplus
extern  "C" {
#endif

#define MALLOCED_BUFFER        (1 << 0x00)
#define INLINE_BUFFER          (1 << 0x01)

static void
bytebuffer_release_node(bytebuffer byte_buf) {
  free(byte_buf);
}

static bytebuffer
bytebuffer_alloc_node() {
  // TODO These fixed size nodes can be managed from a pool.
  return (bytebuffer) malloc(sizeof(struct bytebuffer_));
}

/**
 * Allocates and wrap a byte array of size 'length' in the provided bytebuffer.
 *
 * @returns 0 if allocation was successful, non-zero otherwise.
 */
static int32_t
bytebuffer_alloc_internal(
    size_t len,
    bytebuffer byte_buf) {
  if (byte_buf != NULL) {
    if (len > INLINE_BUFFER_SIZE) {
      // would an aligned alloc be better?
      byte_buf->buffer = (unsigned char*) malloc(len);
      if (byte_buf->buffer == NULL) {
        return ENOMEM;
      }
      byte_buf->flags_ = MALLOCED_BUFFER;
      byte_buf->capacity_ = len;
    } else {
      byte_buf->buffer = byte_buf->internal_;
      byte_buf->flags_ = INLINE_BUFFER;
      byte_buf->capacity_ = INLINE_BUFFER_SIZE;
    }
    byte_buf->length = len;
    return 0;
  }
  return EINVAL;
}

/* exported functions */

uint64_t
currentTimeMicroSeconds() {
  struct timeval te;
  gettimeofday(&te, NULL); // get current time
  return (te.tv_sec*1000000uLL + (te.tv_usec)); // calculate microseconds
}

//from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
uint64_t
FNVHash64(uint64_t val) {
  uint64_t hashval = 0xcbf29ce484222325uLL;
  for (int32_t i = 0; i < 8; i++) {
    long octet = val & 0xff;
    val = val >> 8;
    hashval = hashval ^ octet;
    hashval = hashval * 0x100000001b3uLL;
  }
  return hashval;
}

bytebuffer
generateRowKey(
    const char* rowkey_prefix,
    const bool hashKeys,
    uint64_t opNum) {
  return bytebuffer_printf("%s%llu", rowkey_prefix,
      (hashKeys ? FNVHash64(opNum) : opNum));
}

/**
 * @returns an allocated bytebuffer of size 'length'
 */
bytebuffer
bytebuffer_alloc(const size_t length) {
  bytebuffer byte_buf = bytebuffer_alloc_node();
  if (byte_buf != NULL) {
    if ((errno = bytebuffer_alloc_internal(length, byte_buf)) != 0) {
      bytebuffer_release_node(byte_buf);
      byte_buf = NULL;
    }
  }
  return byte_buf;
}

/**
 * Deallocates any previously allocated block of memory buffer.
 */
int32_t
bytebuffer_free(bytebuffer byte_buf) {
  if(byte_buf == NULL) return EINVAL;

  if (byte_buf->flags_ & MALLOCED_BUFFER) {
    free(byte_buf->buffer);
  }
  byte_buf->capacity_ = byte_buf->length = byte_buf->flags_ = 0;
  byte_buf->buffer = NULL;
  bytebuffer_release_node(byte_buf);
  return 0;
}

/**
 * Creates an bytebuffer with a copy of byte array bounded by
 * 'start' and 'length'.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains a copy of the byte array specified by 'start' and
 * 'length', or NULL if fails.
 */
bytebuffer
bytebuffer_memcpy(const char *buffer,
    const size_t length) {
  bytebuffer byte_buf = bytebuffer_alloc(length);
  if (byte_buf != NULL) {
    memcpy(byte_buf->buffer, buffer, length);
  }
  return byte_buf;
}

/**
 * Creates an bytebuffer of specified length and fill it with
 * random bytes.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains an allocated byte array of length 'length' filled
 * with pseudo random bytes, or NULL if fails.
 */
bytebuffer
bytebuffer_random(const size_t length) {
  bytebuffer byte_buf = bytebuffer_alloc(length);
  uint64_t *uint64_ptr = (uint64_t *)byte_buf->buffer;
  uint64_t seed = currentTimeMicroSeconds();
  size_t numInts = length/sizeof(uint64_t);
  for (size_t i = 0; i < numInts; ++i) {
    *(uint64_ptr++) = seed = (uint64_t)FNVHash64(seed);
  }
  unsigned char *byte_ptr = (unsigned char *)uint64_ptr;
  for (size_t i = numInts*sizeof(uint64_t); i < length; ++i) {
    *(byte_ptr++) = (unsigned char)rand();
  }
  return byte_buf;
}

/**
 * Create an bytebuffer with a copy of a C style NULL
 * terminated string. The terminating NULL is NOT copied.
 *
 * @returns a pointer to an allocated bytebuffer structure
 * which contains a copy of the NULL terminated string,
 * or NULL if fails.
 */
bytebuffer
bytebuffer_strcpy(const char *string) {
  return bytebuffer_memcpy(string, strlen(string));
}

/**
 * Create an bytebuffer populated with formatted text in the
 * same way as printf() function would output to stdout.
 *
 * A NULL terminator is NOT appended to the buffer.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains the same text that would be printed if format and
 * arguments were used with printf(format, ...).
 */
bytebuffer
bytebuffer_printf(
    const char *format,
    ...) {
  va_list args_size;
  va_start(args_size, format);
  size_t length = vsnprintf(NULL, 0, format, args_size) + 1;
  va_end(args_size);

  bytebuffer bytebuffer = bytebuffer_alloc(length);
  if (bytebuffer != NULL) {
    va_list args;
    va_start(args, format);
    vsnprintf((char *)bytebuffer->buffer, length, format, args);
    bytebuffer->length--;
    va_end(args);
  }

  return bytebuffer;
}

#ifdef __cplusplus
}
#endif

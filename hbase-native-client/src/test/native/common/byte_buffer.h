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
#ifndef HBASE_TESTS_BYTE_BUFFER_H_
#define HBASE_TESTS_BYTE_BUFFER_H_

#ifdef __cplusplus
extern  "C" {
#endif

/**
 * Size of the in-line buffer.
 */
#define INLINE_BUFFER_SIZE  (128 - ( \
                                   sizeof(char*)  + \
                                   sizeof(size_t) + \
                                   sizeof(size_t) + \
                                   sizeof(uint64_t)))
struct bytebuffer_ {
//public:
  unsigned char *buffer; /* pointer to the data held in the buffer */
  size_t length; /* length of the available data in bytes */

//private:
  size_t   capacity_; /* size of allocated buffer, >= 'length' */
  uint64_t flags_;    /* reserved for internal use */

  /**
   * An internal buffer which can be used to optimize allocation of short
   * length byte arrays in-line with the structure thus avoiding double
   * allocation. Application must never access it directly instead use
   * 'buffer' and 'length' to access the data.
   */
  unsigned char internal_[INLINE_BUFFER_SIZE];
};
typedef struct bytebuffer_ *bytebuffer;

/**
 * Use this function to allocate an bytebuffer structure.
 *
 * @returns a pointer to an allocated bytebuffer of size 'length', or NULL
 * if fails.
 */
bytebuffer
bytebuffer_alloc(const size_t length);

/**
 * Deallocates any previously allocated block of memory buffer. Any application
 * owned bytebuffer must be freed by calling this method.
 */
int32_t
bytebuffer_free(bytebuffer byteBuf);

/**
 * Creates an bytebuffer with a copy of byte array bounded by
 * 'start' and 'length'.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains a copy of the byte array specified by 'start' and
 * 'length', or NULL if fails.
 */
bytebuffer
bytebuffer_memcpy(const char *start, const size_t length);

/**
 * Create an bytebuffer with a copy of a C style NULL
 * terminated string. The terminating NULL is NOT copied.
 *
 * @returns a pointer to an allocated bytebuffer structure
 * which contains a copy of the NULL terminated string,
 * or NULL if fails.
 */
bytebuffer
bytebuffer_strcpy(const char *string);

/**
 * Create an bytebuffer populated with formatted text in the
 * same way as printf() function would output to stdout.
 *
 * A NULL terminator is NOT appended to the buffer.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains the same text that would be printed if format and
 * arguments were used with printf(format, ...) or NULL if fails.
 */
bytebuffer
bytebuffer_printf(const char *format, ...);

/**
 * Creates an bytebuffer of specified length and fill it with
 * random bytes.
 *
 * @returns a pointer to an allocated bytebuffer structure which
 * contains an allocated byte array of length 'length' filled
 * with pseudo random bytes, or NULL if fails.
 */
bytebuffer
bytebuffer_random(const size_t length);


bytebuffer
generateRowKey(const char *rowkey_prefix,
    const bool hashKeys,
    uint64_t opNum);

uint64_t
currentTimeMicroSeconds();

uint64_t
FNVHash64(uint64_t val);

#ifdef __cplusplus
}
#endif

#endif /* HBASE_TESTS_BYTE_BUFFER_H_ */

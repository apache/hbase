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
#line 19 "hbase_put.cc" // ensures short filename in logs.

#include <jni.h>
#include <string.h>
#include <errno.h>

#include <hbase/mutations.h>

#include "hbase_put.h"

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Create empty put object with row key set to 'rowkey'.
 */
HBASE_API int32_t
hb_put_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_put_t *put_ptr) {
  RETURN_IF_INVALID_PARAM((rowkey == NULL),
      Msgs::ERR_KEY_NULL);
  RETURN_IF_INVALID_PARAM((rowkey_len <= 0),
      Msgs::ERR_KEY_LEN, rowkey_len);
  RETURN_IF_INVALID_PARAM((put_ptr == NULL),
      Msgs::ERR_PUTPTR_NULL);

  *put_ptr = NULL;
  Put *put = new Put();
  Status status = put->Init(
      CLASS_PUT_PROXY, rowkey, rowkey_len);
  if (UNLIKELY(!status.ok())) {
    delete put;
    return status.GetCode();
  }
  *put_ptr = reinterpret_cast<hb_put_t> (put);
  return 0;
}

HBASE_API int32_t
hb_put_add_cell(
    hb_put_t p,
    const hb_cell_t *cell) {
  RETURN_IF_INVALID_PARAM((cell == NULL),
      Msgs::ERR_CELLPTR_NULL);

  return hb_put_add_ts_column(p,
      cell->family, cell->family_len,
      cell->qualifier, cell->qualifier_len,
      cell->value, cell->value_len, cell->ts);
}

/**
 * Adds a cell (key-value) with latest timestamp to the put.
 */
HBASE_API int32_t
hb_put_add_column(
    hb_put_t p,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const byte_t *value,
    const size_t value_len) {
  return hb_put_add_ts_column(p,
      family, family_len,
      qualifier, qualifier_len,
      value, value_len, HBASE_LATEST_TIMESTAMP);
}

/**
 * Adds a cell (key-value) with the specified timestamp to the put.
 */
HBASE_API int32_t
hb_put_add_ts_column(
    hb_put_t p,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const byte_t *value,
    const size_t value_len,
    const int64_t timestamp) {
  RETURN_IF_INVALID_PARAM((p == NULL),
      Msgs::ERR_PUT_NULL);
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);
  RETURN_IF_INVALID_PARAM((qualifier == NULL),
      Msgs::ERR_QUAL_NULL);
  RETURN_IF_INVALID_PARAM((qualifier_len <= 0),
      Msgs::ERR_QUAL_LEN, qualifier_len);
  RETURN_IF_INVALID_PARAM((value == NULL),
      Msgs::ERR_VALUE_NULL);
  RETURN_IF_INVALID_PARAM((value_len <= 0),
      Msgs::ERR_VALUE_LEN, value_len);
  RETURN_IF_INVALID_PARAM((timestamp <= 0),
      Msgs::ERR_TIMESTAMP1, timestamp);

  return reinterpret_cast<Put *>(p)->
      AddColumn(family, family_len,
          qualifier, qualifier_len,
          value, value_len, timestamp).GetCode();
}

} /* extern "C" */

} /* namespace hbase */

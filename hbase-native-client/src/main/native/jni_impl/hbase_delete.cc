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
#line 19 "hbase_delete.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_delete.h"

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Creates a structure for delete operation and return its handle.
 */
HBASE_API int32_t
hb_delete_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_delete_t *delete_ptr) {
  RETURN_IF_INVALID_PARAM((rowkey == NULL),
      Msgs::ERR_KEY_NULL);
  RETURN_IF_INVALID_PARAM((rowkey_len <= 0),
      Msgs::ERR_KEY_LEN, rowkey_len);
  RETURN_IF_INVALID_PARAM((delete_ptr == NULL),
      Msgs::ERR_DELPTR_NULL);

  *delete_ptr = NULL;
  Delete *del = new Delete();
  Status status = del->Init(
      CLASS_DELETE_PROXY, rowkey, rowkey_len);
  if (UNLIKELY(!status.ok())) {
    delete del;
    return status.GetCode();
  }
  *delete_ptr = reinterpret_cast<hb_delete_t> (del);
  return 0;
}

/**
 * Optional.
 * Sets the timestamp of the delete row or delete family operation.
 * This does not override the timestamp of individual columns added via
 * hb_delete_add_column().
 */
HBASE_API int32_t
hb_delete_set_timestamp(
    hb_delete_t d,
    const int64_t timestamp) {
  RETURN_IF_INVALID_PARAM((d == NULL),
      Msgs::ERR_DEL_NULL);
  RETURN_IF_INVALID_PARAM((timestamp <= 0),
      Msgs::ERR_TIMESTAMP1, timestamp);

  return reinterpret_cast<Delete*>(d)->
      SetTS(timestamp).GetCode();
}

/**
 * Optional.
 * Set the column criteria for hb_delete_t object.
 * Set the qualifier to NULL to delete all columns of a family.
 * Only the cells with timestamp less than or equal to the specified timestamp
 * are deleted. Set the timestamp to -1 to delete all versions of the column.
 */
HBASE_API int32_t
hb_delete_add_column(
    hb_delete_t d,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const int64_t timestamp) {
  RETURN_IF_INVALID_PARAM((d == NULL),
      Msgs::ERR_DEL_NULL);
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);
  RETURN_IF_INVALID_PARAM((timestamp <= 0 && timestamp != -1),
      Msgs::ERR_TIMESTAMP2, timestamp);

  return reinterpret_cast<Delete*>(d)->
      AddColumn(family, family_len, qualifier,
          qualifier_len, NULL, 0, timestamp).GetCode();
}

} /* extern "C" */

} /* namespace hbase */

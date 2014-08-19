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
#line 19 "hbase_get.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_get.h"

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Creates an hb_get_t object and populate the handle get_ptr.
 */
HBASE_API int32_t
hb_get_create(
    const byte_t *rowkey,
    const size_t rowkey_len,
    hb_get_t *get_ptr) {
  RETURN_IF_INVALID_PARAM((rowkey == NULL),
      Msgs::ERR_KEY_NULL);
  RETURN_IF_INVALID_PARAM((rowkey_len <= 0),
      Msgs::ERR_KEY_LEN, rowkey_len);
  RETURN_IF_INVALID_PARAM((get_ptr == NULL),
      Msgs::ERR_GETPTR_NULL);

  *get_ptr = NULL;
  Get *get = new Get();
  if (get == NULL) {
    return ENOMEM;
  }
  Status status = get->Init(
      CLASS_GET_PROXY, rowkey, rowkey_len);
  if (UNLIKELY(!status.ok())) {
    delete get;
    return status.GetCode();
  }
  *get_ptr = reinterpret_cast<hb_get_t> (get);
  return 0;
}

/**
 * Sets the row key, required.
 */
HBASE_API int32_t
hb_get_set_row(
    hb_get_t get,
    const byte_t *rowkey,
    const size_t rowkey_len) {
  RETURN_IF_INVALID_PARAM((get == NULL),
      Msgs::ERR_GET_NULL);
  RETURN_IF_INVALID_PARAM((rowkey == NULL),
      Msgs::ERR_KEY_NULL);
  RETURN_IF_INVALID_PARAM((rowkey_len <= 0),
      Msgs::ERR_KEY_LEN, rowkey_len);

  return reinterpret_cast<Get*>(get)->
      SetRowKey(rowkey, rowkey_len).GetCode();
}

/**
 * Sets the table name, required.
 */
HBASE_API int32_t
hb_get_set_table(
    hb_get_t get,
    const char *table,
    const size_t table_len) {
  RETURN_IF_INVALID_PARAM((get == NULL),
      Msgs::ERR_GET_NULL);
  RETURN_IF_INVALID_PARAM((table == NULL),
      Msgs::ERR_TBL_NULL);
  RETURN_IF_INVALID_PARAM((table_len <= 0),
      Msgs::ERR_TBL_LEN, table_len);

  return reinterpret_cast<Get *>(get)->
      SetTable(table, table_len).GetCode();
}

/**
 * Sets the table namespace (0.96 and later)
 */
HBASE_API int32_t
hb_get_set_namespace(
    hb_get_t get,
    const char *name_space,
    const size_t name_space_len) {
  return ENOSYS;
}

/**
 * Adds a column family and optionally a column qualifier to
 * the hb_get_t object, optional.
 */
HBASE_API int32_t
hb_get_add_column(
    hb_get_t get,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len) {
  RETURN_IF_INVALID_PARAM((get == NULL),
      Msgs::ERR_GET_NULL);
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);

  return reinterpret_cast<Get*>(get)->
      AddColumn(family, family_len,
          qualifier, qualifier_len).GetCode();
}

/**
 * Optional. Adds a filter to the hb_get_t object.
 *
 * The filter must be specified using HBase Filter Language.
 * Refer to class org.apache.hadoop.hbase.filter.ParseFilter and
 * https://issues.apache.org/jira/browse/HBASE-4176 or
 * http://hbase.apache.org/book.html#thrift.filter-language for
 * language syntax and additional details.
 */
HBASE_API int32_t
hb_get_set_filter(
    hb_get_t get,
    const char *filter) {
  return ENOSYS;
}

/**
 * Optional. Sets maximum number of latest values of each column to be
 * fetched.
 */
HBASE_API int32_t
hb_get_set_num_versions(
    hb_get_t get,
    const int32_t num_versions) {
  RETURN_IF_INVALID_PARAM((get == NULL),
      Msgs::ERR_GET_NULL);
  RETURN_IF_INVALID_PARAM((num_versions <= 0),
      Msgs::ERR_NUM_VERSIONS);

  return reinterpret_cast<Get*>(get)->
      SetNumVersions(num_versions).GetCode();
}

/**
 * Optional. Only columns with the specified timestamp will be included.
 */
HBASE_API int32_t
hb_get_set_timestamp(
    hb_get_t get,
    const int64_t ts) {
  return ENOSYS;
}

/**
 * Release any resource held by hb_get_t object.
 */
HBASE_API int32_t
hb_get_destroy(hb_get_t get) {
  RETURN_IF_INVALID_PARAM((get == NULL),
      Msgs::ERR_GET_NULL);

  delete reinterpret_cast<Get *>(get);
  return 0;
}

} /* extern "C" */

Status
Get::AddColumn(
    const byte_t *f,
    const size_t fLen,
    const byte_t *q,
    const size_t qLen,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult family = JniHelper::CreateJavaByteArray(env, f, 0, fLen);
  RETURN_IF_ERROR(family);

  jobject qualifier = NULL;
  if (q != NULL) {
    JniResult result = JniHelper::CreateJavaByteArray(env, q, 0, qLen);
    RETURN_IF_ERROR(result);
    qualifier = result.GetObject();
  }

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_GET_PROXY, "addColumn",
      "([B[B)L"CLASS_GET_PROXY";", family.GetObject(), qualifier);
}

Status
Get::SetNumVersions(
    const int32_t numVersions,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_GET_PROXY, "setMaxVersions",
      JMETHOD1("I", JPARAM(CLASS_GET_PROXY)), numVersions);
}

Status
Get::SetFilter(
    const char *filter,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  jstring filterString = env->NewStringUTF(filter);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_GET_PROXY, "setFilter",
      JMETHOD1(JPARAM(JAVA_STRING), JPARAM(CLASS_GET_PROXY)), filterString);
}

} /* namespace hbase */

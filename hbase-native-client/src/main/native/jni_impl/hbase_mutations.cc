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
#line 19 "hbase_mutations.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_mutations.h"

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Sets the table name for the mutation.
 */
HBASE_API int32_t
hb_mutation_set_table(
    hb_mutation_t mutation,
    const char *table,
    size_t const table_len) {
  RETURN_IF_INVALID_PARAM((mutation == NULL),
      Msgs::ERR_MUTATION_NULL);
  RETURN_IF_INVALID_PARAM((table == NULL),
      Msgs::ERR_TBL_NULL);
  RETURN_IF_INVALID_PARAM((table_len <= 0),
      Msgs::ERR_TBL_LEN, table_len);

  return reinterpret_cast<Mutation *>(mutation)->
      SetTable(table, table_len).GetCode();
}

/**
 * Sets the row key for the mutation.
 */
HBASE_API int32_t
hb_mutation_set_row(
    hb_mutation_t mutation,
    const byte_t *rowkey,
    const size_t rowkey_len) {
  RETURN_IF_INVALID_PARAM((mutation == NULL),
      Msgs::ERR_MUTATION_NULL);
  RETURN_IF_INVALID_PARAM((rowkey == NULL),
      Msgs::ERR_KEY_NULL);
  RETURN_IF_INVALID_PARAM((rowkey_len <= 0),
      Msgs::ERR_KEY_LEN, rowkey_len);

  return reinterpret_cast<Mutation *>(mutation)->
      SetRowKey(rowkey, rowkey_len).GetCode();
}

/**
 * Optional (default = DURABILITY_USE_DEFAULT). Sets the durability
 * for this mutation. Note that RegionServers prior to 0.94.7 will
 * only honor SKIP_WAL.
 */
HBASE_API int32_t
hb_mutation_set_durability(
    hb_mutation_t mutation,
    const hb_durability_t durability) {
  RETURN_IF_INVALID_PARAM((mutation == NULL),
      Msgs::ERR_MUTATION_NULL);

  return reinterpret_cast<Mutation *>(mutation)->
      SetDurability(durability).GetCode();
}

/**
 * Sets whether or not this RPC can be buffered on the client side.
 *
 * Currently only puts and deletes can be buffered. Calling this for
 * any other mutation type will return EINVAL.
 *
 * The default is 'true'.
 */
HBASE_API int32_t
hb_mutation_set_bufferable(
    hb_mutation_t mutation,
    const bool bufferable) {
  RETURN_IF_INVALID_PARAM((mutation == NULL),
      Msgs::ERR_MUTATION_NULL);

  return reinterpret_cast<Mutation *>(mutation)->
      SetBufferable(bufferable).GetCode();
}

/**
 * Frees up any resource held by the mutation structure.
 */
HBASE_API int32_t
hb_mutation_destroy(hb_mutation_t mutation) {
  RETURN_IF_INVALID_PARAM((mutation == NULL),
      Msgs::ERR_MUTATION_NULL);

  delete reinterpret_cast<Mutation *>(mutation);
  return 0;
}

} /* extern "C" */

Status
Mutation::SetDurability(
    const hb_durability_t durability,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_MUTATION_PROXY,
      "setDurability", "(I)V", (int32_t)durability);
}

Status inline
Mutation::SetBufferable(
    const bool bufferable,
    JNIEnv *current_env) {
  return ENOTSUP;
}

Status
BufferableRpc::SetBufferable(
    const bool bufferable,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_MUTATION_PROXY,
      "setBufferable", "(Z)V", (jboolean)bufferable);
}

Status
Mutation::AddColumn(
    const byte_t *f,
    const size_t fLen,
    const byte_t *q,
    const size_t qLen,
    const byte_t *v,
    const size_t vLen,
    const int64_t ts,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);

  JniResult family = JniHelper::CreateJavaByteArray(env, f, 0, fLen);
  RETURN_IF_ERROR(family);

  jobject qualObj = NULL;
  if (q) {
    JniResult qualifier = JniHelper::CreateJavaByteArray(env, q, 0, qLen);
    RETURN_IF_ERROR(qualifier);
    qualObj = qualifier.GetObject();
  }

  jobject valueObj = NULL;
  if (v) {
    JniResult value = JniHelper::CreateJavaByteArray(env, v, 0, vLen);
    RETURN_IF_ERROR(value);
    valueObj = value.GetObject();
  }

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_MUTATION_PROXY,
      "addColumn", "([B[BJ[B)L"CLASS_MUTATION_PROXY";",
      family.GetObject(), qualObj, (jlong)ts, valueObj);
}

} /* namespace hbase */

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
#line 19 "hbase_row.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_row.h"

#include "hbase_macros.h"
#include "jnihelper.h"

namespace hbase {

Status
Row::Init(
    const char *CLASS_NAME,
    const byte_t *rowKey,
    const size_t rowKeyLen,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::CreateJavaByteArray(
      env, rowKey, 0, rowKeyLen);
  RETURN_IF_ERROR(result);
  result = JniHelper::NewObject(
      env, CLASS_NAME, "([B)V", result.GetObject());
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
  }
  return result;
}

Status
Row::SetRowKey(
    const byte_t *rowkey,
    const size_t rowkey_len,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult rowKey = JniHelper::CreateJavaByteArray(
      env, rowkey, 0, rowkey_len);
  RETURN_IF_ERROR(rowKey);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_ROW_PROXY,
      "setRow", "([B)V", rowKey.GetObject());
}

Status
Row::SetTable(
    const char *table,
    const size_t tableLen,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult tableName = JniHelper::CreateJavaByteArray(
      env, (const byte_t *)table, 0, tableLen);
  RETURN_IF_ERROR(tableName);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_ROW_PROXY,
      "setTable", "([B)V", tableName.GetObject());
}

Status
Row::SetTS(
    const int64_t ts,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_ROW_PROXY, "setTS", "(I)V", ts);
}

} /* namespace hbase */

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
#line 19 "hbase_scanner.cc" // ensures short filename in logs.

#include <jni.h>

#include "hbase_scanner.h"

#include "hbase_client.h"
#include "hbase_msgs.h"
#include "hbase_result.h"

#include "org_apache_hadoop_hbase_jni_CallbackHandlers.h"

namespace hbase {

extern "C" {

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    scanNextCallBack
 * Signature: (Ljava/lang/Throwable;JJ[Ljava/lang/Object;IJ)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_scanNextCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr, jlong cb,
    jlong scanner, jobjectArray results, jint num_results, jlong extra) {
  if (cb) {
    hb_result_t *rows = num_results ? new hb_result_t[num_results] : NULL;
    for (int i = 0; i < num_results; ++i) {
      jobject result = env->GetObjectArrayElement(results, i);
      rows[i] = Result::From(jthr, result, env);
    }
    hb_scanner_cb scanner_next_cb = (hb_scanner_cb)cb;
    scanner_next_cb(JniHelper::ErrorFromException(env, jthr),
                    (hb_scanner_t)scanner,
                    rows,
                    num_results,
                    (void*)extra);
    if (rows) {
      delete[] rows;
    }
  }
}

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    scannerCloseCallBack
 * Signature: (Ljava/lang/Throwable;JJJ)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_scannerCloseCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr,
    jlong cb, jlong scanner, jlong extra) {
  if (cb) {
    hb_scanner_destroy_cb close_cb = (hb_scanner_destroy_cb) cb;
    close_cb(JniHelper::ErrorFromException(env, jthr),
             (hb_scanner_t)scanner,
             (void*)extra);
  }
  delete reinterpret_cast<HScanner*>(scanner);
}

/**
 * Creates a client side row scanner. The returned scanner is not thread safe.
 * No RPC will be invoked until the call to fetch the next set of rows is made.
 * You can set the various attributes of this scanner until that point.
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_scanner_create(
    hb_client_t client,
    hb_scanner_t *scanner_ptr) {
  RETURN_IF_INVALID_PARAM((client == NULL),
      Msgs::ERR_CLIENT_NULL);
  RETURN_IF_INVALID_PARAM((scanner_ptr == NULL),
      Msgs::ERR_SCANNERPTR_NULL);

  HScanner *scanner = new HScanner();
  if (scanner == NULL) return ENOMEM;

  Status status = scanner->Init(
      reinterpret_cast<HBaseClient*>(client));
  if (status.ok()) {
    *scanner_ptr = reinterpret_cast<hb_scanner_t> (scanner);
  } else {
    *scanner_ptr = NULL;
    delete scanner_ptr;
  }

  return status.GetCode();
}

/**
 * Request the next set of results from the server. You can set the maximum
 * number of rows returned by this call using hb_scanner_set_num_max_rows().
 */
HBASE_API int32_t
hb_scanner_next(
    hb_scanner_t scanner,
    hb_scanner_cb cb,
    void *extra) {
  if (UNLIKELY((scanner == NULL))){
    HBASE_LOG_ERROR(Msgs::ERR_SCANNER_NULL);
    if (cb) {
      cb(EINVAL, scanner, NULL, 0, extra);
    }
    return 0;
  }

  return reinterpret_cast<HScanner*>(scanner)->
      NextRows(cb, extra).GetCode();
}

/**
 * Close the scanner releasing any local and server side resources held.
 * The call back is fired just before the scanner's memory is freed.
 */
HBASE_API int32_t
hb_scanner_destroy(
    hb_scanner_t scanner,
    hb_scanner_destroy_cb cb,
    void *extra) {
  if (UNLIKELY((scanner == NULL))){
    HBASE_LOG_ERROR(Msgs::ERR_SCANNER_NULL);
    if (cb) {
      cb(EINVAL, scanner, extra);
    }
    return 0;
  }

  return reinterpret_cast<HScanner*>(scanner)->
      Close(cb, extra).GetCode();
}

/**
 * Set the table name for the scanner
 */
HBASE_API int32_t
hb_scanner_set_table(
    hb_scanner_t scanner,
    const char *table,
    const size_t table_len) {
  RETURN_IF_INVALID_PARAM((scanner == NULL),
      Msgs::ERR_SCANNER_NULL);
  RETURN_IF_INVALID_PARAM((table == NULL),
      Msgs::ERR_TBL_NULL);
  RETURN_IF_INVALID_PARAM((table_len <= 0),
      Msgs::ERR_TBL_LEN, table_len);

  return reinterpret_cast<HScanner*>(scanner)->
      SetTable(table, table_len).GetCode();
}

/**
 * Set the name space for the scanner (0.96 and above)
 */
HBASE_API int32_t
hb_scanner_set_namespace(
    hb_scanner_t scanner,
    const char *name_space,
    const size_t name_space_length) {
  return ENOSYS;
}

/**
 * Specifies the start row key for this scanner (inclusive).
 */
HBASE_API int32_t
hb_scanner_set_start_row(
    hb_scanner_t scanner,
    const byte_t *start_row,
    const size_t start_row_len) {
  RETURN_IF_INVALID_PARAM((scanner == NULL),
      Msgs::ERR_SCANNER_NULL);
  RETURN_IF_INVALID_PARAM((start_row == NULL),
      Msgs::ERR_START_ROW_NULL);
  RETURN_IF_INVALID_PARAM((start_row_len <= 0),
      Msgs::ERR_START_ROWL_LEN, start_row_len);

  return reinterpret_cast<HScanner*>(scanner)->
      SetStartRow(start_row, start_row_len).GetCode();
}

/**
 * Specifies the end row key for this scanner (exclusive).
 */
HBASE_API int32_t
hb_scanner_set_end_row(
    hb_scanner_t scanner,
    const byte_t *end_row,
    const size_t end_row_len) {
  RETURN_IF_INVALID_PARAM((scanner == NULL),
      Msgs::ERR_SCANNER_NULL);
  RETURN_IF_INVALID_PARAM((end_row == NULL),
      Msgs::ERR_END_ROW_NULL);
  RETURN_IF_INVALID_PARAM((end_row_len <= 0),
      Msgs::ERR_END_ROWL_LEN, end_row_len);

  return reinterpret_cast<HScanner*>(scanner)->
      SetEndRow(end_row, end_row_len).GetCode();
}

/**
 * Sets the maximum versions of a column to fetch.
 */
HBASE_API int32_t
hb_scanner_set_num_versions(
    hb_scanner_t scanner,
    int8_t num_versions) {
  RETURN_IF_INVALID_PARAM((scanner == NULL),
      Msgs::ERR_SCANNER_NULL);
  RETURN_IF_INVALID_PARAM((num_versions <= 0),
      Msgs::ERR_NUM_VERSIONS, num_versions);

  return reinterpret_cast<HScanner*>(scanner)->
      SetNumVersions(num_versions).GetCode();
}


/**
 * Sets the maximum number of rows to scan per call to hb_scanner_next().
 */
HBASE_API int32_t
hb_scanner_set_num_max_rows(
    hb_scanner_t scanner,
    size_t cache_size) {
  RETURN_IF_INVALID_PARAM((scanner == NULL),
      Msgs::ERR_SCANNER_NULL);
  RETURN_IF_INVALID_PARAM((cache_size <= 0),
      Msgs::ERR_CACHE_SIZE, cache_size);

  return reinterpret_cast<HScanner*>(scanner)->
      SetMaxNumRows(cache_size).GetCode();
}

} /* extern "C" */

#define ERROR_IF_SCANNER_OPEN() \
    if (is_open_) { \
      HBASE_LOG_ERROR(Msgs::ERR_SCANNER_OPEN); \
      return Status::EBusy; \
    }

Status
HScanner::Init(
    HBaseClient *client,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);

  JniResult result = JniHelper::NewObject(
      env, CLASS_SCANNER_PROXY,
      JMETHOD1(JPARAM(CLASS_CLIENT_PROXY), "V"),
      client->JObject());
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
  }
  return result.GetCode();
}

Status
HScanner::NextRows(
    hb_scanner_cb cb,
    void *extra,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);

  is_open_ = true;
  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY, "next", "(JJJ)V",
      (jlong) cb, (jlong) this, (jlong) extra).GetCode();
}

Status
HScanner::Close(
    hb_scanner_destroy_cb cb,
    void *extra,
    JNIEnv *current_env) {
  if (jobject_ != NULL) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_SCANNER_PROXY, "close", "(JJJ)V",
        (jlong) cb, (jlong) this, (jlong) extra);
    env->DeleteGlobalRef(jobject_);
    jobject_ = NULL;
    return result.GetCode();
  }
  return Status::Success;
}

Status
HScanner::SetTable(
    const char *table,
    const size_t tableLen,
    JNIEnv *current_env) {
  ERROR_IF_SCANNER_OPEN();
  JNI_GET_ENV(current_env);

  JniResult tableName = JniHelper::CreateJavaByteArray(
      env, (const byte_t *)table, 0, tableLen);
  RETURN_IF_ERROR(tableName);

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY,
      "setTable", "([B)V", tableName.GetObject());
}

Status
HScanner::SetMaxNumRows(
    const size_t cache_size,
    JNIEnv *current_env) {
  ERROR_IF_SCANNER_OPEN();
  JNI_GET_ENV(current_env);

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY,
      "setMaxNumRows", "(I)V", (int32_t)cache_size);
}

Status
HScanner::SetNumVersions(
    const size_t num_versions,
    JNIEnv *current_env) {
  ERROR_IF_SCANNER_OPEN();
  JNI_GET_ENV(current_env);

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY,
      "setNumVersions", "(I)V", (int32_t)num_versions);
}

Status
HScanner::SetStartRow(
    const byte_t *start_row,
    const size_t start_row_len,
    JNIEnv *current_env) {
  ERROR_IF_SCANNER_OPEN();
  JNI_GET_ENV(current_env);

  JniResult startRow = JniHelper::CreateJavaByteArray(
      env, (const byte_t *)start_row, 0, start_row_len);
  RETURN_IF_ERROR(startRow);

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY,
      "setRow", "([B)V", startRow.GetObject());
}

Status
HScanner::SetEndRow(
    const byte_t *end_row,
    const size_t end_row_len,
    JNIEnv *current_env) {
  ERROR_IF_SCANNER_OPEN();
  JNI_GET_ENV(current_env);

  JniResult endRow = JniHelper::CreateJavaByteArray(
      env, (const byte_t *)end_row, 0, end_row_len);
  RETURN_IF_ERROR(endRow);

  return JniHelper::InvokeMethod(
      env, jobject_, CLASS_SCANNER_PROXY,
      "setEndRow", "([B)V", endRow.GetObject());
}

} /* namespace hbase */

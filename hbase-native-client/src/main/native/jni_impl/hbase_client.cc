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
#line 19 "hbase_client.cc" // ensures short filename in logs.

#include <jni.h>
#include <stdint.h>
#include <errno.h>

#include "hbase_client.h"

#include "hbase_consts.h"
#include "hbase_msgs.h"
#include "hbase_mutations.h"
#include "hbase_result.h"
#include "jnihelper.h"

#include "org_apache_hadoop_hbase_jni_CallbackHandlers.h"

namespace hbase {

extern "C" {

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    mutationCallBack
 * Signature: (Ljava/lang/Throwable;JJJLjava/lang/Object;J)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_mutationCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr,
    jlong cb, jlong client, jlong mutation,
    jobject result, jlong extra) {
  hb_mutation_cb mutation_cb = (hb_mutation_cb) cb;
  if (mutation_cb) {
    mutation_cb(JniHelper::ErrorFromException(env, jthr),
                (hb_client_t)client,
                (hb_mutation_t)mutation,
                Result::From(jthr, result, env),
                (void*)extra);
  }
}

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    getCallBack
 * Signature: (Ljava/lang/Throwable;JJJLjava/lang/Object;J)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_getCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr,
    jlong cb, jlong client, jlong get,
    jobject result, jlong extra) {
  hb_get_cb get_cb = (hb_get_cb) cb;
  if (get_cb) {
    get_cb(JniHelper::ErrorFromException(env, jthr),
           (hb_client_t)client,
           (hb_get_t)get,
           Result::From(jthr, result, env),
           (void*)extra);
  }
}

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    clientFlushCallBack
 * Signature: (Ljava/lang/Throwable;JJJ)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_clientFlushCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr,
    jlong cb, jlong client, jlong extra) {
  hb_client_flush_cb flush_cb = (hb_client_flush_cb) cb;
  if (flush_cb) {
    flush_cb(JniHelper::ErrorFromException(env, jthr),
             (hb_client_t)client,
             (void*)extra);
  }
}

/*
 * Class:     org_apache_hadoop_hbase_jni_CallbackHandlers
 * Method:    clientCloseCallBack
 * Signature: (Ljava/lang/Throwable;JJJ)V
 */
HBASE_API void JNICALL
Java_org_apache_hadoop_hbase_jni_CallbackHandlers_clientCloseCallBack(
    JNIEnv *env, jclass clazz, jthrowable jthr,
    jlong cb, jlong client, jlong extra) {
  hb_client_disconnection_cb close_cb = (hb_client_disconnection_cb) cb;
  if (close_cb) {
    close_cb(JniHelper::ErrorFromException(env, jthr),
             (hb_client_t)client,
             (void*)extra);
  }
  delete reinterpret_cast<HBaseClient*>(client);
}

/**
 * Initializes a handle to hb_client_t which can be passed to other HBase APIs.
 * You need to use this method only once per HBase cluster. The returned handle
 * is thread safe.
 *
 * @returns 0 on success, non-zero error code in case of failure.
 */
HBASE_API int32_t
hb_client_create(
    hb_connection_t connection,
    hb_client_t *c_ptr) {
  RETURN_IF_INVALID_PARAM((connection == NULL),
      Msgs::ERR_CONN_NULL);
  RETURN_IF_INVALID_PARAM((c_ptr == NULL),
      Msgs::ERR_CONNPTR_NULL);

  HBaseClient *client = new HBaseClient();
  if (!client) return ENOMEM;

  Status status = client->Init(
      reinterpret_cast<HBaseConfiguration*>(connection));
  if (status.ok()) {
    *c_ptr = reinterpret_cast<hb_client_t> (client);
  } else {
    *c_ptr = NULL;
    delete client;
  }
  return status.GetCode();
}

/**
 * Queue a mutation to go out. These mutations will not be performed atomically
 * and can be batched in a non-deterministic way on either the server or the
 * client side.
 *
 * Any buffer attached to the the mutation objects must not be altered until the
 * callback has been triggered.
 */
HBASE_API int32_t
hb_mutation_send(
    hb_client_t client,
    hb_mutation_t mutation,
    hb_mutation_cb cb,
    void *extra) {
  CALLBACK_IF_INVALID_PARAM((client == NULL),
      cb, client, mutation, NULL, extra, Msgs::ERR_CLIENT_NULL);
  CALLBACK_IF_INVALID_PARAM((mutation == NULL),
      cb, client, mutation, NULL, extra, Msgs::ERR_MUTATION_NULL);

  return reinterpret_cast<HBaseClient*>(client)->
      SendMutation(reinterpret_cast<Mutation *>(mutation),
          cb, extra).GetCode();
}

/**
 * Queues the get request. Callback specified by cb will be called on completion.
 * Any buffer(s) attached to the get object can be reclaimed only after the
 * callback is received.
 */
HBASE_API int32_t
hb_get_send(
    hb_client_t client,
    hb_get_t get,
    hb_get_cb cb,
    void *extra) {
  CALLBACK_IF_INVALID_PARAM((client == NULL),
      cb, client, get, NULL, extra, Msgs::ERR_CLIENT_NULL);
  CALLBACK_IF_INVALID_PARAM((get == NULL),
      cb, client, get, NULL, extra, Msgs::ERR_GET_NULL);

  return reinterpret_cast<HBaseClient*>(client)->
      SendGet(reinterpret_cast<Get *>(get),
          cb, extra).GetCode();
}

/**
 * Flushes any buffered client-side write operations to HBase.
 * The callback will be invoked when everything that was buffered at the time of
 * the call has been flushed.
 * Note that this doesn't guarantee that ALL outstanding RPCs have completed.
 * This doesn't introduce any sort of global sync point.  All it does really is
 * it sends any buffered RPCs to HBase.
 */
HBASE_API int32_t
hb_client_flush(
    hb_client_t client,
    hb_client_flush_cb cb,
    void *extra) {
  if (UNLIKELY((client == NULL))){
    HBASE_LOG_ERROR(Msgs::ERR_CLIENT_NULL);
    cb(EINVAL, client, extra);
    return 0;
  }
  return reinterpret_cast<HBaseClient*>(client)->
      Flush(cb, extra).GetCode();
}

/**
 * Cleans up hb_client_t handle and release any held resources.
 * The callback is called after the connections are closed, but just before the
 * client is freed.
 */
HBASE_API int32_t
hb_client_destroy(
    hb_client_t client,
    hb_client_disconnection_cb cb,
    void *extra) {
  if (UNLIKELY((client == NULL))){
    HBASE_LOG_ERROR(Msgs::ERR_CLIENT_NULL);
    cb(EINVAL, client, extra);
    return 0;
  }
  return reinterpret_cast<HBaseClient*>(client)->
      Close(cb, extra).GetCode();
}

} /* extern "C" */

HBaseClient::HBaseClient() {
  pthread_mutex_init(&client_mutex, NULL);
}

HBaseClient::~HBaseClient() {
  pthread_mutex_destroy(&client_mutex);
}

Status
HBaseClient::Init(
    HBaseConfiguration *conf,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::NewObject(
      env, CLASS_CLIENT_PROXY,
      JMETHOD1(JPARAM(HADOOP_CONF), "V"),
      conf->GetConf());
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
  }
  return result;
}

Status
HBaseClient::SendMutation(
    Mutation* mutation,
    hb_mutation_cb cb,
    void* extra,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_CLIENT_PROXY, "sendMutation",
      "(L"CLASS_MUTATION_PROXY";JJJJ)V", mutation->JObject(),
      (jlong) cb, (jlong) this, (jlong) mutation, (jlong) extra);
  return Status::Success;
}

Status
HBaseClient::SendGet(
    Get* get,
    hb_get_cb cb,
    void* extra,
    JNIEnv* current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_CLIENT_PROXY, "sendGet",
      "(L"CLASS_GET_PROXY";JJJJ)V", get->JObject(),
      (jlong) cb, (jlong) this, (jlong) get, (jlong) extra);
  return Status::Success;
}

Status
HBaseClient::Flush(
    hb_client_disconnection_cb cb,
    void* extra,
    JNIEnv* current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_CLIENT_PROXY, "flush", "(JJJ)V",
      (jlong) cb, (jlong) this, (jlong) extra);
  return Status::Success;
}

Status
HBaseClient::Close(
    hb_client_disconnection_cb cb,
    void *extra,
    JNIEnv *current_env) {
  pthread_mutex_lock(&client_mutex);
  if (jobject_ != NULL) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_CLIENT_PROXY, "close",
        "(JJJ)V", (jlong) cb, (jlong) this, (jlong) extra);
    env->DeleteGlobalRef(jobject_);
    jobject_ = NULL;
  }
  pthread_mutex_unlock(&client_mutex);
  return Status::Success;
}

} /* namespace hbase */

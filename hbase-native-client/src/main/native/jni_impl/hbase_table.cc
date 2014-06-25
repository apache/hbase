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
#line 19 "hbase_table.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_table.h"

#include "hbase_client.h"
#include "hbase_config.h"
#include "hbase_msgs.h"
#include "hbase_mutations.h"
#include "hbase_put.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

HBASE_API int32_t
hb_table_open(
    const hb_connection_t conn,
    const char *name_space,
    const char *tableName,
    hb_table_t *t_ptr) {
  RETURN_IF_INVALID_PARAM((conn == NULL),
      Msgs::ERR_CONN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);
  RETURN_IF_INVALID_PARAM((t_ptr == NULL),
      Msgs::ERR_HTBLPTR_NULL);

  if (name_space == NULL) {
    HTable *table = new HTable();
    Status status = table->Init(
        reinterpret_cast<HBaseConfiguration *>(conn), tableName);
    if (status.ok()) {
      *t_ptr = reinterpret_cast<hb_client_t> (table);
    } else {
      *t_ptr = NULL;
      delete table;
    }
    return status.GetCode();
  }

  // HBase 0.96 not implemented yet
  return ENOSYS;
}

HBASE_API int32_t
hb_table_close(hb_table_t t) {
  RETURN_IF_INVALID_PARAM((t == NULL),
      Msgs::ERR_HTBL_NULL);

  HTable *table = reinterpret_cast<HTable *>(t);
  Status status = table->Close();
  delete table;
  return status.GetCode();
}

HBASE_API int32_t
hb_table_send_mutations(
    hb_table_t t,
    hb_mutation_t mutations[],
    int32_t num_mutations) {
  RETURN_IF_INVALID_PARAM((t == NULL),
      Msgs::ERR_HTBL_NULL);
  RETURN_IF_INVALID_PARAM((mutations == NULL),
      Msgs::ERR_MUTATIONS_NULL);
  RETURN_IF_INVALID_PARAM((num_mutations <= 0),
      Msgs::ERR_NUM_MUTATIONS, num_mutations);

  return reinterpret_cast<HTable*>(t)->
      Batch(reinterpret_cast<Mutation **>(mutations),
          num_mutations).GetCode();
}

HBASE_API int32_t
hb_table_send_puts(
    hb_table_t t,
    hb_put_t puts[],
    int32_t num_puts) {
  RETURN_IF_INVALID_PARAM((t == NULL),
      Msgs::ERR_HTBL_NULL);
  RETURN_IF_INVALID_PARAM((puts == NULL),
      Msgs::ERR_PUTS_NULL);
  RETURN_IF_INVALID_PARAM((num_puts <= 0),
      Msgs::ERR_NUM_PUTS, num_puts);

  return reinterpret_cast<HTable*>(t)->
      SendPut(reinterpret_cast<Put **>(puts),
          num_puts).GetCode();
}

HBASE_API int32_t
hb_table_flush_puts(hb_table_t t) {
  RETURN_IF_INVALID_PARAM((t == NULL),
      Msgs::ERR_HTBL_NULL);

  return reinterpret_cast<HTable*>(t)->
      Flush().GetCode();
}

} /* extern "C" */

HTable::~HTable() {
  if (jobject_ != NULL) {
    Close();
  }
}

Status
HTable::Init(
    const HBaseConfiguration *conf,
    const char *tableName,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  jstring tableNameString = env->NewStringUTF(tableName);
  JniResult result = JniHelper::NewObject(
      env, CLASS_TABLE_PROXY,
      JMETHOD2(JPARAM(HADOOP_CONF), JPARAM(JAVA_STRING), "V"),
      conf->GetConf(), tableNameString);
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
    JniHelper::InvokeMethod(env, jobject_,
        CLASS_TABLE_PROXY, "setAutoFlush", "(Z)V", (jboolean)0);
  }

  return result;
}

Status
HTable::Close(JNIEnv *current_env) {
  if (jobject_ != NULL) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_TABLE_PROXY, "close", "()V");
    env->DeleteGlobalRef(jobject_);
    jobject_ = NULL;
  }
  return Status::Success;
}

Status
HTable::Batch(
    Mutation *mutations[],
    size_t num_mutations,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult list = JniHelper::CreateJniObjectList(env,
      reinterpret_cast<JniObject**>(mutations), num_mutations);
  RETURN_IF_ERROR(list);

  return JniHelper::InvokeMethod(env,
      jobject_, CLASS_TABLE_PROXY, "batch",
      JMETHOD1(JPARAM(JAVA_LIST), JARRPARAM(JAVA_OBJECT)),
      list.GetObject());
}

Status
HTable::SendPut(
    Put *puts[],
    size_t num_puts,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult list = JniHelper::CreateJniObjectList(
      env, reinterpret_cast<JniObject**>(puts), num_puts);
  RETURN_IF_ERROR(list);
  return JniHelper::InvokeMethod(env,
      jobject_, CLASS_TABLE_PROXY, "put",
      JMETHOD1(JPARAM(JAVA_LIST), "V"), list.GetObject());
}

Status
HTable::Flush(JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(env,
      jobject_, CLASS_TABLE_PROXY, "flushCommits", "()V");
}

} /* namespace hbase */

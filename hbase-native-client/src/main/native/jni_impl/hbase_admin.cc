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
#line 19 "hbase_admin.cc" // ensures short filename in logs.

#include <jni.h>
#include <stdint.h>
#include <errno.h>

#include "hbase_admin.h"

#include "hbase_client.h"
#include "hbase_config.h"
#include "hbase_consts.h"
#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Create a new hb_admin.
 * All fields are initialized to the defaults. If you want to set
 * connection or other properties, set those before calling any
 * RPC functions.
 */
HBASE_API int32_t
hb_admin_create(
    hb_connection_t conn,
    hb_admin_t *admin_ptr) {
  RETURN_IF_INVALID_PARAM((conn == NULL),
      Msgs::ERR_CONN_NULL);
  RETURN_IF_INVALID_PARAM((admin_ptr == NULL),
      Msgs::ERR_ADMIN_PTR_NULL);

  HBaseAdmin *admin = new HBaseAdmin();
  Status status = admin->Init(
      reinterpret_cast<HBaseConfiguration*>(conn));
  if (status.ok()) {
    *admin_ptr = reinterpret_cast<hb_client_t> (admin);
  } else {
    *admin_ptr = NULL;
    delete admin;
  }
  return status.GetCode();
}

/**
 * Disconnect the admin releasing any internal objects
 * or connections created in the background.
 */
HBASE_API int32_t
hb_admin_destroy(
    hb_admin_t a,
    hb_admin_disconnection_cb cb,
    void *extra) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);

  HBaseAdmin *admin = reinterpret_cast<HBaseAdmin*>(a);
  int32_t err = admin->Close().GetCode();
  if (cb != NULL) {
    cb(err, a, extra);
  }
  delete admin;
  return 0;
}

/**
 * Checks if a table exists.
 * @returns 0 on if the table exist, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_exists(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);

  return reinterpret_cast<HBaseAdmin *>(a)->
      TableExists(name_space, tableName).GetCode();
}

/**
 * Checks if a table is enabled.
 * @returns 0 on if the table enabled, HBASE_TABLE_DISABLED if the table
 * is disabled or an error code if an error occurs.
 */
HBASE_API int32_t
hb_admin_table_enabled(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);

  return reinterpret_cast<HBaseAdmin *>(a)->
      TableEnabled(name_space, tableName).GetCode();
}

/**
 * Enable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_enable(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);

  return reinterpret_cast<HBaseAdmin *>(a)->
      EnableTable(name_space, tableName).GetCode();
}

/**
 * Disable an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_disable(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);

  return reinterpret_cast<HBaseAdmin *>(a)->
      DisableTable(name_space, tableName).GetCode();
}

/**
 * Creates an HBase table.
 * @returns 0 on success, an error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_create(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName,
    const hb_columndesc families[],
    const size_t numFamilies) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);
  RETURN_IF_INVALID_PARAM((families == NULL),
      Msgs::ERR_FAMILIES_NULL);
  RETURN_IF_INVALID_PARAM((numFamilies <= 0),
      Msgs::ERR_NUM_FAMILIES, numFamilies);

  return reinterpret_cast<HBaseAdmin *>(a)->
      CreateTable(name_space, tableName,
          (const HColumnDescriptor**)families,
          numFamilies).GetCode();
}

/**
 * Deletes an HBase table, disables the table if not already disabled.
 * Returns 0 on success, and error code otherwise.
 */
HBASE_API int32_t
hb_admin_table_delete(
    const hb_admin_t a,
    const char *name_space,
    const char *tableName) {
  RETURN_IF_INVALID_PARAM((a == NULL),
      Msgs::ERR_ADMIN_NULL);
  RETURN_IF_INVALID_PARAM((tableName == NULL),
      Msgs::ERR_TBL_NAME_NULL);

  return reinterpret_cast<HBaseAdmin *>(a)->
      DeleteTable(name_space, tableName).GetCode();
}

} /* extern "C" */

static JniResult
CreateHTableDescriptor(
    JNIEnv *env,
    const char *tableName) {
  jstring tableNameString = env->NewStringUTF(tableName);
  return JniHelper::NewObject(env, HBASE_TBLDSC,
      JMETHOD1(JPARAM(JAVA_STRING), "V"),
      tableNameString);
}

HBaseAdmin::HBaseAdmin() {
  pthread_mutex_init(&admin_mutex, NULL);
}

HBaseAdmin::~HBaseAdmin() {
  pthread_mutex_destroy(&admin_mutex);
}

Status
HBaseAdmin::Init(
    HBaseConfiguration *conf,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::NewObject(
      env, HBASE_HADMIN, JMETHOD1(JPARAM(HADOOP_CONF), "V"),
      conf->GetConf());
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
  }
  return result;
}

Status
HBaseAdmin::Close(JNIEnv *current_env) {
  pthread_mutex_lock(&admin_mutex);
  if (jobject_ != NULL) {
    JNI_GET_ENV(current_env);
    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, HBASE_HADMIN, "close", "()V");
    env->DeleteGlobalRef(jobject_);
    jobject_ = NULL;
    return result;
  }
  pthread_mutex_unlock(&admin_mutex);
  return Status::Success;
}

Status
HBaseAdmin::TableExists(
    const char *name_space,
    const char *tableName,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  jstring tableNameString = env->NewStringUTF(tableName);
  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "tableExists",
      JMETHOD1(JPARAM(JAVA_STRING), "Z"), tableNameString);
  if (result.ok()) {
    return (result.GetValue().z != 0)
        ? Status::Success : Status::ENoEntry;
  }
  return result;
}

Status
HBaseAdmin::TableEnabled(
    const char *name_space,
    const char *tableName,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  jstring tableNameString = env->NewStringUTF(tableName);
  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "isTableEnabled",
      JMETHOD1(JPARAM(JAVA_STRING), "Z"), tableNameString);
  if (result.ok()) {
    return (result.GetValue().z != 0)
        ? Status::Success : Status::HBaseTableDisabled;
  }
  return result;
}

Status
HBaseAdmin::CreateTable(
    const char *name_space,
    const char *tableName,
    const HColumnDescriptor *families[],
    const size_t numFamilies,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  JniResult htd = CreateHTableDescriptor(env, tableName);
  if (!htd.ok()) return htd;
  for (size_t i = 0; i < numFamilies; ++i) {
    if (families[i] == NULL) {
      return Status::EInvalid;
    }
    RETURN_IF_ERROR(JniHelper::InvokeMethod(
        env, htd.GetObject(), HBASE_TBLDSC, "addFamily",
        JMETHOD1(JPARAM(HBASE_CLMDSC), "V"),
        families[i]->JObject()));
  }
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "createTable",
      JMETHOD1(JPARAM(HBASE_TBLDSC), "V"), htd.GetObject());
}

Status
HBaseAdmin::EnableTable(
    const char *name_space,
    const char *tableName,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  jstring tableNameString = env->NewStringUTF(tableName);
  Status status = JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "enableTable",
      JMETHOD1(JPARAM(JAVA_STRING), "V"), tableNameString);
  if (status.GetCode() == HBASE_TABLE_NOT_DISABLED) {
    HBASE_LOG_WARN(Msgs::ERR_TABLE_ALREADY_ENABLED, tableName);
    return Status::Success;
  }
  return status;
}

Status
HBaseAdmin::DisableTable(
    const char *name_space,
    const char *tableName,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  jstring tableNameString = env->NewStringUTF(tableName);
  Status status = JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "disableTable",
      JMETHOD1(JPARAM(JAVA_STRING), "V"), tableNameString);
  if (status.GetCode() == HBASE_TABLE_DISABLED) {
    HBASE_LOG_WARN(Msgs::ERR_TABLE_ALREADY_DISABLED, tableName);
    return Status::Success;
  }
  return status;
}

Status
HBaseAdmin::DeleteTable(
    const char *name_space,
    const char *tableName,
    JNIEnv *current_env) {
  // HBase 0.96 not implemented yet
  if (name_space != NULL) return Status::ENoSys;

  JNI_GET_ENV(current_env);
  Status status = DisableTable(name_space, tableName, env);
  if (!status.ok()
      && status.GetCode() != HBASE_TABLE_DISABLED) {
    return status;
  }
  jstring tableNameString = env->NewStringUTF(tableName);
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_HADMIN, "deleteTable",
      JMETHOD1(JPARAM(JAVA_STRING), "V"), tableNameString);
}

} /* namespace hbase */

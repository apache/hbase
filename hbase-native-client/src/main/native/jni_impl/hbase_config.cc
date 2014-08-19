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
#line 19 "hbase_config.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_config.h"

#include "hbase_consts.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {
/**
 * Creates an hb_connection_t instance and initializes its address into
 * the passed pointer.
 */
HBASE_API int32_t
hb_connection_create(
    const char *zookeeprEnsemble,
    const char *zookeeprBaseNode,
    hb_connection_t *c_ptr) {
  RETURN_IF_INVALID_PARAM((c_ptr == NULL),
      Msgs::ERR_CONNPTR_NULL);

  HBaseConfiguration *conf = new HBaseConfiguration();
  if (conf == NULL) return ENOMEM;

  Status status = conf->Init(
      zookeeprEnsemble, zookeeprBaseNode);
  if (status.ok()) {
    *c_ptr = reinterpret_cast<hb_connection_t> (conf);
  } else {
    *c_ptr = NULL;
    delete conf;
  }
  return status.GetCode();
}

HBASE_API int32_t
hb_connection_destroy(hb_connection_t conn) {
  RETURN_IF_INVALID_PARAM((conn == NULL),
      Msgs::ERR_CONN_NULL);
  delete reinterpret_cast<HBaseConfiguration*>(conn);
  return 0;
}

} // extern "C"

Status
HBaseConfiguration::Init(
    const char *zookeeprEnsemble,
    const char *zookeeprBaseNode,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);

  JniResult result = JniHelper::InvokeMethodS(
      env, HBASE_CONF, "create", JMETHOD1("", JPARAM(HADOOP_CONF)));
  RETURN_IF_ERROR(result);

  jobject_ = env->NewGlobalRef(result.GetObject());
  if (zookeeprEnsemble != NULL) {
    RETURN_IF_ERROR(
        SetProperty(HConstants::ZK_ENSEMBLE, zookeeprEnsemble, env));
    RETURN_IF_ERROR(
        SetProperty(HConstants::ZK_QUORUM, zookeeprEnsemble, env));
    std::string ensemble = zookeeprEnsemble;
    std::string clientPort = HConstants::ZK_DEFAULT_PORT;
    size_t portStart = ensemble.find(':', 0);
    if (portStart != std::string::npos) {
      size_t portEnd = ensemble.find(',', ++portStart);
      clientPort = ensemble.substr(portStart, (portEnd-portStart));
      for (size_t i = 0; i < clientPort.length(); ++i) {
        char digit = clientPort.at(i);
        if (digit < '0' || digit > '9') {
          HBASE_LOG_ERROR(Msgs::ZK_PORT_INVALID,
              clientPort.c_str(), zookeeprEnsemble);
          return Status::EInvalid;
        }
      }
    }
    RETURN_IF_ERROR(
        SetProperty(HConstants::ZK_CLIENT_PORT,clientPort.c_str(), env));
  }
  if (zookeeprBaseNode != NULL) {
    RETURN_IF_ERROR(
        SetProperty(HConstants::ZK_ROOT_NODE, zookeeprBaseNode, env));
  }
  return result;
}

HBaseConfiguration::~HBaseConfiguration() {
   if (jobject_ != NULL) {
     JNIEnv *env = JniHelper::GetJNIEnv();
     if (env != NULL) {
       env->DeleteGlobalRef(jobject_);
     }
   }
 }

Status
HBaseConfiguration::SetProperty(
    const char *propName,
    const char *propValue,
    JNIEnv *current_env) {
  if (propName != NULL) {
    JNI_GET_ENV(current_env);
    jstring name = env->NewStringUTF(propName);
    jstring value = env->NewStringUTF((propValue == NULL) ? "" : propValue);

    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, HBASE_CONF, "set",
        JMETHOD2(JPARAM(JAVA_STRING), JPARAM(JAVA_STRING), "V"),
        name, value);
    return result;
  }
  return Status::EInvalid;
}

} /* namespace hbase */

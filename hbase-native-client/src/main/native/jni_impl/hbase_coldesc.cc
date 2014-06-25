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
#line 19 "hbase_coldesc.cc" // ensures short filename in logs.

#include <string.h>
#include <jni.h>
#include <errno.h>

#include "hbase_coldesc.h"

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

HBASE_API int32_t
hb_coldesc_create(
    const byte_t *family,
    const size_t family_len,
    hb_columndesc *col_desc) {
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);

  HColumnDescriptor *hcd = new HColumnDescriptor();
  Status status = hcd->Init(family, family_len);
  if (UNLIKELY(!status.ok())) {
    *col_desc = NULL;
    delete hcd;
  } else {
    *col_desc = reinterpret_cast<hb_columndesc> (hcd);
  }
  return status.GetCode();
}

HBASE_API int32_t
hb_coldesc_destroy(hb_columndesc c) {
  RETURN_IF_INVALID_PARAM((c == NULL),
      Msgs::ERR_COLDESC_NULL);

  delete reinterpret_cast<HColumnDescriptor*>(c);
  return 0;
}

HBASE_API int32_t
hb_coldesc_set_maxversions(
    hb_columndesc c,
    int32_t max_versions) {
  RETURN_IF_INVALID_PARAM((c == NULL),
      Msgs::ERR_COLDESC_NULL);
  RETURN_IF_INVALID_PARAM((max_versions <= 0),
      Msgs::ERR_MAX_VERSIONS);

  return reinterpret_cast<HColumnDescriptor *>(c)->
      SetMaxVersions(max_versions).GetCode();
}

HBASE_API int32_t
hb_coldesc_set_minversions(
    hb_columndesc c,
    int32_t min_versions) {
  RETURN_IF_INVALID_PARAM((c == NULL),
      Msgs::ERR_COLDESC_NULL);
  RETURN_IF_INVALID_PARAM((min_versions < 0),
      Msgs::ERR_MIN_VERSIONS);

  return reinterpret_cast<HColumnDescriptor *>(c)->
      SetMinVersions(min_versions).GetCode();
}

HBASE_API int32_t
hb_coldesc_set_ttl(
    hb_columndesc c,
    int32_t ttl) {
  RETURN_IF_INVALID_PARAM((c == NULL),
      Msgs::ERR_COLDESC_NULL);
  RETURN_IF_INVALID_PARAM((ttl < 0), Msgs::ERR_TTL);

  return reinterpret_cast<HColumnDescriptor *>(c)->
      SetTimeToLive(ttl).GetCode();
}

HBASE_API int32_t
hb_coldesc_set_inmemory(
    hb_columndesc c,
    int32_t in_memory) {
  RETURN_IF_INVALID_PARAM((c == NULL),
      Msgs::ERR_COLDESC_NULL);

  return reinterpret_cast<HColumnDescriptor *>(c)->
      SetInMemory(in_memory).GetCode();
}

} /* extern "C" */

Status
HColumnDescriptor::Init(
    const byte_t *family,
    const size_t family_len,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  JniResult result = JniHelper::CreateJavaByteArray(
      env, family, 0, family_len);
  RETURN_IF_ERROR(result);

  result = JniHelper::NewObject(
      env, HBASE_CLMDSC, "([B)V", result.GetObject());
  if (result.ok()) {
    jobject_ = env->NewGlobalRef(result.GetObject());
  }
  return result;
}

HColumnDescriptor::~HColumnDescriptor() {
  Destroy();
}

Status
HColumnDescriptor::Destroy(JNIEnv *current_env) {
  if (jobject_ != NULL) {
    JNI_GET_ENV(current_env);
    env->DeleteGlobalRef(jobject_);
    jobject_= NULL;
  }
  return Status::Success;
}

Status
HColumnDescriptor::SetMaxVersions(
    int32_t maxVersions,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_CLMDSC, "setMaxVersions",
      "(I)"JPARAM(HBASE_CLMDSC), maxVersions);
}

Status
HColumnDescriptor::SetMinVersions(
    int32_t minVersions,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_CLMDSC, "setMinVersions",
      "(I)"JPARAM(HBASE_CLMDSC), minVersions);
}

Status
HColumnDescriptor::SetTimeToLive(
    int32_t ttl,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_CLMDSC, "setTimeToLive",
      "(I)"JPARAM(HBASE_CLMDSC), ttl);
}

Status
HColumnDescriptor::SetInMemory(
    bool inMemory,
    JNIEnv *current_env) {
  JNI_GET_ENV(current_env);
  return JniHelper::InvokeMethod(
      env, jobject_, HBASE_CLMDSC, "setInMemory",
      "(Z)"JPARAM(HBASE_CLMDSC), (inMemory ? 1 : 0));
}

} /* namespace hbase */

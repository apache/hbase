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
#ifndef HBASE_JNI_IMPL_COLDESC_H_
#define HBASE_JNI_IMPL_COLDESC_H_

#include <jni.h>

#include "jnihelper.h"

namespace hbase {

class HColumnDescriptor : public JniObject {
public:
  HColumnDescriptor() {}

  ~HColumnDescriptor();

  Status Init(const byte_t *family, const size_t family_len, JNIEnv *current_env=NULL);

  Status SetMaxVersions(int32_t maxVersions, JNIEnv *current_env=NULL);

  Status SetMinVersions(int32_t minVersions, JNIEnv *current_env=NULL);

  Status SetTimeToLive(int32_t ttl, JNIEnv *current_env=NULL);

  Status SetInMemory(bool inMemory, JNIEnv *current_env=NULL);

  friend class HBaseAdmin;

private:
  Status Destroy(JNIEnv *current_env=NULL);
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_COLDESC_H_ */

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
#ifndef HBASE_JNI_IMPL_ROW_H_
#define HBASE_JNI_IMPL_ROW_H_

#include <jni.h>

#include <hbase/types.h>

#include "hbase_status.h"
#include "jnihelper.h"

namespace hbase {

class Row : public JniObject {
public:
  Row() {}

  ~Row() {}

  Status Init(const char *CLASS_NAME,
      const byte_t *rowKey, const size_t rowKeyLen, JNIEnv *current_env=NULL);

  Status SetRowKey(const byte_t *rowKey, const size_t rowKeyLen, JNIEnv *current_env=NULL);

  Status SetTable(const char *tableName, const size_t tableNameLen, JNIEnv *current_env=NULL);

  Status SetTS(const int64_t ts, JNIEnv *current_env=NULL);

  friend class HTable;
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_ROW_H_ */

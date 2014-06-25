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
#ifndef HBASE_JNI_IMPL_MUTATIONS_H_
#define HBASE_JNI_IMPL_MUTATIONS_H_

#include <jni.h>

#include <hbase/types.h>

#include "hbase_row.h"
#include "hbase_status.h"
#include "jnihelper.h"

namespace hbase {

class Mutation : public Row {
public:
  Status SetDurability(const hb_durability_t durability, JNIEnv *current_env=NULL);

  Status AddColumn(const byte_t *family, const size_t family_len,
      const byte_t *qualifier, const size_t qualifierLen, const byte_t *value,
      const size_t valueLen, const int64_t ts=HBASE_LATEST_TIMESTAMP, JNIEnv *current_env=NULL);

  virtual Status SetBufferable(const bool bufferable, JNIEnv *current_env=NULL);

  friend class HTable;

protected:
  Mutation(bool isIncrement) : isIncrement_(isIncrement) {}

  bool isIncrement_;
};

class BufferableRpc : public Mutation {
protected:
  BufferableRpc() : Mutation(false) { }

public:
  Status SetBufferable(const bool bufferable, JNIEnv *current_env=NULL);
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_MUTATIONS_H_ */

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
#include <jni.h>
#include <errno.h>

#include <hbase/scanner.h>

#include "hbase_client.h"
#include "jnihelper.h"

#ifndef HBASE_JNI_IMPL_SCANNER_H_
#define HBASE_JNI_IMPL_SCANNER_H_

namespace hbase {

class HScanner : public JniObject {
public:
  HScanner() : is_open_(false) {}

  ~HScanner() {}

  Status Init(HBaseClient *client, JNIEnv *current_env=NULL);

  Status NextRows(hb_scanner_cb cb, void *extra, JNIEnv *current_env=NULL);

  Status Close(hb_scanner_destroy_cb cb, void *extra, JNIEnv *current_env=NULL);

  Status SetMaxNumRows(const size_t cache_size, JNIEnv *current_env=NULL);

  Status SetNumVersions(const size_t num_versions, JNIEnv *current_env=NULL);

  Status SetTable(const char *table, const size_t table_len, JNIEnv *current_env=NULL);

  Status SetStartRow(const byte_t *start_row, const size_t start_row_len, JNIEnv *current_env=NULL);

  Status SetEndRow(const byte_t *start_row, const size_t start_row_len, JNIEnv *current_env=NULL);

private:
  bool is_open_;
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_SCANNER_H_ */

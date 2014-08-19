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
#ifndef HBASE_JNI_IMPL_RESULT_H_
#define HBASE_JNI_IMPL_RESULT_H_

#include <jni.h>

#include <hbase/types.h>

#include "hbase_status.h"
#include "jnihelper.h"

namespace hbase {

class Result : public JniObject {
public:
  ~Result();

  Status Init(JNIEnv *current_env=NULL);

  Status GetTable(const char **table=NULL, size_t *tableLength=0, JNIEnv *current_env=NULL);

  Status GetRowKey(const byte_t **rowKey=NULL, size_t *rowKeyLength=0, JNIEnv *current_env=NULL);

  Status GetCellCount(size_t *countPtr=NULL, JNIEnv *current_env=NULL);

  Status GetCell(const byte_t *family, const size_t family_len, const byte_t *qualifier,
      const size_t qualifier_len, const hb_cell_t **cell_ptr, JNIEnv *current_env=NULL);

  Status GetCellAt(const size_t index, const hb_cell_t **cell_ptr, JNIEnv *current_env=NULL);

  Status GetCells(const hb_cell_t ***cell_ptr, size_t *num_cells, JNIEnv *current_env=NULL);

  static hb_result_t From(jthrowable jthr, jobject result, JNIEnv *env);

protected:
  Result(jobject resultProxy);

  void EnsureCells();

  static void FreeCell(hb_cell_t *cell_ptr);

private:
  char      *tableName_;
  size_t    tableNameLen_;

  byte_t    *rowKey_;
  size_t    rowKeyLen_;

  hb_cell_t **cells_;
  size_t    cellCount_;
};

} /* namespace hbase */

#endif /* HBASE_JNI_IMPL_RESULT_H_ */

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
#line 19 "hbase_result.cc" // ensures short filename in logs.

#include <jni.h>
#include <errno.h>

#include "hbase_macros.h"
#include "hbase_msgs.h"
#include "hbase_result.h"
#include "jnihelper.h"

namespace hbase {

extern "C" {

/**
 * Frees any resources held by the hb_result_t object.
 */
HBASE_API int32_t
hb_result_destroy(hb_result_t r) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);

  delete reinterpret_cast<Result *>(r);
  return 0;
}

/**
 * Returns the row key of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_key(
    const hb_result_t r,
    const byte_t **key_ptr,
    size_t *key_length_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((key_ptr == NULL),
      Msgs::ERR_TBLPTR_NULL);
  RETURN_IF_INVALID_PARAM((key_length_ptr == NULL),
      Msgs::ERR_KEY_LENPTR);

  return reinterpret_cast<Result *>(r)->
      GetRowKey(key_ptr, key_length_ptr).GetCode();
}

/**
 * Returns the table name of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_table(
    const hb_result_t r,
    const char **table_ptr,
    size_t *table_length_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((table_ptr == NULL),
      Msgs::ERR_TBLPTR_NULL);
  RETURN_IF_INVALID_PARAM((table_length_ptr == NULL),
      Msgs::ERR_TBL_LENPTR_NULL);

  return reinterpret_cast<Result *>(r)->
      GetTable(table_ptr, table_length_ptr).GetCode();
}

/**
 * HBase 0.96 or later.
 * Returns the namespace of this hb_result_t object.
 * This buffer is valid until hb_result_destroy() is called.
 * Callers should not modify this buffer.
 */
HBASE_API int32_t
hb_result_get_namespace(
    const hb_result_t result,
    const char **name_space,
    size_t *name_space_length_ptr) {
  return ENOSYS;
}

/**
 * Returns the total number of cells in this hb_result_t object.
 */
HBASE_API int32_t
hb_result_get_cell_count(
    const hb_result_t r,
    size_t *cell_count_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((cell_count_ptr == NULL),
      Msgs::ERR_CELL_COUNTPTR_NULL);

  return reinterpret_cast<Result*>(r)->
      GetCellCount(cell_count_ptr).GetCode();
}

/**
 * Returns the pointer to a constant hb_cell_t structure with the most recent
 * value of the given column. The buffers are valid until hb_result_destroy()
 * is called. Callers should not modify these buffers.
 *
 * @returns 0       if operation succeeds.
 * @returns ENOENT  if a matching cell is not found.
 */
HBASE_API int32_t
hb_result_get_cell(
    const hb_result_t r,
    const byte_t *family,
    const size_t family_len,
    const byte_t *qualifier,
    const size_t qualifier_len,
    const hb_cell_t **cell_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((family == NULL),
      Msgs::ERR_FAMILY_NULL);
  RETURN_IF_INVALID_PARAM((family_len <= 0),
      Msgs::ERR_FAMILY_LEN, family_len);
  RETURN_IF_INVALID_PARAM((qualifier == NULL),
      Msgs::ERR_QUAL_NULL);
  RETURN_IF_INVALID_PARAM((qualifier_len <= 0),
      Msgs::ERR_QUAL_LEN, qualifier_len);
  RETURN_IF_INVALID_PARAM((cell_ptr == NULL),
      Msgs::ERR_CELLPTR_NULL);

  return reinterpret_cast<Result*>(r)->
      GetCell(family, family_len, qualifier,
          qualifier_len, cell_ptr).GetCode();
}

/**
 * Returns the pointer to a constant hb_cell_t structure containing the cell
 * value at the given 0 based index of the result. The buffers are valid until
 * hb_result_destroy() is called. Callers should not modify these buffers.
 *
 * @returns 0       if operation succeeds.
 * @returns ERANGE  if the index is outside the bounds.
 */
HBASE_API int32_t
hb_result_get_cell_at(
    const hb_result_t r,
    const size_t index,
    const hb_cell_t **cell_ptr) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);
  RETURN_IF_INVALID_PARAM((cell_ptr == NULL),
      Msgs::ERR_CELLPTR_NULL);

  return reinterpret_cast<Result*>(r)->
      GetCellAt(index, cell_ptr).GetCode();
}

/**
 * Returns the array of pointers to constant hb_cell_t structures with the cells
 * of the result. The buffers are valid until hb_result_destroy() is called. The
 * variable pointed by num_cells_ptr is set to the number of cells in the result.
 *
 * Calling this function multiple times for the same hb_result_t may return
 * the same buffers. Callers should not modify these buffers.
 */
HBASE_API int32_t
hb_result_get_cells(
    const hb_result_t r,
    const hb_cell_t ***cells_ptr,
    size_t *num_cells) {
  RETURN_IF_INVALID_PARAM((r == NULL),
      Msgs::ERR_RESULT_NULL);

  return reinterpret_cast<Result *>(r)->
      GetCells(cells_ptr, num_cells).GetCode();
}

} /* extern "C" */

/**
 * Wraps a JNI object handle into @link Result class
 */
hb_result_t
Result::From(
    const jthrowable jthr,
    const jobject result,
    JNIEnv *env) {
  if (result) {
    // TODO: may be wrap exception if not null
    return (hb_result_t) new Result(env->NewGlobalRef(result));
  }
  return NULL;
}

void
Result::FreeCell(hb_cell_t *cell_ptr) {
  if (cell_ptr) {
    if (cell_ptr->family) {
      delete[] cell_ptr->family;
      cell_ptr->family = NULL;
    }
    if (cell_ptr->qualifier) {
      delete[] cell_ptr->qualifier;
      cell_ptr->qualifier = NULL;
    }
    if (cell_ptr->value) {
      delete[] cell_ptr->value;
      cell_ptr->value = NULL;
    }
  }
}

Result::Result(jobject resultProxy)
:   JniObject(resultProxy),
    tableName_(NULL),
    tableNameLen_(0),
    rowKey_(NULL),
    rowKeyLen_(0),
    cells_(NULL),
    cellCount_(0) {
  Init();
}

Result::~Result() {
  if (tableName_ != NULL) {
    delete[] tableName_;
    tableName_ = NULL;
  }

  if (rowKey_ != NULL) {
    delete[] rowKey_;
    rowKey_ = NULL;
  }

  if (cells_ != NULL) {
    for (size_t i = 0; i < cellCount_; ++i) {
      if (cells_[i]) {
        FreeCell(cells_[i]);
        delete cells_[i];
      }
    }
    delete[] cells_;
    cells_ = NULL;
  }
}

Status
Result::Init(JNIEnv* current_env) {
  JNI_GET_ENV(current_env);

  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getTable", "()[B");
  RETURN_IF_ERROR(result);
  RETURN_IF_ERROR(JniHelper::CreateByteArray(
      env, result.GetObject(),
      (byte_t **)&tableName_, &tableNameLen_));

  JniResult row = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getRowKey", "()[B");
  RETURN_IF_ERROR(row);
  RETURN_IF_ERROR(JniHelper::CreateByteArray(
      env, row.GetObject(), &rowKey_, &rowKeyLen_));

  JniResult count = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "getCellCount", "()I");
  cellCount_ = (count.ok() ? count.GetValue().i : -1);

  return Status::Success;
}

Status
Result::GetTable(
    const char **table,
    size_t *tableLength,
    JNIEnv *current_env) {
  if (table) {
    *table = tableName_;
    *tableLength = tableNameLen_;
  }
  return Status::Success;
}

Status
Result::GetRowKey(
    const byte_t **rowKey,
    size_t *rowKeyLen,
    JNIEnv *current_env) {
  if (rowKey) {
    *rowKey = rowKey_;
    *rowKeyLen = rowKeyLen_;
  }
  return Status::Success;
}

Status
Result::GetCellCount(
    size_t *count_ptr,
    JNIEnv *current_env) {
  if (count_ptr) {
    *count_ptr = cellCount_;
  }
  return Status::Success;
}

Status
Result::GetCell(
    const byte_t *f,
    const size_t f_len,
    const byte_t *q,
    const size_t q_len,
    const hb_cell_t **cell_ptr,
    JNIEnv *current_env) {
  if (!cellCount_) {
    return Status::ENoEntry;
  }
  JNI_GET_ENV(current_env);

  JniResult family = JniHelper::CreateJavaByteArray(env, f, 0, f_len);
  RETURN_IF_ERROR(family);

  JniResult qualifier = JniHelper::CreateJavaByteArray(env, q, 0, q_len);
  RETURN_IF_ERROR(qualifier);

  JniResult result = JniHelper::InvokeMethod(
      env, jobject_, CLASS_RESULT_PROXY, "indexOf",
      "([B[B)I", family.GetObject(), qualifier.GetObject());
  RETURN_IF_ERROR(result);

  int32_t cellIndex = result.GetValue().i;
  if (cellIndex < 0) {
    return Status::ENoEntry;
  }

  return GetCellAt(cellIndex, cell_ptr, env);
}

/**
 * Precondition: GetCellCount() and GetRowKey() has been called.
 */
Status
Result::GetCellAt(
    const size_t index,
    const hb_cell_t **cell_ptr,
    JNIEnv *current_env) {
  if (UNLIKELY(index < 0 || index >= cellCount_)) {
    return Status::ERange;
  }

  EnsureCells();
  if (!cells_[index]) {
    hb_cell_t *cell = cells_[index] = new hb_cell_t();
    cell->row = rowKey_;
    cell->row_len = rowKeyLen_;

    JNI_GET_ENV(current_env);

    JniResult result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getFamily", "(I)[B", index);
    RETURN_IF_ERROR(result);
    RETURN_IF_ERROR(JniHelper::CreateByteArray(
        env, result.GetObject(), &cell->family, &cell->family_len));

    result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getQualifier", "(I)[B", index);
    RETURN_IF_ERROR(result);
    RETURN_IF_ERROR(JniHelper::CreateByteArray(
        env, result.GetObject(), &cell->qualifier, &cell->qualifier_len));

    result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getValue", "(I)[B", index);
    RETURN_IF_ERROR(result);
    RETURN_IF_ERROR(JniHelper::CreateByteArray(
        env, result.GetObject(), &cell->value, &cell->value_len));

    result = JniHelper::InvokeMethod(
        env, jobject_, CLASS_RESULT_PROXY, "getTS", "(I)J", index);
    RETURN_IF_ERROR(result);
    cell->ts = result.GetValue().j;
  }

  if (cell_ptr) {
    *(const_cast<hb_cell_t **>(cell_ptr)) = cells_[index];
  }

  return Status::Success;
}

Status
Result::GetCells(
    const hb_cell_t ***cell_ptr,
    size_t *num_cells,
    JNIEnv *current_env) {
  Status status = Status::Success;
  if (cellCount_) {
    JNI_GET_ENV(current_env);
    EnsureCells();
    for (size_t i = 0; i < cellCount_; ++i) {
      if (!cells_[i]) {
        GetCellAt(i, NULL, env);
      }
    }
  }

  *(const_cast<hb_cell_t ***>(cell_ptr)) = cells_;
  *num_cells = cellCount_;
  return status;
}

inline void
Result::EnsureCells() {
  if (cellCount_ > 0 && cells_ == NULL) {
    cells_ = new hb_cell_t*[cellCount_]();
  }
}

} /* namespace hbase */

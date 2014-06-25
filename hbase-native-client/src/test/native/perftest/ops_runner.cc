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
#line 19 "ops_runner.cc" // ensures short filename in logs.

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <hbase/hbase.h>

#include "common_utils.h"
#include "test_types.h"
#include "ops_runner.h"

namespace hbase {
namespace test {

void
OpsRunner::GetCallback(
    int32_t err,
    hb_client_t client,
    hb_get_t get,
    hb_result_t result,
    void* extra) {
  RowSpec *rowSpec = reinterpret_cast<RowSpec*>(extra);
  OpsRunner *runner = dynamic_cast<OpsRunner *>(rowSpec->runner);

  runner->EndRpc(err, rowSpec->key, StatKeeper::OP_GET);

  if (runner->checkRead_) {
    size_t cellCount = 0;
    if (result) {
      hb_result_get_cell_count(result, &cellCount);
    }
    if (cellCount != 1) {
      HBASE_LOG_ERROR("Number of cells for row \'%.*s\' = %d.",
          rowSpec->key->length, rowSpec->key->buffer, cellCount);
    }
  }

  rowSpec->Destroy();
  hb_get_destroy(get);
  if (result) {
    hb_result_destroy(result);
  }
}

void
OpsRunner::PutCallback(
    int32_t err,
    hb_client_t client,
    hb_mutation_t mutation,
    hb_result_t result,
    void* extra) {
  RowSpec *rowSpec = reinterpret_cast<RowSpec*>(extra);
  OpsRunner *runner = dynamic_cast<OpsRunner *>(rowSpec->runner);

  runner->EndRpc(err, rowSpec->key, StatKeeper::OP_PUT);

  rowSpec->Destroy();
  hb_mutation_destroy(mutation);
  if (result) {
    hb_result_destroy(result);
  }
}

void
OpsRunner::BeginRpc() {
  if (paused_) {
    pthread_mutex_lock(&pauseMutex_);
    while(paused_) {
      pthread_cond_wait(&pauseCond_, &pauseMutex_);
    }
    pthread_mutex_unlock(&pauseMutex_);
  }
  semaphore_->Acquire();
}

void
OpsRunner::EndRpc(
    int32_t err,
    bytebuffer key,
    StatKeeper::OpType type)  {
  semaphore_->Release();
  statKeeper_->RpcComplete(err, type);
  const char *opName = StatKeeper::GetOpName(type);
  if (err == 0) {
    HBASE_LOG_TRACE("%s completed for row \'%.*s\'.",
        opName, key->length, key->buffer);
    Resume();
  } else {
    if (err == ENOBUFS) {
      Pause();
    } else {
      HBASE_LOG_ERROR("%s failed for row \'%.*s\', result = %d.",
          opName, key->length, key->buffer, err);
    }
  }
}

void
OpsRunner::Pause()  {
  if (!paused_) {
    pthread_mutex_lock(&pauseMutex_);
    if (!paused_) {
      HBASE_LOG_INFO("Pausing OpsRunner(0x%08x) operations.", Id());
      paused_ = true;
    }
    pthread_mutex_unlock(&pauseMutex_);
  }
}

void
OpsRunner::Resume() {
  if (paused_ && semaphore_->NumAcquired() == 0) {
    pthread_mutex_lock(&pauseMutex_);
    if (paused_) {
      HBASE_LOG_INFO("Resuming OpsRunner(0x%08x) operations.", Id());
      paused_ = false;
      pthread_cond_broadcast(&pauseCond_);
    }
    pthread_mutex_unlock(&pauseMutex_);
  }
}

void
OpsRunner::WaitForCompletion() {
  while(semaphore_->NumAcquired() > 0) {
    usleep(20000);
  }
}

void
OpsRunner::SendPut(uint64_t row) {
  hb_put_t put = NULL;
  RowSpec *rowSpec = new RowSpec();
  rowSpec->runner = this;
  rowSpec->key = generateRowKey(keyPrefix_, hashKeys_, row);
  hb_put_create(rowSpec->key->buffer, rowSpec->key->length, &put);
  hb_mutation_set_table(put, (const char *)table_->buffer, table_->length);
  hb_mutation_set_bufferable(put, bufferPuts_);
  hb_mutation_set_durability(put, (writeToWAL_ ? DURABILITY_USE_DEFAULT : DURABILITY_SKIP_WAL));

  cell_data_t *cell_data = new_cell_data();
  rowSpec->first_cell = cell_data;
  cell_data->value = bytebuffer_random(valueLen_);

  hb_cell_t *cell = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
  cell_data->hb_cell = cell;

  cell->row = rowSpec->key->buffer;
  cell->row_len = rowSpec->key->length;
  cell->family = family_->buffer;
  cell->family_len = family_->length;
  cell->qualifier = column_->buffer;
  cell->qualifier_len = column_->length;
  cell->value = cell_data->value->buffer;
  cell->value_len = cell_data->value->length;
  cell->ts = HBASE_LATEST_TIMESTAMP;

  hb_put_add_cell(put, cell);

  HBASE_LOG_TRACE("Sending row with row key : '%.*s'.", cell->row_len, cell->row);
  uint64_t startTime = currentTimeMicroSeconds();
  hb_mutation_send(client_, put, OpsRunner::PutCallback, rowSpec);
  uint64_t endTime = currentTimeMicroSeconds();
  statKeeper_->UpdateStats(1, endTime - startTime, StatKeeper::OP_PUT, true);
}

void
OpsRunner::SendGet(uint64_t row) {
  hb_get_t get = NULL;
  RowSpec *rowSpec = new RowSpec();
  rowSpec->runner = this;
  rowSpec->key = generateRowKey(keyPrefix_, hashKeys_, row);

  hb_get_create(rowSpec->key->buffer, rowSpec->key->length, &get);
  hb_get_set_table(get, (const char *)table_->buffer, table_->length);

  HBASE_LOG_TRACE("Sending row with row key : '%.*s'.",
      rowSpec->key->length, rowSpec->key->buffer);
  uint64_t startTime = currentTimeMicroSeconds();
  hb_get_send(client_, get, GetCallback, rowSpec);
  uint64_t endTime = currentTimeMicroSeconds();
  statKeeper_->UpdateStats(1, endTime - startTime, StatKeeper::OP_GET, false);
}

void*
OpsRunner::Run() {
  uint64_t endRow = startRow_ + numOps_;
  HBASE_LOG_INFO("Starting OpsRunner(0x%08x) for start row %"
      PRIu64 ", operation count %" PRIu64 ".", Id(), startRow_, numOps_);

  double rand_max = RAND_MAX;
  for (uint64_t row = startRow_; row < endRow; ++row) {
    BeginRpc(); // ensures that we have permit to send the rpc
    double p = rand()/rand_max;
    if (((p < putWeight_) && (putsSent_ < maxPuts_) && !paused_)
        || (getsSent_ >= maxGets_)) {
      putsSent_++;
      SendPut(row);
    } else {
      getsSent_++;
      SendGet(row);
    }
  }

  flush_client_and_wait(client_);
  HBASE_LOG_INFO("OpsRunner(0x%08x) waiting for operations to complete.", Id());
  WaitForCompletion();
  HBASE_LOG_INFO("OpsRunner(0x%08x) complete.", Id());
  return NULL;
}

} /* namespace test */
} /* namespace hbase */

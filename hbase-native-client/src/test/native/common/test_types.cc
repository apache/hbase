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
#line 19 "test_types.cc" // ensures short filename in logs.

#include <stdint.h>
#include <stdlib.h>

#include "byte_buffer.h"
#include "test_types.h"

namespace hbase {
namespace test {

cell_data_t*
new_cell_data() {
  cell_data_t *cell_data = (cell_data_t*) calloc(1, sizeof(cell_data_t));
  cell_data->next_cell = NULL;
  return cell_data;
}

TaskRunner::TaskRunner()
    : tid_(0), stop_(false) {
}

void
TaskRunner::Start() {
  pthread_create(&thread_, 0, ThreadFunction, this);
}

void
TaskRunner::Stop() {
  stop_ = true;
  pthread_join(thread_, NULL);
}

void*
TaskRunner::ThreadFunction(void* arg) {
  TaskRunner* runner = (TaskRunner*) (arg);
  runner->tid_ = (uint32_t) (pthread_self());
  return runner->Run();
}

Semaphore::Semaphore(uint32_t numPermits)
    : numPermits_(numPermits) {
  sem_init(&sem_, 0, numPermits);
}

Semaphore::~Semaphore() {
  sem_destroy(&sem_);
}

void
Semaphore::Acquire(uint32_t num) {
  for (uint32_t i = 0; i < num; ++i) {
    sem_wait(&sem_);
  }
}

void
Semaphore::Release(uint32_t num) {
  for (uint32_t i = 0; i < num; ++i) {
    sem_post(&sem_);
  }
}

uint32_t
Semaphore::NumAcquired() {
  int semVal = 0;
  sem_getvalue(&sem_, &semVal);
  return (numPermits_ - semVal);
}

uint32_t
Semaphore::Drain() {
  uint32_t permits = 0;
  while (sem_trywait(&sem_) == 0) {
    ++permits;
  }
  return permits;
}

RowSpec::RowSpec()
    : runner(NULL), key(NULL), first_cell(NULL) {
}

void
RowSpec::Destroy() {
  cell_data_t* cell = first_cell;
  while (cell) {
    bytebuffer_free(cell->value);
    free(cell->hb_cell);
    cell_data_t* cur_cell = cell;
    cell = cell->next_cell;
    free(cur_cell);
  }
  first_cell = NULL;
  if (key) {
    bytebuffer_free(key);
    key = NULL;
  }
  delete this;
}

} /* namespace test */
} /* namespace hbase */

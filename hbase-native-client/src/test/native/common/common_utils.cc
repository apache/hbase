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
#line 19 "common_utils.cc" // ensures short filename in logs.

#include <pthread.h>
#include <stdlib.h>

#include <hbase/hbase.h>

#include "common_utils.h"

namespace hbase {
namespace test {

/**
 * Client destroy synchronizer and callbacks
 */
static volatile bool client_destroyed = false;
static pthread_cond_t client_destroyed_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
client_disconnection_callback(
    int32_t err,
    hb_client_t client,
    void *extra) {
  HBASE_LOG_INFO("Received client disconnection callback.");
  pthread_mutex_lock(&client_destroyed_mutex);
  client_destroyed = true;
  pthread_cond_signal(&client_destroyed_cv);
  pthread_mutex_unlock(&client_destroyed_mutex);
}

static void
wait_client_disconnection() {
  HBASE_LOG_INFO("Waiting for client to disconnect.");
  pthread_mutex_lock(&client_destroyed_mutex);
  while (!client_destroyed) {
    pthread_cond_wait(&client_destroyed_cv, &client_destroyed_mutex);
  }
  pthread_mutex_unlock(&client_destroyed_mutex);
  HBASE_LOG_INFO("Client disconnected.");
}

/**
 * Flush synchronizer and callback
 */
static volatile bool g_flushDone = false;
static pthread_cond_t flush_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t flush_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
client_flush_callback(
    int32_t err,
    hb_client_t client,
    void *extra) {
  HBASE_LOG_TRACE("Received client flush callback.");
  pthread_mutex_lock(&flush_mutex);
  g_flushDone = true;
  pthread_cond_signal(&flush_cv);
  pthread_mutex_unlock(&flush_mutex);
}

static void
wait_for_flush() {
  HBASE_LOG_TRACE("Waiting for flush to complete.");
  pthread_mutex_lock(&flush_mutex);
  while (!g_flushDone) {
    pthread_cond_wait(&flush_cv, &flush_mutex);
  }
  pthread_mutex_unlock(&flush_mutex);
  HBASE_LOG_TRACE("Flush completed.");
}

void
flush_client_and_wait(hb_client_t client) {
  g_flushDone = false;
  hb_client_flush(client, client_flush_callback, NULL);
  wait_for_flush();
}

void
disconnect_client_and_wait(hb_client_t client) {
  HBASE_LOG_INFO("Disconnecting client.");
  hb_client_destroy(client, client_disconnection_callback, NULL);
  wait_client_disconnection();
}

} /* namespace test */
} /* namespace hbase */

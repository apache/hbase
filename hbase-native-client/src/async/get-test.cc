/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <pthread.h>

#include "gtest/gtest.h"
#include "async/hbase_get.h"
#include "async/hbase_client.h"


pthread_cond_t cv;
pthread_mutex_t mutex;

TEST(GetTest, TestPut) {
  char tn[] = "T1";
  hb_byte_t row[] = "ROW";

  hb_client_t client = NULL;
  hb_get_t get = NULL;
  int32_t s1 = -1;

  pthread_cond_init(&cv, NULL);
  pthread_mutex_init(&mutex, NULL);

  s1 = hb_client_create(&client, NULL);
  EXPECT_EQ(0, s1);

  s1 = hb_get_create(&get);
  EXPECT_EQ(0, s1);

  hb_get_set_table(get, tn, 2);
  hb_get_set_row(get, row, 3);

  /*
   * TODO:
   * This is currently a NO-OP as there is no CB.
   */
  hb_get_send(client, get, NULL, NULL);

  hb_client_destroy(client, NULL, NULL);

  EXPECT_EQ(0, s1);
}

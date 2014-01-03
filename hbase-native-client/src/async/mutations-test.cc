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
#include "async/hbase_mutations.h"
#include "async/hbase_client.h"

pthread_cond_t cv;
pthread_mutex_t mutex;

bool sent = false;

TEST(ClientTest, EasyTest) {
    EXPECT_EQ(1, 1);
}

void mutate_cb(int32_t status,
    hb_client_t client, hb_mutation_t mutation,
    hb_result_t result, void * extra) {

  // Test Stuff.
  EXPECT_EQ(status, 0);
  EXPECT_TRUE(client != NULL);
  EXPECT_TRUE(mutation != NULL);

  pthread_mutex_lock(&mutex);
  sent = true;
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&mutex);
}

void wait_send() {
  pthread_mutex_lock(&mutex);
  while (!sent) {
    pthread_cond_wait(&cv, &mutex);
  }
  pthread_mutex_unlock(&mutex);
}

TEST(MutationTest, TestPut) {
  char tn[] = "T1";
  hb_byte_t row[] = "ROW";
  char fam[] = "D";
  hb_byte_t qual[] = "QUAL";
  hb_byte_t data[] = "Z";

  hb_client_t client = NULL;
  hb_put_t put = NULL;
  hb_cell_t cell;

  cell.family = fam;
  cell.family_length = 1;

  cell.qual = qual;
  cell.qual_length = 4;

  cell.value = data;
  cell.value_length = 1;

  int32_t status = -1;

  status = hb_client_create(&client, NULL);
  EXPECT_EQ(0, status);

  hb_put_create(&put);
  hb_mutation_set_table((hb_mutation_t) put, tn, 2);
  hb_mutation_set_row((hb_mutation_t) put, row, 3);
  hb_put_add_cell(put, &cell);

  pthread_cond_init(&cv, NULL);
  pthread_mutex_init(&mutex, NULL);

  status = hb_mutation_send(client, (hb_mutation_t) put, &mutate_cb, NULL);
  EXPECT_EQ(0, status);

  // Now wait a while for things to send.
  wait_send();
  EXPECT_EQ(true, sent);

  hb_mutation_destroy((hb_mutation_t *) put);
  hb_client_destroy(client, NULL, NULL);

  EXPECT_EQ(0, status);
}

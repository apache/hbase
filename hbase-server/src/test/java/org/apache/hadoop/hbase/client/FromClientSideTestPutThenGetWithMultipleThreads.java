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
 */
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromClientSideTestPutThenGetWithMultipleThreads extends FromClientSideTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(FromClientSideTestPutThenGetWithMultipleThreads.class);

  protected FromClientSideTestPutThenGetWithMultipleThreads(
    Class<? extends ConnectionRegistry> registryImpl, int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testPutThenGetWithMultipleThreads() throws Exception {
    final int numThreads = 20;
    final int numRounds = 10;
    for (int round = 0; round < numRounds; round++) {
      ArrayList<Thread> threads = new ArrayList<>(numThreads);
      final AtomicInteger successCnt = new AtomicInteger(0);
      TEST_UTIL.createTable(tableName, FAMILY);
      TEST_UTIL.waitTableAvailable(tableName, 10_000);
      try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
        for (int i = 0; i < numThreads; i++) {
          final int index = i;
          Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
              final byte[] row = Bytes.toBytes("row-" + index);
              final byte[] value = Bytes.toBytes("v" + index);
              try {
                Put put = new Put(row);
                put.addColumn(FAMILY, QUALIFIER, value);
                ht.put(put);
                Get get = new Get(row);
                Result result = ht.get(get);
                byte[] returnedValue = result.getValue(FAMILY, QUALIFIER);
                if (Bytes.equals(value, returnedValue)) {
                  successCnt.getAndIncrement();
                } else {
                  LOG.error("Should be equal but not, original value: " + Bytes.toString(value)
                    + ", returned value: "
                    + (returnedValue == null ? "null" : Bytes.toString(returnedValue)));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          });
          threads.add(t);
        }
        for (Thread t : threads) {
          t.start();
        }
        for (Thread t : threads) {
          t.join();
        }
        assertEquals(numThreads, successCnt.get(), "Not equal in round " + round);
      } finally {
        TEST_UTIL.deleteTable(tableName);
      }
    }
  }
}

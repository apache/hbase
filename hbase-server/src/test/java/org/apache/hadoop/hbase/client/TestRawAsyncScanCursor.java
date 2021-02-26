/**
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestRawAsyncScanCursor extends AbstractTestScanCursor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRawAsyncScanCursor.class);

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractTestScanCursor.setUpBeforeClass();
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  public static void tearDownAfterClass() throws Exception {
    if (CONN != null) {
      CONN.close();
    }
    AbstractTestScanCursor.tearDownAfterClass();
  }

  private void doTest(boolean reversed)
      throws InterruptedException, ExecutionException, IOException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    AsyncTable<AdvancedScanResultConsumer> table = CONN.getTable(TABLE_NAME);
    table.scan(reversed ? createReversedScanWithSparseFilter() : createScanWithSparseFilter(),
      new AdvancedScanResultConsumer() {

        private int count;

        @Override
        public void onHeartbeat(ScanController controller) {
          int row = count / NUM_FAMILIES / NUM_QUALIFIERS;
          if (reversed) {
            row = NUM_ROWS - 1 - row;
          }
          try {
            assertArrayEquals(ROWS[row], controller.cursor().get().getRow());
            count++;
          } catch (Throwable e) {
            future.completeExceptionally(e);
            throw e;
          }
        }

        @Override
        public void onNext(Result[] results, ScanController controller) {
          try {
            assertEquals(1, results.length);
            assertEquals(NUM_ROWS - 1, count / NUM_FAMILIES / NUM_QUALIFIERS);
            // we will always provide a scan cursor if time limit is reached.
            assertTrue(controller.cursor().isPresent());
            assertArrayEquals(ROWS[reversed ? 0 : NUM_ROWS - 1],
              controller.cursor().get().getRow());
            assertArrayEquals(ROWS[reversed ? 0 : NUM_ROWS - 1], results[0].getRow());
            count++;
          } catch (Throwable e) {
            future.completeExceptionally(e);
            throw e;
          }
        }

        @Override
        public void onError(Throwable error) {
          future.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
          future.complete(null);
        }
      });
    future.get();
  }

  @Test
  public void testHeartbeatWithSparseFilter()
      throws IOException, InterruptedException, ExecutionException {
    doTest(false);
  }

  @Test
  public void testHeartbeatWithSparseFilterReversed()
      throws IOException, InterruptedException, ExecutionException {
    doTest(true);
  }

  @Test
  public void testSizeLimit() throws InterruptedException, ExecutionException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    AsyncTable<AdvancedScanResultConsumer> table = CONN.getTable(TABLE_NAME);
    table.scan(createScanWithSizeLimit(), new AdvancedScanResultConsumer() {

      private int count;

      @Override
      public void onHeartbeat(ScanController controller) {
        try {
          assertArrayEquals(ROWS[count / NUM_FAMILIES / NUM_QUALIFIERS],
            controller.cursor().get().getRow());
          count++;
        } catch (Throwable e) {
          future.completeExceptionally(e);
          throw e;
        }
      }

      @Override
      public void onNext(Result[] results, ScanController controller) {
        try {
          assertFalse(controller.cursor().isPresent());
          assertEquals(1, results.length);
          assertArrayEquals(ROWS[count / NUM_FAMILIES / NUM_QUALIFIERS], results[0].getRow());
          count++;
        } catch (Throwable e) {
          future.completeExceptionally(e);
          throw e;
        }
      }

      @Override
      public void onError(Throwable error) {
        future.completeExceptionally(error);
      }

      @Override
      public void onComplete() {
        future.complete(null);
      }
    });
    future.get();
  }
}

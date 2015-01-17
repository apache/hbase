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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDelayingRunner {

  private static final TableName DUMMY_TABLE =
      TableName.valueOf("DUMMY_TABLE");
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static HRegionInfo hri1 =
      new HRegionInfo(DUMMY_TABLE, DUMMY_BYTES_1, DUMMY_BYTES_2, false, 1);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testDelayingRunner() throws Exception{
    MultiAction<Row> ma = new MultiAction<Row>();
    ma.add(hri1.getRegionName(), new Action<Row>(new Put(DUMMY_BYTES_1), 0));
    final AtomicLong endTime = new AtomicLong();
    final long sleepTime = 1000;
    DelayingRunner runner = new DelayingRunner(sleepTime, ma.actions.entrySet().iterator().next());
    runner.setRunner(new Runnable() {
      @Override
      public void run() {
        endTime.set(EnvironmentEdgeManager.currentTime());
      }      
    });
    long startTime = EnvironmentEdgeManager.currentTime();
    runner.run();
    long delay = endTime.get() - startTime;
    assertTrue("DelayingRunner did not delay long enough", delay >= sleepTime);
    assertFalse("DelayingRunner delayed too long", delay > sleepTime + sleepTime*0.2);
  }

}

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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Testcase for HBASE-21503.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestRaceBetweenGetWALAndGetWALs {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRaceBetweenGetWALAndGetWALs.class);

  private static Future<List<WAL>> GET_WALS_FUTURE;

  private static final class FSWALProvider extends AbstractFSWALProvider<AbstractFSWAL<?>> {

    @Override
    protected AbstractFSWAL<?> createWAL() throws IOException {
      // just like what may do in the WALListeners, schedule an asynchronous task to call the
      // getWALs method.
      GET_WALS_FUTURE = ForkJoinPool.commonPool().submit(this::getWALs);
      // sleep a while to make the getWALs arrive before we return
      Threads.sleep(2000);
      return Mockito.mock(AbstractFSWAL.class);
    }

    @Override
    protected void doInit(Configuration conf) throws IOException {
    }
  }

  @Test
  public void testRace() throws IOException, InterruptedException, ExecutionException {
    FSWALProvider p = new FSWALProvider();
    WAL wal = p.getWAL(null);
    assertNotNull(GET_WALS_FUTURE);
    List<WAL> wals = GET_WALS_FUTURE.get();
    assertSame(wal, Iterables.getOnlyElement(wals));
  }
}

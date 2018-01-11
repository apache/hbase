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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Based class for testing timeout logic for {@link ConnectionImplementation}.
 */
public abstract class AbstractTestCITimeout {

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static final byte[] FAM_NAM = Bytes.toBytes("f");

  @Rule
  public final TestName name = new TestName();

  /**
   * This copro sleeps 20 second. The first call it fails. The second time, it works.
   */
  public static class SleepAndFailFirstTime implements RegionCoprocessor, RegionObserver {
    static final AtomicLong ct = new AtomicLong(0);
    static final String SLEEP_TIME_CONF_KEY = "hbase.coprocessor.SleepAndFailFirstTime.sleepTime";
    static final long DEFAULT_SLEEP_TIME = 20000;
    static final AtomicLong sleepTime = new AtomicLong(DEFAULT_SLEEP_TIME);

    public SleepAndFailFirstTime() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
      RegionCoprocessorEnvironment env = c.getEnvironment();
      Configuration conf = env.getConfiguration();
      sleepTime.set(conf.getLong(SLEEP_TIME_CONF_KEY, DEFAULT_SLEEP_TIME));
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get,
        final List<Cell> results) throws IOException {
      Threads.sleep(sleepTime.get());
      if (ct.incrementAndGet() == 1) {
        throw new IOException("first call I fail");
      }
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(sleepTime.get());
      if (ct.incrementAndGet() == 1) {
        throw new IOException("first call I fail");
      }
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(sleepTime.get());
      if (ct.incrementAndGet() == 1) {
        throw new IOException("first call I fail");
      }
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      Threads.sleep(sleepTime.get());
      if (ct.incrementAndGet() == 1) {
        throw new IOException("first call I fail");
      }
      return null;
    }

  }

  public static class SleepCoprocessor implements RegionCoprocessor, RegionObserver {
    public static final int SLEEP_TIME = 5000;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get,
        final List<Cell> results) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      Threads.sleep(SLEEP_TIME);
      return null;
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      Threads.sleep(SLEEP_TIME);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    // Up the handlers; this test needs more than usual.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 3);
    TEST_UTIL.startMiniCluster(2);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}

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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;

/**
 * Tests bulk loading of HFiles with old secure Endpoint client for backward compatibility. Will be
 * removed when old non-secure client for backward compatibility is not supported.
 */
@RunWith(Parameterized.class)
@Category({RegionServerTests.class, LargeTests.class})
@Ignore // BROKEN. FIX OR REMOVE.
public class TestHRegionServerBulkLoadWithOldSecureEndpoint extends TestHRegionServerBulkLoad {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionServerBulkLoadWithOldSecureEndpoint.class);

  public TestHRegionServerBulkLoadWithOldSecureEndpoint(int duration) {
    super(duration);
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHRegionServerBulkLoadWithOldSecureEndpoint.class);

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    conf.setInt("hbase.rpc.timeout", 10 * 1000);
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
  }

  public static class AtomicHFileLoader extends RepeatingTestThread {
    final AtomicLong numBulkLoads = new AtomicLong();
    final AtomicLong numCompactions = new AtomicLong();
    private TableName tableName;

    public AtomicHFileLoader(TableName tableName, TestContext ctx, byte[][] targetFamilies)
            throws IOException {
      super(ctx);
      this.tableName = tableName;
    }

    public void doAnAction() throws Exception {
      long iteration = numBulkLoads.getAndIncrement();
      Path dir =  UTIL.getDataTestDirOnTestFS(String.format("bulkLoad_%08d",
          iteration));

      // create HFiles for different column families
      FileSystem fs = UTIL.getTestFileSystem();
      byte[] val = Bytes.toBytes(String.format("%010d", iteration));
      final List<Pair<byte[], String>> famPaths = new ArrayList<>(NUM_CFS);
      for (int i = 0; i < NUM_CFS; i++) {
        Path hfile = new Path(dir, family(i));
        byte[] fam = Bytes.toBytes(family(i));
        createHFile(fs, hfile, fam, QUAL, val, 1000);
        famPaths.add(new Pair<>(fam, hfile.toString()));
      }

      // bulk load HFiles
      final ClusterConnection conn = (ClusterConnection) UTIL.getAdmin().getConnection();
      Table table = conn.getTable(tableName);
      final String bulkToken = new SecureBulkLoadEndpointClient(table).prepareBulkLoad(tableName);
      RpcControllerFactory rpcControllerFactory = new RpcControllerFactory(UTIL.getConfiguration());
      ClientServiceCallable<Void> callable =
        new ClientServiceCallable<Void>(conn, tableName, Bytes.toBytes("aaa"),
            rpcControllerFactory.newController(), HConstants.PRIORITY_UNSET) {
          @Override
          protected Void rpcCall() throws Exception {
            LOG.debug("Going to connect to server " + getLocation() + " for row " +
                Bytes.toStringBinary(getRow()));
            try (Table table = conn.getTable(getTableName())) {
              boolean loaded = new SecureBulkLoadEndpointClient(table).bulkLoadHFiles(famPaths,
                  null, bulkToken, getLocation().getRegionInfo().getStartKey());
            }
            return null;
          }
        };
      RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(conf);
      RpcRetryingCaller<Void> caller = factory.<Void> newCaller();
      caller.callWithRetries(callable, Integer.MAX_VALUE);

      // Periodically do compaction to reduce the number of open file handles.
      if (numBulkLoads.get() % 5 == 0) {
        // 5 * 50 = 250 open file handles!
        callable = new ClientServiceCallable<Void>(conn, tableName, Bytes.toBytes("aaa"),
            rpcControllerFactory.newController(), HConstants.PRIORITY_UNSET) {
          @Override
          protected Void rpcCall() throws Exception {
            LOG.debug("compacting " + getLocation() + " for row "
                + Bytes.toStringBinary(getRow()));
            AdminProtos.AdminService.BlockingInterface server =
              conn.getAdmin(getLocation().getServerName());
            CompactRegionRequest request =
              RequestConverter.buildCompactRegionRequest(
                getLocation().getRegionInfo().getRegionName(), true, null);
            server.compactRegion(null, request);
            numCompactions.incrementAndGet();
            return null;
          }
        };
        caller.callWithRetries(callable, Integer.MAX_VALUE);
      }
    }
  }

  void runAtomicBulkloadTest(TableName tableName, int millisToRun, int numScanners)
          throws Exception {
    setupTable(tableName, 10);

    TestContext ctx = new TestContext(UTIL.getConfiguration());

    AtomicHFileLoader loader = new AtomicHFileLoader(tableName, ctx, null);
    ctx.addThread(loader);

    List<AtomicScanReader> scanners = Lists.newArrayList();
    for (int i = 0; i < numScanners; i++) {
      AtomicScanReader scanner = new AtomicScanReader(tableName, ctx, families);
      scanners.add(scanner);
      ctx.addThread(scanner);
    }

    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();

    LOG.info("Loaders:");
    LOG.info("  loaded " + loader.numBulkLoads.get());
    LOG.info("  compations " + loader.numCompactions.get());

    LOG.info("Scanners:");
    for (AtomicScanReader scanner : scanners) {
      LOG.info("  scanned " + scanner.numScans.get());
      LOG.info("  verified " + scanner.numRowsScanned.get() + " rows");
    }
  }
}

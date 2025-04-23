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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.quotas.TestNoopOperationQuota;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@Category({ MediumTests.class, ClientTests.class })
public class TestScannerLeaseCount {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScannerLeaseCount.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("ScannerLeaseCount");
  private static final byte[] FAM = Bytes.toBytes("Fam");
  private static final String SCAN_IDENTIFIER_NAME = "_scan_id_";
  private static final byte[] SCAN_IDENTIFIER = Bytes.toBytes("_scan_id_");
  private static final Scan SCAN = new Scan().setAttribute(SCAN_IDENTIFIER_NAME, SCAN_IDENTIFIER);

  private static volatile boolean SHOULD_THROW = false;
  private static final AtomicBoolean EXCEPTION_THROWN = new AtomicBoolean(false);
  private static final AtomicBoolean SCAN_SEEN = new AtomicBoolean(false);

  private static Connection CONN;
  private static Table TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().rsClass(MockedQuotaManagerRegionServer.class).build();
    UTIL.startMiniCluster(option);
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAM)).build());
    Configuration conf = new Configuration(UTIL.getConfiguration());
    CONN = ConnectionFactory.createConnection(conf);
    TABLE = CONN.getTable(TABLE_NAME);
    UTIL.loadTable(TABLE, FAM);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      TABLE.close();
    } catch (Exception ignore) {
    }
    try {
      CONN.close();
    } catch (Exception ignore) {
    }
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() {
    SHOULD_THROW = false;
    SCAN_SEEN.set(false);
    EXCEPTION_THROWN.set(false);
  }

  @Test
  public void itIncreasesScannerCount() throws Exception {
    try (ResultScanner ignore = TABLE.getScanner(SCAN)) {
      // We need to wait until the scan and lease are created server-side.
      // Otherwise, our scanner counting will not reflect the new scan that was created
      UTIL.waitFor(1000, () -> SCAN_SEEN.get() && !EXCEPTION_THROWN.get());
    }
  }

  @Test
  public void itDoesNotIncreaseScannerLeaseCount() throws Exception {
    SHOULD_THROW = true;
    try (ResultScanner ignore = TABLE.getScanner(SCAN)) {
      // We need to wait until the scan and lease are created server-side.
      // Otherwise, our scanner counting will not reflect the new scan that was created
      UTIL.waitFor(1000, () -> !SCAN_SEEN.get() && EXCEPTION_THROWN.get());
    }
  }

  public static final class MockedQuotaManagerRegionServer
    extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {
    private final MockedRpcQuotaManager rpcQuotaManager;

    public MockedQuotaManagerRegionServer(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
      this.rpcQuotaManager = new MockedRpcQuotaManager(this);
    }

    @Override
    public RegionServerRpcQuotaManager getRegionServerRpcQuotaManager() {
      return rpcQuotaManager;
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new ScannerTrackingRSRpcServicesForTest(this);
    }
  }

  private static class MockedRpcQuotaManager extends RegionServerRpcQuotaManager {
    private static final RpcThrottlingException EX = new RpcThrottlingException("test_ex");

    public MockedRpcQuotaManager(RegionServerServices rsServices) {
      super(rsServices);
    }

    @Override
    public OperationQuota checkScanQuota(Region region, ClientProtos.ScanRequest scanRequest,
      long maxScannerResultSize, long maxBlockBytesScanned, long prevBlockBytesScannedDifference)
      throws IOException, RpcThrottlingException {
      if (SHOULD_THROW) {
        if (isTestScan(scanRequest)) {
          EXCEPTION_THROWN.set(true);
        }
        throw EX;
      }
      return TestNoopOperationQuota.INSTANCE;
    }

    @Override
    public OperationQuota checkBatchQuota(Region region, OperationQuota.OperationType type)
      throws IOException, RpcThrottlingException {
      if (SHOULD_THROW) {
        throw EX;
      }
      return TestNoopOperationQuota.INSTANCE;
    }

    @Override
    public OperationQuota checkBatchQuota(Region region, List<ClientProtos.Action> actions,
      boolean hasCondition) throws IOException, RpcThrottlingException {
      if (SHOULD_THROW) {
        throw EX;
      }
      return TestNoopOperationQuota.INSTANCE;
    }

    @Override
    public OperationQuota checkBatchQuota(Region region, int numWrites, int numReads,
      boolean isAtomic) throws IOException, RpcThrottlingException {
      if (SHOULD_THROW) {
        throw EX;
      }
      return TestNoopOperationQuota.INSTANCE;
    }
  }

  private static class ScannerTrackingRSRpcServicesForTest extends RSRpcServices {
    public ScannerTrackingRSRpcServicesForTest(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    RegionScannerContext checkQuotaAndGetRegionScannerContext(ClientProtos.ScanRequest request,
      ClientProtos.ScanResponse.Builder builder) throws IOException {
      RegionScannerContext rsx = super.checkQuotaAndGetRegionScannerContext(request, builder);
      if (isTestScan(request)) {
        SCAN_SEEN.set(true);
      }
      return rsx;
    }
  }

  private static boolean isTestScan(ClientProtos.ScanRequest request) {
    ClientProtos.Scan scan = request.getScan();
    return scan.getAttributeList().stream()
      .anyMatch(nbp -> nbp.getName().equals(SCAN_IDENTIFIER_NAME)
        && Bytes.equals(nbp.getValue().toByteArray(), SCAN_IDENTIFIER));
  }
}

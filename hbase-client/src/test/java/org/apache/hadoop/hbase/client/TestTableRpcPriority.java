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

import static org.apache.hadoop.hbase.HConstants.HIGH_QOS;
import static org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
import static org.apache.hadoop.hbase.HConstants.SYSTEMTABLE_QOS;
import static org.apache.hadoop.hbase.NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Test that correct rpc priority is sent to server from blocking Table calls. Currently only
 * implements checks for scans, but more could be added here.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestTableRpcPriority {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableRpcPriority.class);

  @Rule
  public TestName name = new TestName();

  private ClientProtos.ClientService.BlockingInterface stub;
  private Connection conn;

  @Before
  public void setUp() throws IOException, ServiceException {
    stub = mock(ClientProtos.ClientService.BlockingInterface.class);

    Configuration conf = HBaseConfiguration.create();

    ExecutorService executorService = Executors.newCachedThreadPool();
    User user = UserProvider.instantiate(conf).getCurrent();
    conn = new ConnectionImplementation(conf, executorService, user,
      new DoNothingConnectionRegistry(conf, user)) {

      @Override
      public ClientProtos.ClientService.BlockingInterface getClient(ServerName serverName)
        throws IOException {
        return stub;
      }

      @Override
      public RegionLocations relocateRegion(final TableName tableName, final byte[] row,
        int replicaId) throws IOException {
        return locateRegion(tableName, row, true, false, replicaId);
      }

      @Override
      public RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache,
        boolean retry, int replicaId) throws IOException {
        RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
        ServerName serverName = ServerName.valueOf("rs", 16010, 12345);
        HRegionLocation loc = new HRegionLocation(info, serverName);
        return new RegionLocations(loc);
      }
    };
  }

  @Test
  public void testScan() throws Exception {
    mockScan(19);
    testForTable(TableName.valueOf(name.getMethodName()), Optional.of(19));
  }

  /**
   * This test verifies that our closeScanner request honors the original priority of the scan if
   * it's greater than our expected HIGH_QOS for close calls.
   */
  @Test
  public void testScanSuperHighPriority() throws Exception {
    mockScan(1000);
    testForTable(TableName.valueOf(name.getMethodName()), Optional.of(1000));
  }

  @Test
  public void testScanNormalTable() throws Exception {
    mockScan(NORMAL_QOS);
    testForTable(TableName.valueOf(name.getMethodName()), Optional.of(NORMAL_QOS));
  }

  @Test
  public void testScanSystemTable() throws Exception {
    mockScan(SYSTEMTABLE_QOS);
    testForTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()),
      Optional.empty());
  }

  @Test
  public void testScanMetaTable() throws Exception {
    mockScan(SYSTEMTABLE_QOS);
    testForTable(TableName.META_TABLE_NAME, Optional.empty());
  }

  private void testForTable(TableName tableName, Optional<Integer> priority) throws Exception {
    Scan scan = new Scan().setCaching(1);
    priority.ifPresent(scan::setPriority);

    try (ResultScanner scanner = conn.getTable(tableName).getScanner(scan)) {
      assertNotNull(scanner.next());
      assertNotNull(scanner.next());
    }

    // just verify that the calls happened. verification of priority occurred in the mocking
    // open, next, then several renew lease
    verify(stub, atLeast(3)).scan(any(), any(ClientProtos.ScanRequest.class));
    verify(stub, times(1)).scan(assertControllerArgs(Math.max(priority.orElse(0), HIGH_QOS)),
      assertScannerCloseRequest());
  }

  private void mockScan(int scanPriority) throws ServiceException {
    int scannerId = 1;

    doAnswer(new Answer<ClientProtos.ScanResponse>() {
      @Override
      public ClientProtos.ScanResponse answer(InvocationOnMock invocation) throws Throwable {
        throw new IllegalArgumentException(
          "Call not covered by explicit mock for arguments controller=" + invocation.getArgument(0)
            + ", request=" + invocation.getArgument(1));
      }
    }).when(stub).scan(any(), any());

    AtomicInteger scanNextCalled = new AtomicInteger(0);
    doAnswer(new Answer<ClientProtos.ScanResponse>() {

      @Override
      public ClientProtos.ScanResponse answer(InvocationOnMock invocation) throws Throwable {
        ClientProtos.ScanRequest req = invocation.getArgument(1);
        assertFalse("close scanner should not come in with scan priority " + scanPriority,
          req.hasCloseScanner() && req.getCloseScanner());
        ClientProtos.ScanResponse.Builder builder = ClientProtos.ScanResponse.newBuilder();

        if (!req.hasScannerId()) {
          builder.setScannerId(scannerId);
        } else {
          builder.setScannerId(req.getScannerId());
        }

        Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
          .setRow(Bytes.toBytes(scanNextCalled.incrementAndGet())).setFamily(Bytes.toBytes("cf"))
          .setQualifier(Bytes.toBytes("cq")).setValue(Bytes.toBytes("v")).build();
        Result result = Result.create(Arrays.asList(cell));
        return builder.setTtl(800).setMoreResultsInRegion(true).setMoreResults(true)
          .addResults(ProtobufUtil.toResult(result)).build();
      }
    }).when(stub).scan(assertControllerArgs(scanPriority), any());

    doAnswer(new Answer<ClientProtos.ScanResponse>() {

      @Override
      public ClientProtos.ScanResponse answer(InvocationOnMock invocation) throws Throwable {
        ClientProtos.ScanRequest req = invocation.getArgument(1);
        assertTrue("close request should have scannerId", req.hasScannerId());
        assertEquals("close request's scannerId should match", scannerId, req.getScannerId());
        assertTrue("close request should have closerScanner set",
          req.hasCloseScanner() && req.getCloseScanner());

        return ClientProtos.ScanResponse.getDefaultInstance();
      }
    }).when(stub).scan(assertControllerArgs(Math.max(scanPriority, HIGH_QOS)),
      assertScannerCloseRequest());
  }

  private HBaseRpcController assertControllerArgs(int priority) {
    return argThat(new ArgumentMatcher<HBaseRpcController>() {

      @Override
      public boolean matches(HBaseRpcController controller) {
        // check specified priority, but also check that it has a timeout
        // this ensures that our conversion from the base controller to the close-specific
        // controller honored the original arguments.
        return controller.getPriority() == priority && controller.hasCallTimeout();
      }
    });
  }

  private ClientProtos.ScanRequest assertScannerCloseRequest() {
    return argThat(new ArgumentMatcher<ClientProtos.ScanRequest>() {

      @Override
      public boolean matches(ClientProtos.ScanRequest request) {
        return request.hasCloseScanner() && request.getCloseScanner();
      }
    });
  }
}

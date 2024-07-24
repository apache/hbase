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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Tests that one can implement their own RpcControllerFactory and expect it to successfully pass
 * custom priority values to the server for all HTable calls.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestCustomPriorityRpcControllerFactory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCustomPriorityRpcControllerFactory.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final AtomicReference<State> STATE = new AtomicReference<>(State.SETUP);
  private static final AtomicInteger EXPECTED_PRIORITY = new AtomicInteger();

  private enum State {
    SETUP,
    WAITING,
    SUCCESS
  }

  private static final TableName TABLE_NAME = TableName.valueOf("Timeout");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes(1L);

  private static final int MIN_CUSTOM_PRIORITY = 201;

  private static Connection CONN;
  private static Table TABLE;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Set RegionServer class and use default values for other options.
    UTIL.startMiniCluster(
      StartTestingClusterOption.builder().rsClass(PriorityRegionServer.class).build());
    TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    UTIL.getAdmin().createTable(descriptor);

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setClass(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
      PriorityRpcControllerFactory.class, RpcControllerFactory.class);
    CONN = ConnectionFactory.createConnection(conf);
    TABLE = CONN.getTable(TABLE_NAME);
  }

  @Test
  public void tetGetPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        TABLE.get(new Get(ROW));
      }
    });
  }

  @Test
  public void testDeletePriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        TABLE.delete(new Delete(ROW));
      }
    });
  }

  @Test
  public void testIncrementPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        TABLE.increment(new Increment(ROW).addColumn(FAMILY, QUALIFIER, 1));
      }
    });
  }

  @Test
  public void testAppendPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        TABLE.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
      }
    });
  }

  @Test
  public void testPutPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        TABLE.put(put);
      }
    });

  }

  @Test
  public void testExistsPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        TABLE.exists(new Get(ROW));
      }
    });
  }

  @Test
  public void testMutatePriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        RowMutations mutation = new RowMutations(ROW);
        mutation.add(new Delete(ROW));
        mutation.add(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
        TABLE.mutateRow(mutation);
      }
    });
  }

  @Test
  public void testCheckAndMutatePriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        RowMutations mutation = new RowMutations(ROW);
        mutation.add(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
        TABLE.checkAndMutate(
          CheckAndMutate.newBuilder(ROW).ifNotExists(FAMILY, QUALIFIER).build(mutation));
      }
    });
  }

  @Test
  public void testMultiGetsPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws Exception {
        Get get1 = new Get(ROW);
        get1.addColumn(FAMILY, QUALIFIER);
        Get get2 = new Get(ROW);
        get2.addColumn(FAMILY, QUALIFIER);
        List<Get> gets = new ArrayList<>();
        gets.add(get1);
        gets.add(get2);
        TABLE.batch(gets, new Object[2]);
      }
    });
  }

  @Test
  public void testMultiPutsPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws Exception {
        Put put1 = new Put(ROW);
        put1.addColumn(FAMILY, QUALIFIER, VALUE);
        Put put2 = new Put(ROW);
        put2.addColumn(FAMILY, QUALIFIER, VALUE);
        List<Put> puts = new ArrayList<>();
        puts.add(put1);
        puts.add(put2);
        TABLE.batch(puts, new Object[2]);
      }
    });
  }

  @Test
  public void testScanPriority() throws Exception {
    testForCall(new ThrowingCallable() {
      @Override
      public void call() throws IOException {
        ResultScanner scanner = TABLE.getScanner(new Scan());
        scanner.next();
      }
    });
  }

  private void testForCall(ThrowingCallable callable) throws Exception {
    STATE.set(State.WAITING);
    // set it higher than MIN_CUSTOM_PRIORITY so we can ignore calls for meta, setup, etc
    EXPECTED_PRIORITY.set(new Random().nextInt(MIN_CUSTOM_PRIORITY) + MIN_CUSTOM_PRIORITY);
    callable.call();

    assertEquals("Expected state to change to SUCCESS. Check for assertion error in logs",
      STATE.get(), State.SUCCESS);
  }

  private interface ThrowingCallable {
    void call() throws Exception;
  }

  public static class PriorityRegionServer
    extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {
    public PriorityRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new PriorityRpcServices(this);
    }
  }

  public static class PriorityRpcControllerFactory extends RpcControllerFactory {

    public PriorityRpcControllerFactory(Configuration conf) {
      super(conf);
    }

    @Override
    public HBaseRpcController newController() {
      return new PriorityController(EXPECTED_PRIORITY.get(), super.newController());
    }

    @Override
    public HBaseRpcController newController(ExtendedCellScanner cellScanner) {
      return new PriorityController(EXPECTED_PRIORITY.get(), super.newController(cellScanner));
    }

    @Override
    public HBaseRpcController newController(List<ExtendedCellScannable> cellIterables) {
      return new PriorityController(EXPECTED_PRIORITY.get(), super.newController(cellIterables));
    }
  }

  private static class PriorityController extends DelegatingHBaseRpcController {
    private final int priority;

    public PriorityController(int priority, HBaseRpcController controller) {
      super(controller);
      this.priority = priority;
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

  public static class PriorityRpcServices extends RSRpcServices {
    PriorityRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    private void checkPriorityIfWaiting() {
      if (STATE.get() == State.WAITING) {
        int priority = RpcServer.getCurrentCall().get().getPriority();
        if (priority < MIN_CUSTOM_PRIORITY) {
          return;
        }
        assertEquals(EXPECTED_PRIORITY.get(), priority);
        STATE.set(State.SUCCESS);
      }
    }

    @Override
    public ClientProtos.GetResponse get(RpcController controller, ClientProtos.GetRequest request)
      throws ServiceException {
      checkPriorityIfWaiting();
      return super.get(controller, request);
    }

    @Override
    public ClientProtos.MutateResponse mutate(RpcController rpcc,
      ClientProtos.MutateRequest request) throws ServiceException {
      checkPriorityIfWaiting();
      return super.mutate(rpcc, request);
    }

    @Override
    public ClientProtos.ScanResponse scan(RpcController controller,
      ClientProtos.ScanRequest request) throws ServiceException {
      checkPriorityIfWaiting();
      return super.scan(controller, request);
    }

    @Override
    public ClientProtos.MultiResponse multi(RpcController rpcc, ClientProtos.MultiRequest request)
      throws ServiceException {
      checkPriorityIfWaiting();
      return super.multi(rpcc, request);
    }
  }
}

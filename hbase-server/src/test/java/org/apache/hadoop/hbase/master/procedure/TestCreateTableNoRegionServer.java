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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CreateTableState;

@Category({ MasterTests.class, MediumTests.class })
public class TestCreateTableNoRegionServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCreateTableNoRegionServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCreateTableNoRegionServer.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");
  
  private static byte[] FAMILY = Bytes.toBytes("f1");

  private static CountDownLatch ARRIVE;

  private static CountDownLatch RESUME;

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    private boolean isInAssignRegionsState() {
      try {
        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
          if (e.getClassName().equals(CreateTableProcedure.class.getName()) &&
            e.getMethodName().equals("executeFromState")) {
            for (Procedure<?> proc : getProcedures()) {
              if (proc instanceof CreateTableProcedure && !proc.isFinished() &&
                ((CreateTableProcedure) proc)
                  .getCurrentStateId() == CreateTableState.CREATE_TABLE_ASSIGN_REGIONS_VALUE) {
                return true;
              }
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return false;
    }

    @Override
    public AssignmentManager getAssignmentManager() {
      if (ARRIVE != null && isInAssignRegionsState()) {
        ARRIVE.countDown();
        ARRIVE = null;
        try {
          RESUME.await();
        } catch (InterruptedException e) {
        }
      }
      return super.getAssignmentManager();
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(
      StartMiniClusterOption.builder().masterClass(HMasterForTest.class).build());
    // this may cause dead lock if there is no live region server and want to start a new server.
    // In JmxCacheBuster we will reinitialize the metrics system so it will get some metrics which
    // will need to access meta, since there is no region server, the request will hang there for a
    // long time while holding the lock of MetricsSystemImpl, but when start a new region server, we
    // also need to update metrics in handleReportForDutyResponse, since we are all in the same
    // process and uses the same metrics instance, we hit dead lock.
    JmxCacheBuster.stop();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreate() throws Exception {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    Admin admin = UTIL.getAdmin();
    ARRIVE = new CountDownLatch(1);
    RESUME = new CountDownLatch(1);
    Future<Void> future = admin.createTableAsync(td);
    ARRIVE.await();

    UTIL.getMiniHBaseCluster().stopRegionServer(0).join();

    // make sure we the region server is done.
    UTIL.waitFor(30000,
      () -> UTIL.getMiniHBaseCluster().getMaster().getServerManager().getOnlineServers().isEmpty());
    RESUME.countDown();

    Thread.sleep(10000);
    // the procedure should still be in the CREATE_TABLE_ASSIGN_REGIONS state, but here, we just
    // warn it as it may cause more serious problem later.
    for (Procedure<?> proc : UTIL.getMiniHBaseCluster().getMaster().getProcedures()) {
      if (proc instanceof CreateTableProcedure && !proc.isFinished() &&
        ((CreateTableProcedure) proc)
          .getCurrentStateId() != CreateTableState.CREATE_TABLE_ASSIGN_REGIONS_VALUE) {
        LOG.warn("Create table procedure {} assigned regions without a region server!", proc);
      }
    }
    UTIL.getMiniHBaseCluster().startRegionServer();
    // the creation should finally be done
    future.get(30, TimeUnit.SECONDS);
    // make sure we could put to the table
    try (Table table = UTIL.getConnection().getTableBuilder(TABLE_NAME, null)
      .setOperationTimeout(5000).build()) {
      table.put(new Put(Bytes.toBytes(0)).addColumn(FAMILY,
        Bytes.toBytes("q"), Bytes.toBytes(0)));
    }
  }
}

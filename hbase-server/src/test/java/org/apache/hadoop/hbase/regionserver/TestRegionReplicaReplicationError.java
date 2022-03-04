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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;

/**
 * Test region replication when error occur.
 * <p/>
 * We can not simply move the secondary replicas as we will trigger a flush for the primary replica
 * when secondary replica is online, which will always make the data of the two regions in sync. So
 * here we need to simulate request errors.
 */
@Category({ FlakeyTests.class, LargeTests.class })
public class TestRegionReplicaReplicationError {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicaReplicationError.class);

  public static final class ErrorReplayRSRpcServices extends RSRpcServices {

    private final AtomicInteger count = new AtomicInteger(0);

    public ErrorReplayRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ReplicateWALEntryResponse replicateToReplica(RpcController controller,
        ReplicateWALEntryRequest request) throws ServiceException {
      List<WALEntry> entries = request.getEntryList();
      if (CollectionUtils.isEmpty(entries)) {
        return ReplicateWALEntryResponse.getDefaultInstance();
      }
      ByteString regionName = entries.get(0).getKey().getEncodedRegionName();
      HRegion region;
      try {
        region = server.getRegionByEncodedName(regionName.toStringUtf8());
      } catch (NotServingRegionException e) {
        throw new ServiceException(e);
      }
      // fail the first several request
      if (region.getRegionInfo().getReplicaId() == 1 && count.addAndGet(entries.size()) < 100) {
        throw new ServiceException("Inject error!");
      }
      return super.replicateToReplica(controller, request);
    }
  }

  public static final class RSForTest
    extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {

    public RSForTest(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new ErrorReplayRSRpcServices(this);
    }
  }

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  private static TableName TN = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  @BeforeClass
  public static void setUp() throws Exception {
    HTU.getConfiguration().setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY,
      true);
    HTU.startMiniCluster(
      StartTestingClusterOption.builder().rsClass(RSForTest.class).numRegionServers(3).build());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TN).setRegionReplication(3)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();
    HTU.getAdmin().createTable(td);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  private boolean checkReplica(Table table, int replicaId) throws IOException {
    boolean ret = true;
    for (int i = 0; i < 500; i++) {
      Result result = table.get(new Get(Bytes.toBytes(i)).setReplicaId(replicaId));
      byte[] value = result.getValue(CF, CQ);
      ret &= value != null && value.length > 0 && Bytes.toInt(value) == i;
    }
    return ret;
  }

  @Test
  public void test() throws IOException {
    try (Table table = HTU.getConnection().getTable(TN)) {
      for (int i = 0; i < 500; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
      HTU.waitFor(30000, () -> checkReplica(table, 2));
      HTU.waitFor(30000, () -> checkReplica(table, 1));
    }
  }
}

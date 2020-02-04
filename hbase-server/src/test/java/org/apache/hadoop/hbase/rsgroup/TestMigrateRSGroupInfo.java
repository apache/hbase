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
package org.apache.hadoop.hbase.rsgroup;

import static org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl.META_FAMILY_BYTES;
import static org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl.META_QUALIFIER_BYTES;
import static org.apache.hadoop.hbase.rsgroup.RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-22819
 */
@Category({ RSGroupTests.class, MediumTests.class })
public class TestMigrateRSGroupInfo extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateRSGroupInfo.class);

  private static String TABLE_NAME_PREFIX = "Table_";

  private static int NUM_TABLES = 10;

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static RSGroupAdminClient RS_GROUP_ADMIN_CLIENT;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class,
      HMaster.class);
    // confirm that we could enable rs group by setting the old CP.
    TEST_UTIL.getConfiguration().setBoolean(RSGroupInfoManager.RS_GROUP_ENABLED, false);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName());
    setUpTestBeforeClass();
    RS_GROUP_ADMIN_CLIENT = new RSGroupAdminClient(TEST_UTIL.getConnection());
    for (int i = 0; i < NUM_TABLES; i++) {
      TEST_UTIL.createTable(TableName.valueOf(TABLE_NAME_PREFIX + i), FAMILY);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  private static CountDownLatch RESUME = new CountDownLatch(1);

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException, KeeperException {
      super(conf);
    }

    @Override
    public TableDescriptors getTableDescriptors() {
      if (RESUME != null) {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
          if (element.getMethodName().equals("migrate")) {
            try {
              RESUME.await();
            } catch (InterruptedException e) {
            }
            RESUME = null;
            break;
          }
        }
      }
      return super.getTableDescriptors();
    }
  }

  @Test
  public void testMigrate() throws IOException, InterruptedException {
    String groupName = getNameWithoutIndex(name.getMethodName());
    addGroup(groupName, TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().size() - 1);
    RSGroupInfo rsGroupInfo = ADMIN.getRSGroup(groupName);
    assertTrue(rsGroupInfo.getTables().isEmpty());
    for (int i = 0; i < NUM_TABLES; i++) {
      rsGroupInfo.addTable(TableName.valueOf(TABLE_NAME_PREFIX + i));
    }
    try (Table table = TEST_UTIL.getConnection().getTable(RSGROUP_TABLE_NAME)) {
      RSGroupProtos.RSGroupInfo proto = ProtobufUtil.toProtoGroupInfo(rsGroupInfo);
      Put p = new Put(Bytes.toBytes(rsGroupInfo.getName()));
      p.addColumn(META_FAMILY_BYTES, META_QUALIFIER_BYTES, proto.toByteArray());
      table.put(p);
    }
    TEST_UTIL.getMiniHBaseCluster().stopMaster(0).join();
    RESUME = new CountDownLatch(1);
    TEST_UTIL.getMiniHBaseCluster().startMaster();

    // wait until we can get the rs group info for a table
    TEST_UTIL.waitFor(30000, () -> {
      try {
        RS_GROUP_ADMIN_CLIENT.getRSGroupInfoOfTable(TableName.valueOf(TABLE_NAME_PREFIX + 0));
        return true;
      } catch (IOException e) {
        return false;
      }
    });
    // confirm that before migrating, we could still get the correct rs group for a table.
    for (int i = 0; i < NUM_TABLES; i++) {
      RSGroupInfo info =
        RS_GROUP_ADMIN_CLIENT.getRSGroupInfoOfTable(TableName.valueOf(TABLE_NAME_PREFIX + i));
      assertEquals(rsGroupInfo.getName(), info.getName());
      assertEquals(NUM_TABLES, info.getTables().size());
    }
    RESUME.countDown();
    TEST_UTIL.waitFor(60000, () -> {
      for (int i = 0; i < NUM_TABLES; i++) {
        TableDescriptor td;
        try {
          td = TEST_UTIL.getAdmin().getDescriptor(TableName.valueOf(TABLE_NAME_PREFIX + i));
        } catch (IOException e) {
          return false;
        }
        if (!rsGroupInfo.getName().equals(td.getRegionServerGroup().orElse(null))) {
          return false;
        }
      }
      return true;
    });
    // make sure that we persist the result to hbase, where we delete all the tables in the rs
    // group.
    TEST_UTIL.waitFor(30000, () -> {
      try (Table table = TEST_UTIL.getConnection().getTable(RSGROUP_TABLE_NAME)) {
        Result result = table.get(new Get(Bytes.toBytes(rsGroupInfo.getName())));
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo
          .parseFrom(result.getValue(META_FAMILY_BYTES, META_QUALIFIER_BYTES));
        RSGroupInfo gi = ProtobufUtil.toGroupInfo(proto);
        return gi.getTables().isEmpty();
      }
    });
    // make sure that the migrate thread has quit.
    TEST_UTIL.waitFor(30000, () -> Thread.getAllStackTraces().keySet().stream()
      .noneMatch(t -> t.getName().equals(RSGroupInfoManagerImpl.MIGRATE_THREAD_NAME)));
    // make sure we could still get the correct rs group info after migration
    for (int i = 0; i < NUM_TABLES; i++) {
      RSGroupInfo info =
        RS_GROUP_ADMIN_CLIENT.getRSGroupInfoOfTable(TableName.valueOf(TABLE_NAME_PREFIX + i));
      assertEquals(rsGroupInfo.getName(), info.getName());
      assertEquals(NUM_TABLES, info.getTables().size());
    }
  }
}

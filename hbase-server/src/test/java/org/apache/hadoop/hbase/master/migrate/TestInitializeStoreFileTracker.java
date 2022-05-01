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
package org.apache.hadoop.hbase.master.migrate;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, MasterTests.class })
public class TestInitializeStoreFileTracker {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestInitializeStoreFileTracker.class);
  private final static String[] tables = new String[] { "t1", "t2", "t3", "t4", "t5", "t6" };
  private final static String famStr = "f1";
  private final static byte[] fam = Bytes.toBytes(famStr);

  private HBaseTestingUtility HTU;
  private Configuration conf;
  private HTableDescriptor tableDescriptor;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    // Speed up the launch of RollingUpgradeChore
    conf.setInt(RollingUpgradeChore.ROLLING_UPGRADE_CHORE_PERIOD_SECONDS_KEY, 1);
    conf.setLong(RollingUpgradeChore.ROLLING_UPGRADE_CHORE_DELAY_SECONDS_KEY, 1);
    // Set the default implementation to file instead of default, to confirm we will not set SFT to
    // file
    conf.set(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name());
    HTU = new HBaseTestingUtility(conf);
    HTU.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testMigrateStoreFileTracker() throws IOException, InterruptedException {
    // create tables to test
    for (int i = 0; i < tables.length; i++) {
      tableDescriptor = HTU.createTableDescriptor(tables[i]);
      tableDescriptor.addFamily(new HColumnDescriptor(fam));
      HTU.createTable(tableDescriptor, null);
    }
    TableDescriptors tableDescriptors = HTU.getMiniHBaseCluster().getMaster().getTableDescriptors();
    for (int i = 0; i < tables.length; i++) {
      TableDescriptor tdAfterCreated = tableDescriptors.get(TableName.valueOf(tables[i]));
      // make sure that TRACKER_IMPL was set by default after tables have been created.
      Assert.assertNotNull(tdAfterCreated.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
      // Remove StoreFileTracker impl from tableDescriptor
      TableDescriptor tdRemovedSFT = TableDescriptorBuilder.newBuilder(tdAfterCreated)
        .removeValue(StoreFileTrackerFactory.TRACKER_IMPL).build();
      tableDescriptors.update(tdRemovedSFT);
    }
    HTU.getMiniHBaseCluster().stopMaster(0).join();
    HTU.getMiniHBaseCluster().startMaster();
    HTU.getMiniHBaseCluster().waitForActiveAndReadyMaster(30000);
    // wait until all tables have been migrated
    TableDescriptors tds = HTU.getMiniHBaseCluster().getMaster().getTableDescriptors();
    HTU.waitFor(30000, () -> {
      try {
        for (int i = 0; i < tables.length; i++) {
          TableDescriptor td = tds.get(TableName.valueOf(tables[i]));
          if (StringUtils.isEmpty(td.getValue(StoreFileTrackerFactory.TRACKER_IMPL))) {
            return false;
          }
        }
        return true;
      } catch (IOException e) {
        return false;
      }
    });
    for (String table : tables) {
      TableDescriptor td = tds.get(TableName.valueOf(table));
      assertEquals(StoreFileTrackerFactory.Trackers.DEFAULT.name(),
        td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
    }
  }
}

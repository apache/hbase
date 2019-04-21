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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;

import static junit.framework.TestCase.assertTrue;

/**
 * Tests that table state is mirrored out to zookeeper for hbase-1.x clients.
 * Also tests that table state gets migrated from zookeeper on master start.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestMirroringTableStateManager {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMirroringTableStateManager.class);
  @Rule
  public TestName name = new TestName();

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void before() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMirroring() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY_STR);
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    assertTrue(TableState.State.ENABLED.equals(getTableStateInZK(zkw, tableName)));
    TEST_UTIL.getAdmin().disableTable(tableName);
    assertTrue(TableState.State.DISABLED.equals(getTableStateInZK(zkw, tableName)));
    TEST_UTIL.getAdmin().deleteTable(tableName);
    assertTrue(getTableStateInZK(zkw, tableName) == null);
  }

  private TableState.State getTableStateInZK(ZKWatcher watcher, final TableName tableName)
      throws KeeperException, IOException, InterruptedException {
    String znode = ZNodePaths.joinZNode(watcher.getZNodePaths().tableZNode,
            tableName.getNameAsString());
    byte [] data = ZKUtil.getData(watcher, znode);
    if (data == null || data.length <= 0) {
      return null;
    }
    try {
      ProtobufUtil.expectPBMagicPrefix(data);
      ZooKeeperProtos.DeprecatedTableState.Builder builder =
          ZooKeeperProtos.DeprecatedTableState.newBuilder();
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, data, magicLen, data.length - magicLen);
      return TableState.State.valueOf(builder.getState().toString());
    } catch (IOException e) {
      KeeperException ke = new KeeperException.DataInconsistencyException();
      ke.initCause(e);
      throw ke;
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }
}

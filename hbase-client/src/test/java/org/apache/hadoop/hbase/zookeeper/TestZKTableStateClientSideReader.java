/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

@Category({SmallTests.class})
public class TestZKTableStateClientSideReader {

  @Test
  public void test() throws Exception {
    ZooKeeperWatcher zkw = Mockito.mock(ZooKeeperWatcher.class);
    RecoverableZooKeeper rzk = Mockito.mock(RecoverableZooKeeper.class);
    Mockito.doReturn(rzk).when(zkw).getRecoverableZooKeeper();
    Mockito.doReturn(null).when(rzk).getData(Mockito.anyString(),
        Mockito.any(Watcher.class), Mockito.any(Stat.class));
    TableName table = TableName.valueOf("table-not-exists");
    try {
      ZKTableStateClientSideReader.getTableState(zkw, table);
      fail("Shouldn't reach here");
    } catch(TableNotFoundException e) {
      // Expected Table not found exception
    }
  }
}

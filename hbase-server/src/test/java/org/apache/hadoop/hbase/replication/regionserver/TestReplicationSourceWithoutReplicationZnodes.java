/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationSourceDummyWithNoTermination;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationSourceWithoutReplicationZnodes
  extends TestReplicationSourceManagerBase {

  @Before
  public void removeExistingSourcesFromSourceManager() {
    manager.getSources().clear();
    manager.getOldSources().clear();
  }

  /**
   * When the peer is removed, hbase remove the peer znodes and there is zk watcher
   * which terminates the replication sources. In case of zk watcher not getting invoked
   * or a race condition between source deleting the log znode and zk watcher
   * terminating the source, we might get the NoNode exception. In that case, the right
   * thing is to terminate the replication source.
   *
   * @throws Exception throws exception
   */
  @Test
  public void testReplicationSourceRunningWithoutPeerZnodes() throws Exception {
    String replicationSourceImplName = conf.get("replication.replicationsource.implementation");
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    KeyValue kv = new KeyValue(r1, f1, r1);
    WALEdit edit = new WALEdit();
    edit.add(kv);
    try {
      conf.set("replication.replicationsource.implementation",
        ReplicationSourceDummyWithNoTermination.class.getCanonicalName());
      List<WALActionsListener> listeners = new ArrayList<>();
      listeners.add(replication);
      final WALFactory wals = new WALFactory(utility.getConfiguration(), listeners,
        URLEncoder.encode("regionserver:60020", "UTF8"));
      final WAL wal = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
      manager.init();

      final long txid = wal.append(htd, hri,
        new WALKey(hri.getEncodedNameAsBytes(), test, System.currentTimeMillis(), mvcc), edit,
        true);
      wal.sync(txid);

      wal.rollWriter();
      ZKUtil.deleteNodeRecursively(zkw, "/hbase/replication/peers/1");
      ZKUtil.deleteNodeRecursively(zkw, "/hbase/replication/rs/" + server.getServerName() + "/1");

      Assert.assertEquals("There should be exactly one source",
        1, manager.getSources().size());
      Assert.assertEquals("Replication source is not correct",
        ReplicationSourceDummyWithNoTermination.class,
        manager.getSources().get(0).getClass());
      manager
        .logPositionAndCleanOldLogs(manager.getSources().get(0).getCurrentPath(), "1", 0, false,
          false);
      Assert.assertTrue("Replication source should be terminated and removed",
        manager.getSources().isEmpty());
    } finally {
      conf.set("replication.replicationsource.implementation", replicationSourceImplName);
    }
  }
}

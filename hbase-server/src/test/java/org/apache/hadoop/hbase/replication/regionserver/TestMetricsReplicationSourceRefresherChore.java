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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ SmallTests.class })
public class TestMetricsReplicationSourceRefresherChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsReplicationSourceRefresherChore.class);

  @Test
  public void testOldestWalAgeMetricsRefresherCore() {
    try {
      HRegionServer rs = mock(HRegionServer.class);
      ReplicationSource mockSource = Mockito.mock(ReplicationSource.class);
      MetricsSource mockMetricsSource = Mockito.mock(MetricsSource.class);
      Mockito.when(mockSource.getSourceMetrics()).thenReturn(mockMetricsSource);
      ManualEnvironmentEdge manualEdge = new ManualEnvironmentEdge();
      EnvironmentEdgeManager.injectEdge(manualEdge);
      MetricsReplicationSourceRefresherChore mrsrChore =
          new MetricsReplicationSourceRefresherChore(10, rs, mockSource);

      manualEdge.setValue(10);
      final Path log1 = new Path("log-walgroup-a.8");
      String walGroupId1 = "fake-walgroup-id-1";
      Map<String, PriorityBlockingQueue<Path>> queues = new ConcurrentHashMap<>();
      PriorityBlockingQueue<Path> queue =
          new PriorityBlockingQueue<>(1, new AbstractFSWALProvider.WALStartTimeComparator());
      queue.put(log1);
      queues.put(walGroupId1, queue);
      Mockito.when(mockSource.getQueues()).thenReturn(queues);
      mrsrChore.chore();
      verify(mockMetricsSource, times(1)).setOldestWalAge(2);

      manualEdge.setValue(20);
      final Path log2 = new Path("log-walgroup-b.8");
      String walGroupId2 = "fake-walgroup-id-2";
      queue.put(log2);
      queues.put(walGroupId2, queue);
      mrsrChore.chore();
      verify(mockMetricsSource, times(1)).setOldestWalAge(12);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }
}

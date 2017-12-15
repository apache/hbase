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
package org.apache.hadoop.hbase.master.replication;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestDummyModifyPeerProcedure {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String PEER_ID;

  private static Path DIR;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    PEER_ID = "testPeer";
    DIR = new Path("/" + PEER_ID);
    UTIL.getTestFileSystem().mkdirs(DIR);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    ProcedureExecutor<?> executor =
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    long procId = executor.submitProcedure(new DummyModifyPeerProcedure(PEER_ID));
    UTIL.waitFor(30000, new Waiter.Predicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return executor.isFinished(procId);
      }
    });
    Set<String> serverNames = UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .map(t -> t.getRegionServer().getServerName().toString())
        .collect(Collectors.toCollection(HashSet::new));
    for (FileStatus s : UTIL.getTestFileSystem().listStatus(DIR)) {
      assertTrue(serverNames.remove(s.getPath().getName()));
    }
    assertTrue(serverNames.isEmpty());
  }
}

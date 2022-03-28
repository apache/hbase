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

package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedure {
  protected static final Logger LOG = LoggerFactory.getLogger(TestSnapshotProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedure.class);

  protected static HBaseTestingUtil TEST_UTIL;
  protected HMaster master;
  protected TableName TABLE_NAME;
  protected byte[] CF;
  protected String SNAPSHOT_NAME;
  protected SnapshotDescription snapshot;
  protected SnapshotProtos.SnapshotDescription snapshotProto;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration config = TEST_UTIL.getConfiguration();
    // using SnapshotVerifyProcedure to verify snapshot
    config.setInt("hbase.snapshot.remote.verify.threshold", 1);
    // disable info server. Info server is useful when we run unit tests locally, but it will
    // fails integration testing of jenkins.
    // config.setInt(HConstants.MASTER_INFO_PORT, 8080);

    // delay dispatch so that we can do something, for example kill a target server
    config.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    config.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("SPTestTable"));
    CF = Bytes.toBytes("cf");
    SNAPSHOT_NAME = "SnapshotProcedureTest";
    snapshot = new SnapshotDescription(SNAPSHOT_NAME, TABLE_NAME, SnapshotType.FLUSH);
    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(TABLE_NAME, CF, splitKeys);
    TEST_UTIL.loadTable(table, CF, false);
  }

  public <T extends Procedure<MasterProcedureEnv>> T waitProcedureRunnableAndGetFirst(
    Class<T> clazz, long timeout) throws IOException {
    TEST_UTIL.waitFor(timeout, () -> master.getProcedures().stream()
      .anyMatch(clazz::isInstance));
    Optional<T> procOpt =  master.getMasterProcedureExecutor().getProcedures().stream()
      .filter(clazz::isInstance).map(clazz::cast).findFirst();
    assertTrue(procOpt.isPresent());
    return procOpt.get();
  }

  protected SnapshotProcedure getDelayedOnSpecificStateSnapshotProcedure(
    SnapshotProcedure sp, MasterProcedureEnv env, SnapshotState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    SnapshotProcedure spySp = Mockito.spy(sp);
    Mockito.doAnswer(new AnswersWithDelay(60000, new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return invocation.callRealMethod();
      }
    })).when(spySp).executeFromState(env, state);
    return spySp;
  }

  @After
  public void teardown() throws Exception {
    if (this.master != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        master.getMasterProcedureExecutor(), false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}

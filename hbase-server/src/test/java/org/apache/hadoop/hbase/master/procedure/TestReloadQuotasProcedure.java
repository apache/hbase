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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.assignment.MockMasterServices;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestReloadQuotasProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestReloadQuotasProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReloadQuotasProcedure.class);

  @Rule
  public TestName name = new TestName();

  private HBaseTestingUtil util;
  private MockMasterServices master;
  private TestServerRemoteProcedure.MockRSProcedureDispatcher rsDispatcher;

  @Before
  public void setUp() throws Exception {
    util = new HBaseTestingUtil();
    master = new MockMasterServices(util.getConfiguration());
    rsDispatcher = new TestServerRemoteProcedure.MockRSProcedureDispatcher(master);
    master.start(2, rsDispatcher);
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
  }

  @Test
  public void itHandlesClassNotFoundExceptionGracefully() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);

    ClassNotFoundException classNotFound =
      new ClassNotFoundException("ReloadQuotasCallable not found");
    IOException wrappedException = new IOException("Remote call failed", classNotFound);

    boolean result =
      procedure.complete(master.getMasterProcedureExecutor().getEnvironment(), wrappedException);

    assertTrue(result);
  }

  @Test
  public void itReturnsFailureForOtherExceptions() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);

    IOException otherException = new IOException("Some other error");

    boolean result =
      procedure.complete(master.getMasterProcedureExecutor().getEnvironment(), otherException);

    assertFalse(result);
  }

  @Test
  public void itReturnsSuccessForNoError() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);

    boolean result = procedure.complete(master.getMasterProcedureExecutor().getEnvironment(), null);

    assertTrue(result);
  }

  @Test
  public void itCorrectlyDetectsCauseClass() {
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure();

    ClassNotFoundException classNotFound = new ClassNotFoundException("test");
    IOException wrappedException = new IOException("wrapper", classNotFound);
    RuntimeException outerWrapper = new RuntimeException("outer", wrappedException);

    assertTrue(procedure.containsCause(outerWrapper, ClassNotFoundException.class));
    assertFalse(procedure.containsCause(outerWrapper, IllegalArgumentException.class));
    assertTrue(procedure.containsCause(classNotFound, ClassNotFoundException.class));
  }

  @Test
  public void itValidatesServerNameInRemoteCallBuild() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ServerName wrongServer = master.getServerManager().getOnlineServersList().get(1);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);

    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();

    Optional<RemoteProcedureDispatcher.RemoteOperation> result =
      procedure.remoteCallBuild(env, targetServer);
    assertTrue(result.isPresent());
    assertTrue(result.get() instanceof RSProcedureDispatcher.ServerOperation);

    assertThrows(IllegalArgumentException.class, () -> {
      procedure.remoteCallBuild(env, wrongServer);
    });
  }

  @Test
  public void itCreatesCorrectRemoteOperation() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();

    Optional<RemoteProcedureDispatcher.RemoteOperation> operation =
      procedure.remoteCallBuild(env, targetServer);

    assertTrue(operation.isPresent());
    RSProcedureDispatcher.ServerOperation serverOp =
      (RSProcedureDispatcher.ServerOperation) operation.get();
    assertEquals(serverOp.getRemoteProcedure(), procedure);
    assertEquals(serverOp.buildRequest().getProcId(), procedure.getProcId());
  }

  @Test
  public void itThrowsUnsupportedOperationExceptionOnRollback() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();

    assertThrows(UnsupportedOperationException.class, () -> {
      procedure.rollback(env);
    });
  }

  @Test
  public void itReturnsFalseOnAbort() throws Exception {
    ServerName targetServer = master.getServerManager().getOnlineServersList().get(0);
    ReloadQuotasProcedure procedure = new ReloadQuotasProcedure(targetServer);
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();

    boolean result = procedure.abort(env);

    assertFalse(result);
  }

  private Future<byte[]> submitProcedure(ReloadQuotasProcedure procedure) {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), procedure);
  }
}

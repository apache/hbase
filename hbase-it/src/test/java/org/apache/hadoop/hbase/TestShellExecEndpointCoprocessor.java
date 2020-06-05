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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecService;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for the {@link ShellExecEndpointCoprocessor}.
 */
@Category(MediumTests.class)
public class TestShellExecEndpointCoprocessor {

  private static IntegrationTestingUtility util;
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    // Set up the integration test util
    if (util == null) {
      util = new IntegrationTestingUtility(createConfiguration());
      util.initializeCluster(3);
      connection = util.getConnection();
    }
  }


  @AfterClass
  public static void teardown() throws Exception {
    IOUtils.closeQuietly(connection);
    // Clean everything up.
    if (util != null) {
      util.restoreCluster();
      util = null;
    }
  }
  @Test
  public void testShellExecUnspecified() throws IOException, ServiceException {
    testShellExecForeground(ShellExecRequest.newBuilder());
  }

  @Test
  public void testShellExecForeground() throws IOException, ServiceException {
    testShellExecForeground(ShellExecRequest.newBuilder().setAwaitResponse(true));
  }

  private void testShellExecForeground(final ShellExecRequest.Builder builder)
      throws IOException, ServiceException {
    final Admin admin = connection.getAdmin();

    final String command = "echo -n \"hello world\"";
    builder.setCommand(command);
    ShellExecService.BlockingInterface stub =
        ShellExecService.newBlockingStub(admin.coprocessorService());
    RpcController controller = new HBaseRpcControllerImpl();
    ShellExecResponse resp = stub.shellExec(controller, builder.build());
    assertEquals(0, resp.getExitCode());
    assertEquals("hello world", resp.getStdout());
  }

  @Test
  public void testShellExecBackground() throws IOException, ServiceException {
    Admin admin = connection.getAdmin();
    final File testDataDir = ensureTestDataDirExists(util);
    final File testFile = new File(testDataDir, "shell_exec_background.txt");
    assertTrue(testFile.createNewFile());
    assertEquals(0, testFile.length());

    final String command = "echo \"hello world\" >> " + testFile.getAbsolutePath();
    final ShellExecRequest req = ShellExecRequest.newBuilder()
      .setCommand(command)
      .setAwaitResponse(false)
      .build();

    ShellExecService.BlockingInterface stub =
        ShellExecService.newBlockingStub(admin.coprocessorService());
    RpcController controller = new HBaseRpcControllerImpl();
    ShellExecResponse resp = stub.shellExec(controller, req);
    assertFalse("the response from a background task should have no exit code", resp.hasExitCode());
    assertFalse("the response from a background task should have no stdout", resp.hasStdout());
    assertFalse("the response from a background task should have no stderr", resp.hasStderr());

    Waiter.waitFor(util.getConfiguration(), 5_000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return testFile.length() > 0;
      }
    });
    final String content = new String(Files.readAllBytes(testFile.toPath())).trim();
    assertEquals("hello world", content);
  }

  private static File ensureTestDataDirExists(final HBaseTestingUtility testingUtility)
      throws IOException {
    final Path testDataDir = Paths.get(testingUtility.getDataTestDir().toString());
    final File testDataDirFile = Files.createDirectories(testDataDir).toFile();
    assertTrue(testDataDirFile.exists());
    return testDataDirFile;
  }

  private static Configuration createConfiguration() {
    final Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.coprocessor.master.classes", ShellExecEndpointCoprocessor.class.getName());
    return conf;
  }
}

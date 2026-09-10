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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ShellExecEndpoint.ShellExecService;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test for the {@link ShellExecEndpointCoprocessor}.
 */
@Tag(MediumTests.TAG)
public class TestShellExecEndpointCoprocessor {

  private static HBaseTestingUtil testingUtility;
  private AsyncConnection conn;

  @BeforeAll
  public static void setUp() throws Exception {
    testingUtility = new HBaseTestingUtil();
    testingUtility.getConfiguration().set("hbase.coprocessor.master.classes",
      ShellExecEndpointCoprocessor.class.getName());
    testingUtility.startMiniCluster();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUpConnection() throws Exception {
    conn = ConnectionFactory.createAsyncConnection(testingUtility.getConfiguration()).get();
  }

  @AfterEach
  public void tearDownConnection() throws IOException {
    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void testShellExecUnspecified() {
    testShellExecForeground(b -> {
    });
  }

  @Test
  public void testShellExecForeground() {
    testShellExecForeground(b -> b.setAwaitResponse(true));
  }

  private void testShellExecForeground(final Consumer<ShellExecRequest.Builder> consumer) {
    final AsyncAdmin admin = conn.getAdmin();

    final String command = "echo -n \"hello world\"";
    final ShellExecRequest.Builder builder = ShellExecRequest.newBuilder().setCommand(command);
    consumer.accept(builder);
    final ShellExecResponse resp =
      admin.<ShellExecService.Stub, ShellExecResponse> coprocessorService(ShellExecService::newStub,
        (stub, controller, callback) -> stub.shellExec(controller, builder.build(), callback))
        .join();
    assertEquals(0, resp.getExitCode());
    assertEquals("hello world", resp.getStdout());
  }

  @Test
  public void testShellExecBackground() throws IOException {
    final AsyncAdmin admin = conn.getAdmin();

    final File testDataDir = ensureTestDataDirExists(testingUtility);
    final File testFile = new File(testDataDir, "shell_exec_background.txt");
    assertTrue(testFile.createNewFile());
    assertEquals(0, testFile.length());

    final String command = "echo \"hello world\" >> " + testFile.getAbsolutePath();
    final ShellExecRequest req =
      ShellExecRequest.newBuilder().setCommand(command).setAwaitResponse(false).build();
    final ShellExecResponse resp =
      admin.<ShellExecService.Stub, ShellExecResponse> coprocessorService(ShellExecService::newStub,
        (stub, controller, callback) -> stub.shellExec(controller, req, callback)).join();

    assertFalse(resp.hasExitCode(), "the response from a background task should have no exit code");
    assertFalse(resp.hasStdout(), "the response from a background task should have no stdout");
    assertFalse(resp.hasStderr(), "the response from a background task should have no stderr");

    Waiter.waitFor(conn.getConfiguration(), 5_000, () -> testFile.length() > 0);
    final String content =
      new String(Files.readAllBytes(testFile.toPath()), StandardCharsets.UTF_8).trim();
    assertEquals("hello world", content);
  }

  private static File ensureTestDataDirExists(final HBaseTestingUtil testingUtility)
    throws IOException {
    final Path testDataDir = Optional.of(testingUtility).map(HBaseTestingUtil::getDataTestDir)
      .map(Object::toString).map(Paths::get)
      .orElseThrow(() -> new RuntimeException("Unable to locate temp directory path."));
    final File testDataDirFile = Files.createDirectories(testDataDir).toFile();
    assertTrue(testDataDirFile.exists());
    return testDataDirFile;
  }
}

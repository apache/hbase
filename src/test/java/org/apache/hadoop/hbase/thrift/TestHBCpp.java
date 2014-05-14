/*
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
package org.apache.hadoop.hbase.thrift;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This class defines testcases for HBCpp client. It spawns an executable binary
 * compiled from C++ code on fbcode, named SimpleClient.
 *
 * Two environment variables are used to find the binary:
 * FBCODE_DIR
 * If none of them is defined, the composed path points to a binary maintained
 * by contbuild (intern/wiki/index.php/Fbcode_Continuous_Build).
 * During debugging new client testcases, set the variable as follows:
 * FBCODE_DIR The root of fbcode
 *
 * Before testing, a MiniCluster is started, and a table named "t1" with a
 * family of "f1" is created. Then the address(and port) of zookeeper is passed
 * to the binary using --hbase flag.
 */
@Category(MediumTests.class)
public class TestHBCpp {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);

    TEST_UTIL.getConfiguration().set(HBaseTestingUtility.FS_TYPE_KEY,
        HBaseTestingUtility.FS_TYPE_LFS);

    TEST_UTIL.startMiniCluster();
    // create the table as SimpleClient assumes.
    byte[] tableName = Bytes.toBytes("t1");
    byte[] family = Bytes.toBytes("f1");
    TEST_UTIL.createTable(tableName, family);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 120000L)
  /**
   * Spawn the current version of client unit tests from fbcode.
   */
  public void testSimpleClient() throws Exception {
    // Allow the developer to override the default fbcode build location.
    String fbcodeDir = System.getProperty("fbcode.root",
        "/home/engshare/contbuild/fbcode/hbase");

    executeCommand(new String[] {
        fbcodeDir + "/_bin/hbase/hbcpp/SimpleClient",
        "--hbase",
        "localhost:"
            + TEST_UTIL.getConfiguration().getInt(
                HConstants.ZOOKEEPER_CLIENT_PORT,
                HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT), "" });
  }

  private void executeCommand(String[] command) throws IOException,
      InterruptedException {
    LOG.debug("Command : " + Arrays.toString(command));

    Process p = Runtime.getRuntime().exec(command);

    final BufferedReader stdInput = new BufferedReader(new InputStreamReader(
        p.getInputStream()));
    BufferedReader stdError = new BufferedReader(new InputStreamReader(
        p.getErrorStream()));

    // a new thread is spawned so that stdout and stderr information is shown
    // more similar to the original order.
    new Thread() {
      @Override
      public void run() {
        try {
          // Read the output from the command
          while (true) {
            String s = stdInput.readLine();
            if (s == null) {
              break;
            }
            System.out.println("[CLIENT] " + s);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    // Read any errors from the attempted command
    while (true) {
      String s = stdError.readLine();
      if (s == null) {
        break;
      }
      System.out.println("[CLIENT] " + s);
    }

    // Exit code of 0 means success.
    Assert.assertEquals("Exit Code", 0, p.waitFor());
  }
}

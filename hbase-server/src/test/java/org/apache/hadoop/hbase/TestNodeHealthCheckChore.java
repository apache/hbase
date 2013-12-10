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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestNodeHealthCheckChore {

  private static final Log LOG = LogFactory.getLog(TestNodeHealthCheckChore.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int SCRIPT_TIMEOUT = 5000;
  private File healthScriptFile;
  private String eol = System.getProperty("line.separator");

  @After
  public void cleanUp() throws IOException {
    // delete and recreate the test directory, ensuring a clean test dir between tests
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    fs.delete(testDir, true);
    if (!fs.mkdirs(testDir)) throw new IOException("Failed mkdir " + testDir);
  }

  @Test(timeout=60000)
  public void testHealthCheckerSuccess() throws Exception {
    String normalScript = "echo \"I am all fine\"";
    healthCheckerTest(normalScript, HealthCheckerExitStatus.SUCCESS);
  }

  @Test(timeout=60000)
  public void testHealthCheckerFail() throws Exception {
    String errorScript = "echo ERROR" + eol + "echo \"Node not healthy\"";
    healthCheckerTest(errorScript, HealthCheckerExitStatus.FAILED);
  }

  @Test(timeout=60000)
  public void testHealthCheckerTimeout() throws Exception {
    String timeOutScript = "sleep 10" + eol + "echo \"I am fine\"";
    healthCheckerTest(timeOutScript, HealthCheckerExitStatus.TIMED_OUT);
  }

  public void healthCheckerTest(String script, HealthCheckerExitStatus expectedStatus)
      throws Exception {
    Configuration config = getConfForNodeHealthScript();
    config.addResource(healthScriptFile.getName());
    String location = healthScriptFile.getAbsolutePath();
    long timeout = config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT, SCRIPT_TIMEOUT);

    HealthChecker checker = new HealthChecker();
    checker.init(location, timeout);

    createScript(script, true);
    HealthReport report = checker.checkHealth();
    assertEquals(expectedStatus, report.getStatus());

    LOG.info("Health Status:" + report.getHealthReport());

    this.healthScriptFile.delete();
  }

  @Test(timeout=60000)
  public void testRSHealthChore() throws Exception{
    Stoppable stop = new StoppableImplementation();
    Configuration conf = getConfForNodeHealthScript();
    String errorScript = "echo ERROR" + eol + " echo \"Server not healthy\"";
    createScript(errorScript, true);
    HealthCheckChore rsChore = new HealthCheckChore(100, stop, conf);
    try {
      //Default threshold is three.
      rsChore.chore();
      rsChore.chore();
      assertFalse("Stoppable must not be stopped.", stop.isStopped());
      rsChore.chore();
      assertTrue("Stoppable must have been stopped.", stop.isStopped());
    } finally {
      stop.stop("Finished w/ test");
    }
  }

  private void createScript(String scriptStr, boolean setExecutable)
      throws Exception {
    if (!this.healthScriptFile.exists()) {
      if (!healthScriptFile.createNewFile()) {
        throw new IOException("Failed create of " + this.healthScriptFile);
      }
    }
    PrintWriter pw = new PrintWriter(new FileOutputStream(healthScriptFile));
    try {
      pw.println(scriptStr);
      pw.flush();
    } finally {
      pw.close();
    }
    healthScriptFile.setExecutable(setExecutable);
    LOG.info("Created " + this.healthScriptFile + ", executable=" + setExecutable);
  }

  private Configuration getConfForNodeHealthScript() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    File tempDir = new File(UTIL.getDataTestDir().toString());
    if (!tempDir.exists()) {
      if (!tempDir.mkdirs()) {
        throw new IOException("Failed mkdirs " + tempDir);
      }
    }
    String scriptName = "HealthScript" + UUID.randomUUID().toString()
        + (Shell.WINDOWS ? ".cmd" : ".sh");
    healthScriptFile = new File(tempDir.getAbsolutePath(), scriptName);
    conf.set(HConstants.HEALTH_SCRIPT_LOC, healthScriptFile.getAbsolutePath());
    conf.setLong(HConstants.HEALTH_FAILURE_THRESHOLD, 3);
    conf.setLong(HConstants.HEALTH_SCRIPT_TIMEOUT, SCRIPT_TIMEOUT);
    return conf;
  }

  /**
   * Simple helper class that just keeps track of whether or not its stopped.
   */
  private static class StoppableImplementation implements Stoppable {
    private volatile boolean stop = false;

    @Override
    public void stop(String why) {
      this.stop = true;
    }

    @Override
    public boolean isStopped() {
      return this.stop;
    }

  }
}

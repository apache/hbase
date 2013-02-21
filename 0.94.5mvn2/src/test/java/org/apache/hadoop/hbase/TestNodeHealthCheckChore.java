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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestNodeHealthCheckChore {

  private static final Log LOG = LogFactory.getLog(TestNodeHealthCheckChore.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private File healthScriptFile;


  @After
  public void cleanUp() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testHealthChecker() throws Exception {
    Configuration config = getConfForNodeHealthScript();
    config.addResource(healthScriptFile.getName());
    String location = healthScriptFile.getAbsolutePath();
    long timeout = config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT, 200);

    HealthChecker checker = new HealthChecker();
    checker.init(location, timeout);

    String normalScript = "echo \"I am all fine\"";
    createScript(normalScript, true);
    HealthReport report = checker.checkHealth();
    assertEquals(HealthCheckerExitStatus.SUCCESS, report.getStatus());

    LOG.info("Health Status:" + checker);

    String errorScript = "echo ERROR\n echo \"Node not healthy\"";
    createScript(errorScript, true);
    report = checker.checkHealth();
    assertEquals(HealthCheckerExitStatus.FAILED, report.getStatus());
    LOG.info("Health Status:" + report.getHealthReport());

    String timeOutScript = "sleep 4\n echo\"I am fine\"";
    createScript(timeOutScript, true);
    report = checker.checkHealth();
    assertEquals(HealthCheckerExitStatus.TIMED_OUT, report.getStatus());
    LOG.info("Health Status:" + report.getHealthReport());

    healthScriptFile.delete();
  }

  @Test
  public void testNodeHealthChore() throws Exception{
    Stoppable stop = new StoppableImplementation();
    Configuration conf = getConfForNodeHealthScript();
    String errorScript = "echo ERROR\n echo \"Node not healthy\"";
    createScript(errorScript, true);
    HealthCheckChore rsChore = new HealthCheckChore(100, stop, conf);
    //Default threshold is three.
    rsChore.chore();
    rsChore.chore();
    assertFalse("Stoppable must not be stopped.", stop.isStopped());
    rsChore.chore();
    assertTrue("Stoppable must have been stopped.", stop.isStopped());
  }

  private void createScript(String scriptStr, boolean setExecutable)
      throws Exception {
    healthScriptFile.createNewFile();
    PrintWriter pw = new PrintWriter(new FileOutputStream(healthScriptFile));
    pw.println(scriptStr);
    pw.flush();
    pw.close();
    healthScriptFile.setExecutable(setExecutable);
  }

  private Configuration getConfForNodeHealthScript() {
    Configuration conf = UTIL.getConfiguration();
    File tempDir = new File(UTIL.getDataTestDir().toString());
    tempDir.mkdirs();
    healthScriptFile = new File(tempDir.getAbsolutePath(), "HealthScript.sh");
    conf.set(HConstants.HEALTH_SCRIPT_LOC,
      healthScriptFile.getAbsolutePath());
    conf.setLong(HConstants.HEALTH_FAILURE_THRESHOLD, 3);
    conf.setLong(HConstants.HEALTH_SCRIPT_TIMEOUT, 200);
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

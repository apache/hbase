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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.assertTrue;


/**
 * Tests for master and regionserver coprocessor stop method
 *
 */
@Category(MediumTests.class)
public class TestCoprocessorStop {
  private static final Log LOG = LogFactory.getLog(TestCoprocessorStop.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String MASTER_FILE =
                              "master" + System.currentTimeMillis();
  private static final String REGIONSERVER_FILE =
                              "regionserver" + System.currentTimeMillis();

  public static class FooCoprocessor implements Coprocessor {
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      String where = null;

      if (env instanceof MasterCoprocessorEnvironment) {
        // if running on HMaster
        where = "master";
      } else if (env instanceof RegionServerCoprocessorEnvironment) {
        where = "regionserver";
      } else if (env instanceof RegionCoprocessorEnvironment) {
        LOG.error("on RegionCoprocessorEnvironment!!");
      }
      LOG.info("start coprocessor on " + where);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
      String fileName = null;

      if (env instanceof MasterCoprocessorEnvironment) {
        // if running on HMaster
        fileName = MASTER_FILE;
      } else if (env instanceof RegionServerCoprocessorEnvironment) {
        fileName = REGIONSERVER_FILE;
      } else if (env instanceof RegionCoprocessorEnvironment) {
        LOG.error("on RegionCoprocessorEnvironment!!");
      }

      Configuration conf = UTIL.getConfiguration();
      Path resultFile = new Path(UTIL.getDataTestDirOnTestFS(), fileName);
      FileSystem fs = FileSystem.get(conf);

      boolean result = fs.createNewFile(resultFile);
      LOG.info("create file " + resultFile + " return rc " + result);
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();

    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      FooCoprocessor.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      FooCoprocessor.class.getName());

    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStopped() throws Exception {
    //shutdown hbase only. then check flag file.
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    LOG.info("shutdown hbase cluster...");
    cluster.shutdown();
    LOG.info("wait for the hbase cluster shutdown...");
    cluster.waitUntilShutDown();

    Configuration conf = UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    Path resultFile = new Path(UTIL.getDataTestDirOnTestFS(), MASTER_FILE);
    assertTrue("Master flag file should have been created",fs.exists(resultFile));

    resultFile = new Path(UTIL.getDataTestDirOnTestFS(), REGIONSERVER_FILE);
    assertTrue("RegionServer flag file should have been created",fs.exists(resultFile));
  }
}

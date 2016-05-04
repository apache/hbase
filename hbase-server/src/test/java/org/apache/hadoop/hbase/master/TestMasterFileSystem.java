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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the master filesystem in a local cluster
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterFileSystem {
  private static final Log LOG = LogFactory.getLog(TestMasterFileSystem.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFsUriSetProperly() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterFileSystem fs = master.getMasterFileSystem();
    Path masterRoot = FSUtils.getRootDir(fs.getConfiguration());
    Path rootDir = FSUtils.getRootDir(fs.getFileSystem().getConf());
    // make sure the fs and the found root dir have the same scheme
    LOG.debug("from fs uri:" + FileSystem.getDefaultUri(fs.getFileSystem().getConf()));
    LOG.debug("from configuration uri:" + FileSystem.getDefaultUri(fs.getConfiguration()));
    // make sure the set uri matches by forcing it.
    assertEquals(masterRoot, rootDir);
  }
}

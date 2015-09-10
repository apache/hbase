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
package org.apache.hadoop.hbase.fs.layout;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, MiscTests.class})
public class TestFsLayout {
  private static Configuration conf;
  private static HBaseTestingUtility TEST_UTIL;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      TEST_UTIL.shutdownMiniCluster();
    } finally {
      FsLayout.reset();
    }
  }
  
  @Test
  public void testLayoutInitialization() throws Exception {
    FsLayout.reset();
    assertNull(FsLayout.getRaw());
    
    HierarchicalFsLayout hierarchicalLayout = HierarchicalFsLayout.get();
    FsLayout.setLayoutForTesting(hierarchicalLayout);
    
    assertTrue(FsLayout.getRaw() instanceof HierarchicalFsLayout);
    assertTrue(FsLayout.get() instanceof HierarchicalFsLayout);
    
    FsLayout.reset();
    assertNull(FsLayout.getRaw());
    
    DistributedFileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rootDir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());
    
    FsLayout.writeLayoutFile(fs, rootDir, hierarchicalLayout, true);
    assertNull(FsLayout.getRaw());
    conf.setBoolean(FsLayout.FS_LAYOUT_DETECT, true);
    FsLayout.initialize(conf);
    assertNotNull(FsLayout.getRaw());
    assertTrue(FsLayout.get() instanceof HierarchicalFsLayout);
    assertTrue(FsLayout.getRaw() instanceof HierarchicalFsLayout);
    
    FsLayout.reset();
    FsLayout.deleteLayoutFile(fs, rootDir);
    FsLayout.initialize(conf);
    assertTrue(FsLayout.get() instanceof StandardHBaseFsLayout);
    assertTrue(FsLayout.getRaw() instanceof StandardHBaseFsLayout);
    
    FsLayout.reset();
    conf.setBoolean(FsLayout.FS_LAYOUT_DETECT_STRICT, true);
    try {
      FsLayout.initialize(conf);
      assertTrue(false);
    } catch (IllegalStateException e) {
      // Should be thrown by initialize
    }
    
    FsLayout.reset();
    conf.setBoolean(FsLayout.FS_LAYOUT_DETECT, false);
    conf.setBoolean(FsLayout.FS_LAYOUT_DETECT_STRICT, false);
    FsLayout.writeLayoutFile(fs, rootDir, StandardHBaseFsLayout.get(), true);
    conf.set(FsLayout.FS_LAYOUT_CHOICE, HierarchicalFsLayout.class.getName());
    assertTrue(FsLayout.get() instanceof StandardHBaseFsLayout);
  }
}

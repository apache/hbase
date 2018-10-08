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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Category({MasterTests.class, SmallTests.class})
public class TestMasterWALManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestMasterWALManager.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterWALManager.class);

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  private MasterWalManager mwm;
  private MasterServices masterServices;

  @Before
  public void before() throws IOException {
    MasterFileSystem mfs = Mockito.mock(MasterFileSystem.class);
    Mockito.when(mfs.getWALFileSystem()).thenReturn(HTU.getTestFileSystem());
    Path walRootDir = HTU.createWALRootDir();
    Mockito.when(mfs.getWALRootDir()).thenReturn(walRootDir);
    this.masterServices = Mockito.mock(MasterServices.class);
    Mockito.when(this.masterServices.getConfiguration()).thenReturn(HTU.getConfiguration());
    Mockito.when(this.masterServices.getMasterFileSystem()).thenReturn(mfs);
    Mockito.when(this.masterServices.getServerName()).
        thenReturn(ServerName.parseServerName("master.example.org,0123,456"));
    this.mwm = new MasterWalManager(this.masterServices);
  }

  @Test
  public void testIsWALDirectoryNameWithWALs() throws IOException {
    ServerName sn = ServerName.parseServerName("x.example.org,1234,5678");
    assertFalse(this.mwm.isWALDirectoryNameWithWALs(sn));
    FileSystem walFS = this.masterServices.getMasterFileSystem().getWALFileSystem();
    Path dir = new Path(this.mwm.getWALDirPath(), sn.toString());
    assertTrue(walFS.mkdirs(dir));
    assertTrue(this.mwm.isWALDirectoryNameWithWALs(sn));
    // Make sure works when dir is SPLITTING
    Set<ServerName> sns = new HashSet<ServerName>();
    sns.add(sn);
    List<Path> paths = this.mwm.createAndGetLogDirs(sns);
    assertTrue(this.mwm.isWALDirectoryNameWithWALs(sn));
  }
}

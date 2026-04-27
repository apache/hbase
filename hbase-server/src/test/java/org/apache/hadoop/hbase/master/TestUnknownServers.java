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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MediumTests.TAG)
public class TestUnknownServers {

  private static HBaseTestingUtil UTIL;
  private static Admin ADMIN;
  private final static int SLAVES = 1;
  private static boolean IS_UNKNOWN_SERVER = true;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtil();
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL,
      TestUnknownServers.HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(SLAVES);
    ADMIN = UTIL.getAdmin();
  }

  @Test
  public void testListUnknownServers() throws Exception {
    assertEquals(ADMIN.listUnknownServers().size(), SLAVES);
    IS_UNKNOWN_SERVER = false;
    assertEquals(ADMIN.listUnknownServers().size(), 0);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) {
      ADMIN.close();
    }
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected ServerManager createServerManager(MasterServices master, RegionServerList storage)
      throws IOException {
      setupClusterConnection();
      return new TestUnknownServers.ServerManagerForTest(master, storage);
    }
  }

  private static final class ServerManagerForTest extends ServerManager {

    public ServerManagerForTest(MasterServices master, RegionServerList storage) {
      super(master, storage);
    }

    @Override
    public boolean isServerUnknown(ServerName serverName) {
      return IS_UNKNOWN_SERVER;
    }
  }
}

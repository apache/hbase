/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestMasterOnlineConfigChange {

  static final Log LOG =
          LogFactory.getLog(TestMasterOnlineConfigChange.class.getName());
  HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();
  Configuration conf = null;
  HMaster master;


  @Before
  public void setUp() throws Exception {
    conf = hbaseTestingUtility.getConfiguration();
    hbaseTestingUtility.startMiniCluster(1,1);
    master = hbaseTestingUtility.getHBaseCluster().getActiveMaster();
  }


  /**
   * Check if the server side profiling config parameter changes online
   * @throws IOException
   */
  @Test
  public void testServerManagerResendMessagesChange() throws IOException {
    ServerManager serverMgr = master.getServerManager();
    boolean origSetting =
        serverMgr.getResendDroppedMessages();

    conf.setBoolean("hbase.master.msgs.resend-openclose",
                    !origSetting);

    // Confirm that without the notification call, the parameter is unchanged
    assertEquals(origSetting,
        serverMgr.getResendDroppedMessages());

    // After the notification, it should be changed
    master.getConfigurationManager().notifyAllObservers(conf);
    assertEquals(!origSetting,
        serverMgr.getResendDroppedMessages());
  }
}

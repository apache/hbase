package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestTransactionLogCreator {

  @Test
  public void testNewLogfileCreation() throws Exception {
    String logDir = "/tmp/testNewLogfileCreation";
    File logDirFile = new File(logDir);
    if (logDirFile.exists()) {
      FileUtils.deleteDirectory(logDirFile);
    }
    logDirFile.mkdir();
    Configuration conf = new Configuration();
    conf.set(HConstants.RAFT_TRANSACTION_LOG_DIRECTORY_KEY, logDir);
    TransactionLogManager logManager =
        new TransactionLogManager(conf, "test", HConstants.UNDEFINED_TERM_INDEX);
    logManager.initialize(null);
    // Wait for new logs to be created
    Thread.sleep(1000);
    File currentLogDir = new File(logDir + "/test/current/");
    File[] files = currentLogDir.listFiles();
    int expected = HConstants.RAFT_MAX_NUM_NEW_LOGS;
    assertEquals("# of new log files", expected, files.length);

    logManager.forceRollLog();
    Thread.sleep(1000);
    files = currentLogDir.listFiles();
    expected++;
    assertEquals("# of new log files", expected, files.length);
  }
}

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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.MockAesKeyProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
public class TestSecureWALReplay extends AbstractTestWALReplay {

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = AbstractTestWALReplay.TEST_UTIL.getConfiguration();
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockAesKeyProvider.class.getName());
    conf.set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    conf.setBoolean(HConstants.ENABLE_WAL_ENCRYPTION, true);
    TestWALReplay.setUpBeforeClass();
  }

  @Override
  protected WAL createWAL(Configuration c, Path hbaseRootDir, String logName) throws IOException {
    return TestWALReplay.createFSHLog(c, hbaseRootDir, logName);
  }
}

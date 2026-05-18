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
package org.apache.hadoop.hbase.tool;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * Test cases for the atomic load error handling of the bulk load functionality.
 */
@Tag(MiscTests.TAG)
@Tag(LargeTests.TAG)
public class TestBulkLoadHFilesSplitRecovery extends BulkLoadHFilesSplitRecoveryTestBase {

  @BeforeAll
  public static void setupCluster() throws Exception {
    util = new HBaseTestingUtil();
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    util.startMiniCluster(1);
  }
}

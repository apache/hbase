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

package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Run the same test as TestReplicationKillMasterRS but with WAL compression enabled
 * Do not add other tests in this class.
 */
@Category(LargeTests.class)
public class TestReplicationKillMasterRSCompressed extends TestReplicationKillMasterRS {

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    TestReplicationBase.setUpBeforeClass();
  }
}

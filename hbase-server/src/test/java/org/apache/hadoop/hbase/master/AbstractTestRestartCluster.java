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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestRestartCluster {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestRestartCluster.class);

  protected HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static final TableName[] TABLES = { TableName.valueOf("restartTableOne"),
    TableName.valueOf("restartTableTwo"), TableName.valueOf("restartTableThree") };

  protected static final byte[] FAMILY = Bytes.toBytes("family");

  protected abstract boolean splitWALCoordinatedByZk();

  @Before
  public void setUp() {
    boolean splitWALCoordinatedByZk = splitWALCoordinatedByZk();
    LOG.info("WAL splitting coordinated by zk {}", splitWALCoordinatedByZk);
    UTIL.getConfiguration().setBoolean(HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
      splitWALCoordinatedByZk);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}

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
package org.apache.hadoop.hbase.client;

import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestConnectionFactoryExecService {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestConnectionFactoryExecService.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void before() throws Exception {
    conf = UTIL.getConfiguration();
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testConnectionWithTPE() throws IOException {
    try(Connection conn = ConnectionFactory.createConnection(conf, new ThreadPoolExecutor( 1,  1, 60L,
      TimeUnit.SECONDS, new LinkedBlockingQueue<>()))) {
      assertTrue(conn.getAdmin().tableExists(TableName.valueOf("hbase:meta")));
    }
  }

  @Test
  public void testConnectionWithFJP() throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf, new ForkJoinPool(1))) {
      assertTrue(conn.getAdmin().tableExists(TableName.valueOf("hbase:meta")));
    }
  }
}

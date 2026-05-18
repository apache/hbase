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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestConnectionCache {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  /**
   * test for ConnectionCache cleaning expired Connection
   */
  @Test
  public void testConnectionChore() throws Exception {
    // 1s for clean interval & 5s for maxIdleTime
    ConnectionCache cache = new ConnectionCache(UTIL.getConfiguration(),
      UserProvider.instantiate(UTIL.getConfiguration()), 1000, 5000);
    ConnectionCache.ConnectionInfo info = cache.getCurrentConnection();

    assertEquals(false, info.connection.isClosed());

    Thread.sleep(7000);

    assertEquals(true, info.connection.isClosed());
  }
}

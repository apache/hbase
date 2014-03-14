/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestServerConfigFromClient {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static int SLAVES = 3;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // set up a custom new configuration to make sure it propagates
    TEST_UTIL.getConfiguration().set("clientconfquery", "Hello World");
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  // Marked as unstable and recorded in #3922005
  @TestTag({ "unstable" })
  @Test
  public void testReadConfig() throws IOException {
    final byte [] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte [] SMALL_FAMILY = Bytes.toBytes("smallfam");
    HTable table = TEST_UTIL.createTable(Bytes.toBytes("testReadConfig"),
      new byte [][] {CONTENTS_FAMILY, SMALL_FAMILY});
    String val1 = table.getServerConfProperty("hbase.regionserver.class");
    assertEquals("test1", "org.apache.hadoop.hbase.ipc.HRegionInterface", val1);
    String val2 = table.getServerConfProperty("clientconfquery");
    assertEquals("test2", "Hello World", val2);
    // should return null for nonexistent properties
    String val3 = table.getServerConfProperty("notaproperty");
    assertEquals("test3", null, val3);
  }

}

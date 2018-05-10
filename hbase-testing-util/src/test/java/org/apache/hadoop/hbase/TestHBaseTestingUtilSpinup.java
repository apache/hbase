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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertFalse;

import java.util.List;

import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Make sure we can spin up a HBTU without a hbase-site.xml
 */
@Category({MiscTests.class, MediumTests.class})
public class TestHBaseTestingUtilSpinup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseTestingUtilSpinup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseTestingUtilSpinup.class);
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    if (!UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000)) {
      throw new RuntimeException("Active master not ready");
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetMetaTableRows() throws Exception {
    List<byte[]> results = UTIL.getMetaTableRows();
    assertFalse("results should have some entries and is empty.", results.isEmpty());
  }

}

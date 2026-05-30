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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Make sure we can spin up a HBTU without a hbase-site.xml
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestHBaseTestingUtilitySpinup {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    if (!UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000)) {
      throw new RuntimeException("Active master not ready");
    }
  }

  @AfterAll
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetMetaTableRows() throws Exception {
    List<byte[]> results = UTIL.getMetaTableRows();
    assertFalse(results.isEmpty(), "results should have some entries and is empty.");
  }

}

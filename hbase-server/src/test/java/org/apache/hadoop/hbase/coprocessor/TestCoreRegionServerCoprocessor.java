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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MockRegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

/**
 * Test CoreCoprocessor Annotation works giving access to facility not usually available. Test
 * RegionServerCoprocessor.
 */
@Tag(CoprocessorTests.TAG)
@Tag(SmallTests.TAG)
public class TestCoreRegionServerCoprocessor {

  private String currentTestName;
  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private RegionServerServices rss;
  private RegionServerCoprocessorHost rsch;

  @BeforeEach
  public void before(TestInfo testInfo) throws IOException {
    currentTestName = testInfo.getTestMethod().get().getName();
    String methodName = this.currentTestName;
    this.rss = new MockRegionServerServices(HTU.getConfiguration());
    this.rsch = new RegionServerCoprocessorHost(this.rss, HTU.getConfiguration());
  }

  @AfterEach
  public void after() throws IOException {
    this.rsch.preStop("Stopping", null);
  }

  /**
   * No annotation with CoreCoprocessor. This should make it so I can NOT get at instance of a
   * RegionServerServices instance after some gymnastics.
   */
  public static class NotCoreRegionServerCoprocessor implements RegionServerCoprocessor {
    public NotCoreRegionServerCoprocessor() {
    }
  }

  /**
   * Annotate with CoreCoprocessor. This should make it so I can get at instance of a
   * RegionServerServices instance after some gymnastics.
   */
  @CoreCoprocessor
  public static class CoreRegionServerCoprocessor implements RegionServerCoprocessor {
    public CoreRegionServerCoprocessor() {
    }
  }

  /**
   * Assert that when a Coprocessor is annotated with CoreCoprocessor, then it is possible to access
   * a RegionServerServices instance. Assert the opposite too. Do it to RegionServerCoprocessors.
   */
  @Test
  public void testCoreRegionCoprocessor() throws IOException {
    RegionServerCoprocessorEnvironment env =
      rsch.load(null, NotCoreRegionServerCoprocessor.class.getName(), 0, HTU.getConfiguration());
    assertFalse(env instanceof HasRegionServerServices);
    env = rsch.load(null, CoreRegionServerCoprocessor.class.getName(), 1, HTU.getConfiguration());
    assertTrue(env instanceof HasRegionServerServices);
    assertEquals(this.rss, ((HasRegionServerServices) env).getRegionServerServices());
  }
}

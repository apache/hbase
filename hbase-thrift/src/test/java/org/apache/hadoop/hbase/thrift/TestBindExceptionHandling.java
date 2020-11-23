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
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.assertNotNull;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;

@Category({ ClientTests.class, MediumTests.class})
public class TestBindExceptionHandling {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBindExceptionHandling.class);

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  /**
   * See if random port choosing works around protocol port clashes
   */
  @Test
  public void testProtocolPortClash() throws Exception {
    try (ThriftServerRunner tsr = TestThriftServerCmdLine.
        createBoundServer(() -> new ThriftServer(HTU.getConfiguration()), true, false)) {
      assertNotNull(tsr.getThriftServer());
    }
  }

  /**
   * See if random port choosing works around protocol port clashes
   */
  @Test
  public void testInfoPortClash() throws Exception {
    try (ThriftServerRunner tsr = TestThriftServerCmdLine.
        createBoundServer(() -> new ThriftServer(HTU.getConfiguration()), false, true)) {
      assertNotNull(tsr.getThriftServer());
    }
  }
}

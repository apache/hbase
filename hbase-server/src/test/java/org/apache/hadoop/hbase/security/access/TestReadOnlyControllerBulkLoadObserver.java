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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Tests methods of BulkLoad Observer which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies
@Category({ SecurityTests.class, SmallTests.class })
public class TestReadOnlyControllerBulkLoadObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerBulkLoadObserver.class);

  ReadOnlyController readOnlyController;
  HBaseConfiguration readOnlyConf;

  // Region Server Coprocessor mocking variables
  ObserverContext<RegionCoprocessorEnvironment> ctx;

  @Before
  public void setup() throws Exception {
    readOnlyController = new ReadOnlyController();
    readOnlyConf = new HBaseConfiguration();
    readOnlyConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);

    // mocking variables initialization
    ctx = mock(ObserverContext.class);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test(expected = IOException.class)
  public void testPrePrepareBulkLoadReadOnlyException() throws IOException {
    readOnlyController.onConfigurationChange(readOnlyConf);
    readOnlyController.prePrepareBulkLoad(ctx);
  }

  @Test
  public void testPrePrepareBulkLoadNoException() throws IOException {
    readOnlyController.prePrepareBulkLoad(ctx);
  }

  @Test(expected = IOException.class)
  public void testPreCleanupBulkLoadReadOnlyException() throws IOException {
    readOnlyController.onConfigurationChange(readOnlyConf);
    readOnlyController.preCleanupBulkLoad(ctx);
  }

  @Test
  public void testPreCleanupBulkLoadNoException() throws IOException {
    readOnlyController.preCleanupBulkLoad(ctx);
  }
}

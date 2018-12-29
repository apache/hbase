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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.USER_COPROCESSORS_ENABLED_CONF_KEY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({MediumTests.class})
@RunWith(Parameterized.class)
public class TestRegionCoprocessorHost {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final boolean skip;

  @Parameters
  public static List<Object> params() {
    return Arrays.asList(new Object[] { new Boolean(true), new Boolean(false) });
  }

  public TestRegionCoprocessorHost(boolean skip) {
    this.skip = skip;
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(COPROCESSORS_ENABLED_CONF_KEY, true);
    conf.setBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY, true);
    conf.setBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR, skip);
    conf.set(REGION_COPROCESSOR_CONF_KEY, SimpleRegionObserver.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLoadDuplicateCoprocessor() throws Exception {
    TableName table = TableName.valueOf("testLoadDuplicateCoprocessor");
    HTableDescriptor tableDesc = new HTableDescriptor(table);
    tableDesc.addFamily(new HColumnDescriptor("cf"));
    tableDesc.addCoprocessor(TestRegionObserver.class.getName());

    // Only one coprocessor SimpleRegionObserver loaded if skipping, two otherwise
    TEST_UTIL.getHBaseAdmin().createTable(tableDesc);
    assertEquals(skip ? 1 : 2, TestRegionObserver.getNumInstances());
  }

  public static final class TestRegionObserver extends SimpleRegionObserver {
    public static final AtomicInteger numInstances = new AtomicInteger(0);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      super.start(e);
      numInstances.incrementAndGet();
    }

    public static int getNumInstances() {
      return numInstances.get();
    }
  }
}
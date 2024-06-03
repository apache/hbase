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
package org.apache.hadoop.hbase.wal;

import java.util.Arrays;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestCompressedWAL extends CompressedWALTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompressedWAL.class);

  @Parameter
  public String walProvider;

  @Parameters(name = "{index}: provider={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { "defaultProvider" }, new Object[] { "asyncfs" });
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

}

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
package org.apache.hadoop.hbase.mapreduce;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MapReduceTests.class, LargeTests.class })
public class TestMRIncrementalLoadWithLocality extends MRIncrementalLoadTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMRIncrementalLoad.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupCluster(true);
  }

  @Parameters(name = "{index}: shouldChangeRegions={0}, putSortReducer={1}," + " tableStr={2}")
  public static List<Object[]> params() {
    return Arrays.asList(
      new Object[] { false, false, Arrays.asList("testMRIncrementalLoadWithLocality1") },
      new Object[] { true, false, Arrays.asList("testMRIncrementalLoadWithLocality2") });
  }
}

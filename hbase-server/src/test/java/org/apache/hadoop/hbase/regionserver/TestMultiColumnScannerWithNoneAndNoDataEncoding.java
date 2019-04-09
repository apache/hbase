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
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test case for Compression.Algorithm.NONE and no use data block encoding.
 * @see org.apache.hadoop.hbase.regionserver.TestMultiColumnScanner
 */
@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, LargeTests.class })
public class TestMultiColumnScannerWithNoneAndNoDataEncoding extends TestMultiColumnScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiColumnScannerWithNoneAndNoDataEncoding.class);

  @Parameters
  public static Collection<Object[]> parameters() {
    return TestMultiColumnScanner.generateParams(Algorithm.NONE, false);
  }
}

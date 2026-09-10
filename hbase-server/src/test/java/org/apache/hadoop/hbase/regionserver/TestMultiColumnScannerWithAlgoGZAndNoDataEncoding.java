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
package org.apache.hadoop.hbase.regionserver;

import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Test case for Compression.Algorithm.GZ and no use data block encoding.
 * @see org.apache.hadoop.hbase.regionserver.TestMultiColumnScanner
 */
@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: algo={0}, bloomType={1}, dataBlockEncoding={2}")
public class TestMultiColumnScannerWithAlgoGZAndNoDataEncoding extends TestMultiColumnScanner {

  public TestMultiColumnScannerWithAlgoGZAndNoDataEncoding(Algorithm comprAlgo, BloomType bloomType,
    DataBlockEncoding dataBlockEncoding) {
    super(comprAlgo, bloomType, dataBlockEncoding);
  }

  public static Stream<Arguments> parameters() {
    return TestMultiColumnScanner.generateParams(Algorithm.GZ, false);
  }
}

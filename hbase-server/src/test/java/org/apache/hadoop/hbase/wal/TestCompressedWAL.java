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

import java.util.stream.Stream;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
@ParameterizedClass(name = "{index}: provider={0}")
@MethodSource("parameters")
public class TestCompressedWAL extends CompressedWALTestBase {

  public String walProvider;

  public TestCompressedWAL(String walProvider) {
    this.walProvider = walProvider;
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of("defaultProvider"), Arguments.of("asyncfs"));
  }

  @BeforeEach
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walProvider);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterEach
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

}

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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

/**
 * A write/read/verify load test on a mini HBase cluster. Tests reading and writing at the same
 * time.
 */
@Tag(MiscTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate
public class TestMiniClusterLoadParallel extends TestMiniClusterLoadSequential {

  public static Stream<Arguments> parameters() {
    return TestMiniClusterLoadSequential.parameters();
  }

  public TestMiniClusterLoadParallel(boolean isMultiPut, DataBlockEncoding encoding) {
    super(isMultiPut, encoding);
  }

  @Override
  @TestTemplate
  public void loadTest() throws Exception {
    prepareForLoadTest();

    readerThreads.linkToWriter(writerThreads);

    writerThreads.start(0, numKeys, NUM_THREADS);
    readerThreads.start(0, numKeys, NUM_THREADS);

    writerThreads.waitForFinish();
    readerThreads.waitForFinish();

    assertEquals(0, writerThreads.getNumWriteFailures());
    assertEquals(0, readerThreads.getNumReadFailures());
    assertEquals(0, readerThreads.getNumReadErrors());
    assertEquals(numKeys, readerThreads.getNumUniqueKeysVerified());
  }

}

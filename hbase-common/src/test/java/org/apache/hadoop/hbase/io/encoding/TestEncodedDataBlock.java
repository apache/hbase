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

package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test for EncodedDataBlock
 */
@Category({MiscTests.class, SmallTests.class})
public class TestEncodedDataBlock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEncodedDataBlock.class);

  private Algorithm algo;
  private static final byte[] INPUT_BYTES = new byte[]{0, 1, 0, 0, 1, 2, 3, 0, 0, 1, 0, 0,
    1, 2, 3, 0, 0, 1, 0, 0, 1, 2, 3, 0, 0, 1, 0, 0, 1, 2, 3, 0};

  @Before
  public void setUp() throws IOException {
    algo = Mockito.mock(Algorithm.class);
  }

  @Test
  public void testGetCompressedSize() throws Exception {
    Mockito.when(algo.createCompressionStream(Mockito.any(), Mockito.any(), Mockito.anyInt()))
      .thenThrow(IOException.class);
    try {
      EncodedDataBlock.getCompressedSize(algo, null, INPUT_BYTES, 0, 0);
      throw new RuntimeException("Should not reach here");
    } catch (IOException e) {
      Mockito.verify(algo, Mockito.times(1)).createCompressionStream(Mockito.any(),
        Mockito.any(), Mockito.anyInt());
    }
  }

}

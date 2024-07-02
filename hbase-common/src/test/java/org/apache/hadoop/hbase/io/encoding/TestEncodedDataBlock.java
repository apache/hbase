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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Test for HBASE-23342
 */
@RunWith(MockitoJUnitRunner.class)
@Category({ MiscTests.class, SmallTests.class })
public class TestEncodedDataBlock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEncodedDataBlock.class);

  // for generating exception
  @Mock
  private MockedStatic<ReflectionUtils> mockedReflectionUtils;

  private static final byte[] INPUT_BYTES = new byte[] { 0, 1, 0, 0, 1, 2, 3, 0, 0, 1, 0, 0, 1, 2,
    3, 0, 0, 1, 0, 0, 1, 2, 3, 0, 0, 1, 0, 0, 1, 2, 3, 0 };

  @SuppressWarnings("unchecked")
  @Test
  public void testGetCompressedSize() throws Exception {
    RuntimeException inject = new RuntimeException("inject error");
    mockedReflectionUtils.when(() -> ReflectionUtils.newInstance(any(Class.class), any()))
      .thenThrow(inject);
    RuntimeException error = assertThrows(RuntimeException.class,
      () -> EncodedDataBlock.getCompressedSize(Algorithm.GZ, null, INPUT_BYTES, 0, 0));
    // make sure we get the injected error instead of NPE
    assertSame(inject, error);
  }

}

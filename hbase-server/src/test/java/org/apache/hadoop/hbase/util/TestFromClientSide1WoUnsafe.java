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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mockStatic;

import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.client.FromClientSideTest1;
import org.apache.hadoop.hbase.client.RpcConnectionRegistry;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.MockedStatic;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: registryImpl={0}, numHedgedReqs={1}")
public class TestFromClientSide1WoUnsafe extends FromClientSideTest1 {

  public TestFromClientSide1WoUnsafe(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @BeforeAll
  public static void setUpBeforeAll() throws Exception {
    try (MockedStatic<HBasePlatformDependent> mocked = mockStatic(HBasePlatformDependent.class)) {
      mocked.when(HBasePlatformDependent::isUnsafeAvailable).thenReturn(false);
      mocked.when(HBasePlatformDependent::unaligned).thenReturn(false);
      assertFalse(ByteBufferUtils.UNSAFE_AVAIL);
      assertFalse(ByteBufferUtils.UNSAFE_UNALIGNED);
    }
    initialize(MultiRowMutationEndpoint.class);
  }

  // Override the parameters in parent class as we will find the parameters method from the current
  // class first in HBaseParameterizedTemplateProvider.
  // Tests will run much slower without Unsafe, and since this test is just to confirm that our code
  // is still OK without Unsafe, and ZKConnectionRegistry does not use our Unsafe classes, so just
  // run with RpcConnectionRegistry to speed up the tests.
  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(RpcConnectionRegistry.class, 2));
  }
}

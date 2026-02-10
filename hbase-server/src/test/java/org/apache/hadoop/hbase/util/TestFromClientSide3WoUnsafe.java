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

import org.apache.hadoop.hbase.client.FromClientSide3TestBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.mockito.MockedStatic;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
public class TestFromClientSide3WoUnsafe extends FromClientSide3TestBase {

  @BeforeAll
  public static void setUpBeforeAll() throws Exception {
    try (MockedStatic<HBasePlatformDependent> mocked = mockStatic(HBasePlatformDependent.class)) {
      mocked.when(HBasePlatformDependent::isUnsafeAvailable).thenReturn(false);
      mocked.when(HBasePlatformDependent::unaligned).thenReturn(false);
      assertFalse(ByteBufferUtils.UNSAFE_AVAIL);
      assertFalse(ByteBufferUtils.UNSAFE_UNALIGNED);
    }
    startCluster();
  }
}

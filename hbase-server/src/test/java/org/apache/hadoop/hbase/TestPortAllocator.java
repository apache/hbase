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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Random;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestPortAllocator {

  @Test
  public void testResolvePortConflict() throws Exception {
    // raises port conflict between 1st call and 2nd call of randomPort() by mocking Random object
    Random random = mock(Random.class);
    when(random.nextInt(anyInt())).thenAnswer(new Answer<Integer>() {
      int[] numbers = { 1, 1, 2 };
      int count = 0;

      @Override
      public Integer answer(InvocationOnMock invocation) {
        int ret = numbers[count];
        count++;
        return ret;
      }
    });

    HBaseTestingUtil.PortAllocator.AvailablePortChecker portChecker =
      mock(HBaseTestingUtil.PortAllocator.AvailablePortChecker.class);
    when(portChecker.available(anyInt())).thenReturn(true);

    HBaseTestingUtil.PortAllocator portAllocator =
      new HBaseTestingUtil.PortAllocator(random, portChecker);

    int port1 = portAllocator.randomFreePort();
    int port2 = portAllocator.randomFreePort();
    assertNotEquals(port1, port2);
    verify(random, Mockito.times(3)).nextInt(anyInt());
  }
}

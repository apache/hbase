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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBoundedArrayQueue {

  private int qMaxElements = 5;
  private BoundedArrayQueue<Integer> queue = new BoundedArrayQueue<Integer>(qMaxElements);

  @Test
  public void testBoundedArrayQueueOperations() throws Exception {
    assertEquals(0, queue.size());
    assertNull(queue.poll());
    assertNull(queue.peek());
    for(int i=0;i<qMaxElements;i++){
      assertTrue(queue.offer(i));
    }
    assertEquals(qMaxElements, queue.size());
    assertFalse(queue.offer(0));
    assertEquals(0, queue.peek().intValue());
    assertEquals(0, queue.peek().intValue());
    for (int i = 0; i < qMaxElements; i++) {
      assertEquals(i, queue.poll().intValue());
    }
    assertEquals(0, queue.size());
    assertNull(queue.poll());
    // Write after one cycle is over
    assertTrue(queue.offer(100));
    assertTrue(queue.offer(1000));
    assertEquals(100, queue.peek().intValue());
    assertEquals(100, queue.poll().intValue());
    assertEquals(1000, queue.poll().intValue());
  }
}

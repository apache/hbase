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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestByteBufferArray {

  @Test
  public void testAsSubBufferWhenEndOffsetLandInLastBuffer() throws Exception {
    int capacity = 4 * 1024 * 1024;
    ByteBufferArray array = new ByteBufferArray(capacity, false);
    ByteBuff subBuf = array.asSubByteBuff(0, capacity);
    subBuf.position(capacity - 1);// Position to the last byte
    assertTrue(subBuf.hasRemaining());
    // Read last byte
    subBuf.get();
    assertFalse(subBuf.hasRemaining());
  }
}

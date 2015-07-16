/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.filter;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestLongComparator {
  private long values[] = { Long.MIN_VALUE, -10000000000L, -1000000L, 0L, 1000000L, 10000000000L,
      Long.MAX_VALUE };

  @Test
  public void testSimple() {
    for (int i = 1; i < values.length ; i++) {
      for (int j = 0; j < i; j++) {
        LongComparator cp = new LongComparator(values[i]);
        assertEquals(1, cp.compareTo(Bytes.toBytes(values[j])));
        ByteBuffer data_bb = ByteBuffer.wrap(Bytes.toBytes(values[j]));
        assertEquals(1, cp.compareTo(data_bb, 0, data_bb.capacity()));
      }
    }
  }
}

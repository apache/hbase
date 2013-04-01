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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category(SmallTests.class)
public class TestIncrement {
  @Test
  public void test() {
    final long expected = 13;
    Increment inc = new Increment(new byte [] {'r'});
    int total = 0;
    for (int i = 0; i < 2; i++) {
      byte [] bytes = Bytes.toBytes(i);
      inc.addColumn(bytes, bytes, expected);
      total++;
    }
    Map<byte[], NavigableMap<byte [], Long>> familyMapOfLongs = inc.getFamilyMapOfLongs();
    int found = 0;
    for (Map.Entry<byte [], NavigableMap<byte [], Long>> entry: familyMapOfLongs.entrySet()) {
      for (Map.Entry<byte [], Long> e: entry.getValue().entrySet()) {
        assertEquals(expected, e.getValue().longValue());
        found++;
      }
    }
    assertEquals(total, found);
  }
}
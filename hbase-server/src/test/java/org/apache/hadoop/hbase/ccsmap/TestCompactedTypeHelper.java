/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.Random;
import static org.junit.Assert.assertEquals;

@Category(SmallTests.class)
public class TestCompactedTypeHelper {

  private static final Random random = new Random();

  private KeyValue.Type[] typeArray = new KeyValue.Type[] {
    KeyValue.Type.Delete,
    KeyValue.Type.DeleteColumn,
    KeyValue.Type.DeleteFamily,
    KeyValue.Type.Put,
    KeyValue.Type.Maximum,
    KeyValue.Type.Minimum
  };

  private KeyValue randomKeyValue() {
    String key = Integer.toString(random.nextInt());
    String qualifier = Integer.toString(random.nextInt());
    qualifier = qualifier.substring(random.nextInt(qualifier.length() - 1));
    String family = Integer.toString(random.nextInt());
    family = family.substring(random.nextInt(family.length() - 1));
    String value = Integer.toBinaryString(random.nextInt());
    value = value.substring(random.nextInt(value.length() - 1));
    long ts = random.nextLong();
    KeyValue.Type type = typeArray[random.nextInt(typeArray.length)];
    return new KeyValue(key.getBytes(), family.getBytes(), qualifier.getBytes(), ts, type, value.getBytes());
  }

  @Test
  public void testKeyValueCompactedTypeHelper() {
    final int RANDOM_TEST_TIMES = 100000;
    KeyValue.KVComparator standardCompacrator = KeyValue.COMPARATOR;
    CompactedTypeHelper<Cell, Cell> compactedHelper = new CompactedCellHelper(standardCompacrator);
    for (int i = 0; i < RANDOM_TEST_TIMES; ++i) {
      KeyValue kv1 = randomKeyValue();
      KeyValue kv2 = randomKeyValue();
      // Test comparator
      assertEquals(
        standardCompacrator.compare(kv1, kv2),
        compactedHelper.compare(kv1, kv2.getRowArray(), kv2.getOffset(), kv2.getLength()));
      assertEquals(
        standardCompacrator.compare(kv1, kv2),
        compactedHelper.compare(kv1.getRowArray(), kv1.getOffset(), kv1.getLength(),
          kv2.getRowArray(), kv2.getOffset(), kv2.getLength()));
      assertEquals(standardCompacrator.compare(kv1, kv2), compactedHelper.compare(kv1, kv2));


      // Test compact
      int csz = compactedHelper.getCompactedSize(kv1, kv1);
      int bufferLength = 1 + csz + random.nextInt(csz);
      byte[] buffer = new byte[bufferLength];
      int offset = random.nextInt(buffer.length - csz);
      compactedHelper.compact(kv1, kv1, buffer, offset, csz);

      assertEquals(compactedHelper.compare(kv1, buffer, offset, csz), 0);

      // Test decomposte
      KVPair<Cell, Cell> kvpair = compactedHelper.decomposite(buffer, offset, csz);
      assertEquals(standardCompacrator.compare(kv1, kvpair.key()), 0);
      assertEquals(standardCompacrator.compare(kvpair.value(), kv1), 0);
      assertEquals(kv1.getMvccVersion(), kvpair.key().getSequenceId());
      assertEquals(kv1.getMvccVersion(), kvpair.value().getSequenceId());

    }
  }

}

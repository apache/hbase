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
package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IOTests.class, MediumTests.class})
public class TestBufferedDataBlockEncoder {

  byte[] row1 = Bytes.toBytes("row1");
  byte[] row2 = Bytes.toBytes("row2");
  byte[] row_1_0 = Bytes.toBytes("row10");

  byte[] fam1 = Bytes.toBytes("fam1");
  byte[] fam2 = Bytes.toBytes("fam2");
  byte[] fam_1_2 = Bytes.toBytes("fam12");

  byte[] qual1 = Bytes.toBytes("qual1");
  byte[] qual2 = Bytes.toBytes("qual2");

  byte[] val = Bytes.toBytes("val");

  @Test
  public void testEnsureSpaceForKey() {
    BufferedDataBlockEncoder.SeekerState state = new BufferedDataBlockEncoder.SeekerState(
        new ObjectIntPair<ByteBuffer>(), false);
    for (int i = 1; i <= 65536; ++i) {
      state.keyLength = i;
      state.ensureSpaceForKey();
      state.keyBuffer[state.keyLength - 1] = (byte) ((i - 1) % 0xff);
      for (int j = 0; j < i - 1; ++j) {
        // Check that earlier bytes were preserved as the buffer grew.
        assertEquals((byte) (j % 0xff), state.keyBuffer[j]);
      }
    }
  }

  @Test
  public void testCommonPrefixComparators() {
    KeyValue kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    KeyValue kv2 = new KeyValue(row1, fam_1_2, qual1, 1l, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonFamilyPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    kv2 = new KeyValue(row_1_0, fam_1_2, qual1, 1l, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonRowPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual2, 1l, Type.Put);
    kv2 = new KeyValue(row1, fam1, qual1, 1l, Type.Maximum);
    assertTrue((BufferedDataBlockEncoder.compareCommonQualifierPrefix(kv1, kv2, 4) > 0));
  }

}

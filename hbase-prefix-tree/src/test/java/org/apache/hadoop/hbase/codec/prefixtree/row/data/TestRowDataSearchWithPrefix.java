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
package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

public class TestRowDataSearchWithPrefix extends BaseTestRowData {

  static byte[] cf = Bytes.toBytes("cf");

  static byte[] cq = Bytes.toBytes("cq");

  static byte[] v = Bytes.toBytes("v");

  static List<KeyValue> d = Lists.newArrayList();

  static long ts = 55L;

  static byte[] createRowKey(int keyPart1, int keyPart2) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(16);
    DataOutputStream dos = new DataOutputStream(bos);
    try {
      dos.writeInt(keyPart1);
      dos.writeInt(keyPart2);
    } catch (IOException e) {
      // should not happen
      throw new RuntimeException(e);
    }

    return bos.toByteArray();
  }

  static {
    d.add(new KeyValue(createRowKey(1, 12345), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x01000000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x01010000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x02000000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x02020000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x03000000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x03030000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x04000000), cf, cq, ts, v));
    d.add(new KeyValue(createRowKey(12345, 0x04040000), cf, cq, ts, v));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

}

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

package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import com.google.common.collect.Lists;

/*
 * test different timestamps
 */
public class TestRowDataDifferentTimestamps extends BaseTestRowData{

  static byte[]
    Arow = Bytes.toBytes("Arow"),
    Brow = Bytes.toBytes("Brow"),
    cf = Bytes.toBytes("fammy"),
    cq0 = Bytes.toBytes("cq0"),
    cq1 = Bytes.toBytes("cq1"),
    v0 = Bytes.toBytes("v0");

  static List<KeyValue> d = Lists.newArrayList();
  static{
    KeyValue kv0 = new KeyValue(Arow, cf, cq0, 0L, v0);
    kv0.setSequenceId(123456789L);
    d.add(kv0);

    KeyValue kv1 = new KeyValue(Arow, cf, cq1, 1L, v0);
    kv1.setSequenceId(3L);
    d.add(kv1);

    KeyValue kv2 = new KeyValue(Brow, cf, cq0, 12345678L, v0);
    kv2.setSequenceId(65537L);
    d.add(kv2);

    //watch out... Long.MAX_VALUE comes back as 1332221664203, even with other encoders
    //d.add(new KeyValue(Brow, cf, cq1, Long.MAX_VALUE, v0));
    KeyValue kv3 = new KeyValue(Brow, cf, cq1, Long.MAX_VALUE-1, v0);
    kv3.setSequenceId(1L);
    d.add(kv3);

    KeyValue kv4 = new KeyValue(Brow, cf, cq1, 999999999, v0);
    //don't set memstoreTS
    d.add(kv4);

    KeyValue kv5 = new KeyValue(Brow, cf, cq1, 12345, v0);
    kv5.setSequenceId(0L);
    d.add(kv5);
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

  @Override
  public void individualBlockMetaAssertions(PrefixTreeBlockMeta blockMeta) {
    Assert.assertTrue(blockMeta.getNumMvccVersionBytes() > 0);
    Assert.assertEquals(12, blockMeta.getNumValueBytes());

    Assert.assertFalse(blockMeta.isAllSameTimestamp());
    Assert.assertNotNull(blockMeta.getMinTimestamp());
    Assert.assertTrue(blockMeta.getTimestampIndexWidth() > 0);
    Assert.assertTrue(blockMeta.getTimestampDeltaWidth() > 0);

    Assert.assertFalse(blockMeta.isAllSameMvccVersion());
    Assert.assertNotNull(blockMeta.getMinMvccVersion());
    Assert.assertTrue(blockMeta.getMvccVersionIndexWidth() > 0);
    Assert.assertTrue(blockMeta.getMvccVersionDeltaWidth() > 0);
  }

}

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

package org.apache.hadoop.hbase.codec.prefixtree.blockmeta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeBlockMeta;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestBlockMeta {

  static int BLOCK_START = 123;

  private static PrefixTreeBlockMeta createSample() {
    PrefixTreeBlockMeta m = new PrefixTreeBlockMeta();
    m.setNumMetaBytes(0);
    m.setNumKeyValueBytes(3195);

    m.setNumRowBytes(0);
    m.setNumFamilyBytes(3);
    m.setNumQualifierBytes(12345);
    m.setNumTagsBytes(50);
    m.setNumTimestampBytes(23456);
    m.setNumMvccVersionBytes(5);
    m.setNumValueBytes(34567);

    m.setNextNodeOffsetWidth(3);
    m.setFamilyOffsetWidth(1);
    m.setQualifierOffsetWidth(2);
    m.setTagsOffsetWidth(2);
    m.setTimestampIndexWidth(1);
    m.setMvccVersionIndexWidth(2);
    m.setValueOffsetWidth(8);
    m.setValueLengthWidth(3);

    m.setRowTreeDepth(11);
    m.setMaxRowLength(200);
    m.setMaxQualifierLength(50);
    m.setMaxTagsLength(40);

    m.setMinTimestamp(1318966363481L);
    m.setTimestampDeltaWidth(3);
    m.setMinMvccVersion(100L);
    m.setMvccVersionDeltaWidth(4);

    m.setAllSameType(false);
    m.setAllTypes(KeyValue.Type.Delete.getCode());

    m.setNumUniqueRows(88);
    m.setNumUniqueFamilies(1);
    m.setNumUniqueQualifiers(56);
    m.setNumUniqueTags(5);
    return m;
  }

  @Test
  public void testStreamSerialization() throws IOException {
    PrefixTreeBlockMeta original = createSample();
    ByteArrayOutputStream os = new ByteArrayOutputStream(10000);
    original.writeVariableBytesToOutputStream(os);
    ByteBuffer buffer = ByteBuffer.wrap(os.toByteArray());
    PrefixTreeBlockMeta roundTripped = new PrefixTreeBlockMeta(buffer);
    Assert.assertTrue(original.equals(roundTripped));
  }

}

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

package org.apache.hadoop.hbase.codec.keyvalue;

import java.nio.ByteBuffer;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.codec.prefixtree.row.TestRowData;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataRandomKeyValuesWithTags;
import org.apache.hadoop.hbase.codec.prefixtree.row.data.TestRowDataTrivialWithTags;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestKeyValueTool {

  @Parameters
  public static Collection<Object[]> parameters() {
    return new TestRowData.InMemory().getAllAsObjectArray();
  }

  private TestRowData rows;

  public TestKeyValueTool(TestRowData testRows) {
    this.rows = testRows;
  }

  @Test
  public void testRoundTripToBytes() {
    if(rows instanceof TestRowDataTrivialWithTags || rows instanceof TestRowDataRandomKeyValuesWithTags) {
      return;
    }
    List<KeyValue> kvs = rows.getInputs();
    ByteBuffer bb = KeyValueTestUtil.toByteBufferAndRewind(kvs, false);
    List<KeyValue> roundTrippedKvs = KeyValueTestUtil.rewindThenToList(bb, false, false);
    Assert.assertArrayEquals(kvs.toArray(), roundTrippedKvs.toArray());
  }
}

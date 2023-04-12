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
package org.apache.hadoop.hbase.regionserver;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.InnerStoreCellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This is not a unit test. It is not run as part of the general unit test suite. It is for
 * comparing cell comparators. You must run it explicitly; e.g. mvn test
 * -Dtest=PerfTestCellComparator
 */
@Category({ MediumTests.class })
@RunWith(Parameterized.class)
public class PerfTestCellComparator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(PerfTestCellComparator.class);

  private CellComparator comparator;
  private Pair<byte[], byte[]> famPair;

  byte[] row1 = Bytes.toBytes("row1");
  byte[] qual1 = Bytes.toBytes("qual1");
  byte[] val = Bytes.toBytes("val");

  int compareCnt = 100000000;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    CellComparator[] cellComparators = new CellComparator[] { CellComparator.getInstance(),
      InnerStoreCellComparator.INNER_STORE_COMPARATOR };

    byte[] fam0 = HConstants.EMPTY_BYTE_ARRAY;
    byte[] fam1 = Bytes.toBytes("fam1");
    Pair<byte[], byte[]>[] famPairs = new Pair[] { new Pair(fam0, fam0), new Pair(fam0, fam1),
      new Pair(fam1, fam0), new Pair(fam1, fam1) };

    List<Object[]> params = new ArrayList<>(cellComparators.length * famPairs.length);
    for (Pair<byte[], byte[]> famPair : famPairs) {
      for (CellComparator cellComparator : cellComparators) {
        params.add(new Object[] { cellComparator, famPair });
      }
    }

    return params;
  }

  public PerfTestCellComparator(CellComparator cellComparator, Pair<byte[], byte[]> famPair) {
    this.comparator = cellComparator;
    this.famPair = famPair;
  }

  @Test
  public void testComparePerf() {
    KeyValue kv1 = new KeyValue(row1, famPair.getFirst(), qual1, val);
    KeyValue kv2 = new KeyValue(row1, famPair.getSecond(), qual1, val);

    ByteBuffer buffer = ByteBuffer.wrap(kv1.getBuffer());
    Cell bbCell1 = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    buffer = ByteBuffer.wrap(kv2.getBuffer());
    Cell bbCell2 = new ByteBufferKeyValue(buffer, 0, buffer.remaining());

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < compareCnt; i++) {
      comparator.compare(kv1, kv2);
      comparator.compare(kv1, bbCell2);
      comparator.compare(bbCell1, kv2);
      comparator.compare(bbCell1, bbCell2);
    }
    long costTime = System.currentTimeMillis() - startTime;
    System.out.println(famPair.getFirst().length + "\t" + famPair.getSecond().length + "\t"
      + comparator.getClass().getSimpleName() + "\t" + costTime);
  }

}

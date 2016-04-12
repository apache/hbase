/**
 *
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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

public class MobTestUtil {
  protected static final char FIRST_CHAR = 'a';
  protected static final char LAST_CHAR = 'z';

  protected static String generateRandomString(int demoLength) {
    String base = "abcdefghijklmnopqrstuvwxyz";
    Random random = new Random();
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < demoLength; i++) {
      int number = random.nextInt(base.length());
      sb.append(base.charAt(number));
    }
    return sb.toString();
  }
  protected static void writeStoreFile(final StoreFileWriter writer, String caseName)
      throws IOException {
    writeStoreFile(writer, Bytes.toBytes(caseName), Bytes.toBytes(caseName));
  }

  /*
   * Writes HStoreKey and ImmutableBytes data to passed writer and then closes
   * it.
   *
   * @param writer
   *
   * @throws IOException
   */
  private static void writeStoreFile(final StoreFileWriter writer, byte[] fam,
      byte[] qualifier) throws IOException {
    long now = System.currentTimeMillis();
    try {
      for (char d = FIRST_CHAR; d <= LAST_CHAR; d++) {
        for (char e = FIRST_CHAR; e <= LAST_CHAR; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          writer.append(new KeyValue(b, fam, qualifier, now, b));
        }
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Compare two Cells only for their row family qualifier value
   */
  public static void assertCellEquals(Cell firstKeyValue, Cell secondKeyValue) {
    Assert.assertArrayEquals(CellUtil.cloneRow(firstKeyValue),
        CellUtil.cloneRow(secondKeyValue));
    Assert.assertArrayEquals(CellUtil.cloneFamily(firstKeyValue),
        CellUtil.cloneFamily(secondKeyValue));
    Assert.assertArrayEquals(CellUtil.cloneQualifier(firstKeyValue),
        CellUtil.cloneQualifier(secondKeyValue));
    Assert.assertArrayEquals(CellUtil.cloneValue(firstKeyValue),
        CellUtil.cloneValue(secondKeyValue));
  }

  public static void assertCellsValue(Table table, Scan scan,
      byte[] expectedValue, int expectedCount) throws IOException {
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertArrayEquals(expectedValue, CellUtil.cloneValue(cell));
        count++;
      }
    }
    results.close();
    Assert.assertEquals(expectedCount, count);
  }
}

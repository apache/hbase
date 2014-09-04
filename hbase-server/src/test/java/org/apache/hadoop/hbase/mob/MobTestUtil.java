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
import java.util.Random;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.StoreFile;
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
  protected static void writeStoreFile(final StoreFile.Writer writer, String caseName)
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
  private static void writeStoreFile(final StoreFile.Writer writer, byte[] fam,
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
   * Compare two KeyValue only for their row family qualifier value
   */
  public static void assertKeyValuesEquals(KeyValue firstKeyValue,
	      KeyValue secondKeyValue) {
		    Assert.assertEquals(Bytes.toString(CellUtil.cloneRow(firstKeyValue)),
	            Bytes.toString(CellUtil.cloneRow(secondKeyValue)));
		    Assert.assertEquals(Bytes.toString(CellUtil.cloneFamily(firstKeyValue)),
	            Bytes.toString(CellUtil.cloneFamily(secondKeyValue)));
		    Assert.assertEquals(Bytes.toString(CellUtil.cloneQualifier(firstKeyValue)),
	            Bytes.toString(CellUtil.cloneQualifier(secondKeyValue)));
		    Assert.assertEquals(Bytes.toString(CellUtil.cloneValue(firstKeyValue)),
	            Bytes.toString(CellUtil.cloneValue(secondKeyValue)));
	  }
}

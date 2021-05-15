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
package org.apache.hadoop.hbase.master.http.gson;

import static org.junit.Assert.assertEquals;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.gson.Gson;

@Category({ MasterTests.class, SmallTests.class})
public class GsonFactoryTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(GsonFactoryTest.class);

  private static Gson gson;

  @BeforeClass
  public static void beforeClass() {
    gson = GsonFactory.buildGson();
  }

  @Test
  public void testSerializeToLowerCaseUnderscores() {
    final SomeBean input = new SomeBean(false, 57, "hello\n");
    final String actual = gson.toJson(input);
    final String expected = "{\"a_boolean\":false,\"an_int\":57,\"a_string\":\"hello\\n\"}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSerializeMapWithSizeKeys() {
    final Map<Size, String> input = new TreeMap<>();
    input.put(new Size(10, Size.Unit.KILOBYTE), "10kb");
    input.put(new Size(5, Size.Unit.MEGABYTE), "5mb");
    final String actual = gson.toJson(input);
    final String expected = "{\"10240.0\":\"10kb\",\"5242880.0\":\"5mb\"}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSerializeNonPrintableByteArrays() {
    final Map<byte[], byte[]> input = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    input.put(Bytes.toBytes("this is printable"), new byte[] { 0, 1, 2, 3, 4, 5 });
    input.put(new byte[] { -127, -63, 0, 63, 127 }, Bytes.toBytes("test"));
    final String actual = gson.toJson(input);
    final String expected = "{" +
      "\"this is printable\":\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\"," +
      "\"��\\u0000?\u007F\":\"test\"}";
    assertEquals(expected, actual);
  }

  private static final class SomeBean {
    private final boolean aBoolean;
    private final int anInt;
    private final String aString;

    public SomeBean(
      final boolean aBoolean,
      final int anInt,
      final String aString
    ) {
      this.aBoolean = aBoolean;
      this.anInt = anInt;
      this.aString = aString;
    }

    public boolean isaBoolean() {
      return aBoolean;
    }

    public int getAnInt() {
      return anInt;
    }

    public String getaString() {
      return aString;
    }
  }
}

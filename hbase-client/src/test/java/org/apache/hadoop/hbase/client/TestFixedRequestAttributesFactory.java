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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestFixedRequestAttributesFactory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFixedRequestAttributesFactory.class);

  @Test
  public void testEmptyFactory() {
    Map<String, byte[]> attrs = FixedRequestAttributesFactory.EMPTY.create();
    assertTrue(attrs.isEmpty());
  }

  @Test
  public void testBuilderSetAttribute() {
    byte[] value = Bytes.toBytes("value1");
    FixedRequestAttributesFactory factory =
      FixedRequestAttributesFactory.newBuilder().setAttribute("key1", value).build();

    Map<String, byte[]> attrs = factory.create();
    assertEquals(1, attrs.size());
    assertArrayEquals(value, attrs.get("key1"));
  }

  @Test
  public void testBuilderMultipleAttributes() {
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");
    FixedRequestAttributesFactory factory = FixedRequestAttributesFactory.newBuilder()
      .setAttribute("key1", value1).setAttribute("key2", value2).build();

    Map<String, byte[]> attrs = factory.create();
    assertEquals(2, attrs.size());
    assertArrayEquals(value1, attrs.get("key1"));
    assertArrayEquals(value2, attrs.get("key2"));
  }

  @Test
  public void testBuilderOverrideAttribute() {
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");
    FixedRequestAttributesFactory factory = FixedRequestAttributesFactory.newBuilder()
      .setAttribute("key1", value1).setAttribute("key1", value2).build();

    Map<String, byte[]> attrs = factory.create();
    assertEquals(1, attrs.size());
    assertArrayEquals(value2, attrs.get("key1"));
  }

  @Test
  public void testBuilderNullValueRemovesAttribute() {
    byte[] value1 = Bytes.toBytes("value1");
    FixedRequestAttributesFactory factory = FixedRequestAttributesFactory.newBuilder()
      .setAttribute("key1", value1).setAttribute("key1", null).build();

    Map<String, byte[]> attrs = factory.create();
    assertTrue(attrs.isEmpty());
  }

  @Test
  public void testBuilderNullValueOnNonExistentKey() {
    FixedRequestAttributesFactory factory =
      FixedRequestAttributesFactory.newBuilder().setAttribute("key1", null).build();

    Map<String, byte[]> attrs = factory.create();
    assertTrue(attrs.isEmpty());
  }

  @Test
  public void testCreateReturnsSameInstance() {
    FixedRequestAttributesFactory factory = FixedRequestAttributesFactory.newBuilder()
      .setAttribute("key1", Bytes.toBytes("value1")).build();

    Map<String, byte[]> attrs1 = factory.create();
    Map<String, byte[]> attrs2 = factory.create();
    assertSame(attrs1, attrs2);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testReturnedMapIsUnmodifiable() {
    FixedRequestAttributesFactory factory = FixedRequestAttributesFactory.newBuilder()
      .setAttribute("key1", Bytes.toBytes("value1")).build();

    Map<String, byte[]> attrs = factory.create();
    attrs.put("key2", Bytes.toBytes("value2"));
  }

  @Test
  public void testBuilderIsolation() {
    FixedRequestAttributesFactory.Builder builder = FixedRequestAttributesFactory.newBuilder();
    builder.setAttribute("key1", Bytes.toBytes("value1"));
    FixedRequestAttributesFactory factory = builder.build();

    builder.setAttribute("key2", Bytes.toBytes("value2"));

    Map<String, byte[]> attrs = factory.create();
    assertEquals(1, attrs.size());
  }

  @Test
  public void testNewBuilderReturnsFreshInstance() {
    FixedRequestAttributesFactory.Builder builder1 = FixedRequestAttributesFactory.newBuilder();
    FixedRequestAttributesFactory.Builder builder2 = FixedRequestAttributesFactory.newBuilder();
    assertNotSame(builder1, builder2);
  }
}

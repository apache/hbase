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

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
/**
 * Addresses HBASE-6047
 * We test put.has call with all of its polymorphic magic
 */
public class TestPutDotHas {

  public static final byte[] ROW_01 = Bytes.toBytes("row-01");
  public static final byte[] QUALIFIER_01 = Bytes.toBytes("qualifier-01");
  public static final byte[] VALUE_01 = Bytes.toBytes("value-01");
  public static final byte[] FAMILY_01 = Bytes.toBytes("family-01");
  public static final long TS = 1234567L;
  public Put put = new Put(ROW_01);

  @Before
  public void setUp() {
    put.add(FAMILY_01, QUALIFIER_01, TS, VALUE_01);
  }

  @Test
  public void testHasIgnoreValueIgnoreTS() {
    Assert.assertTrue(put.has(FAMILY_01, QUALIFIER_01));
    Assert.assertFalse(put.has(QUALIFIER_01, FAMILY_01));
  }

  @Test
  public void testHasIgnoreValue() {
    Assert.assertTrue(put.has(FAMILY_01, QUALIFIER_01, TS));
    Assert.assertFalse(put.has(FAMILY_01, QUALIFIER_01, TS + 1));
  }

  @Test
  public void testHasIgnoreTS() {
    Assert.assertTrue(put.has(FAMILY_01, QUALIFIER_01, VALUE_01));
    Assert.assertFalse(put.has(FAMILY_01, VALUE_01, QUALIFIER_01));
  }

  @Test
  public void testHas() {
    Assert.assertTrue(put.has(FAMILY_01, QUALIFIER_01, TS, VALUE_01));
    // Bad TS
    Assert.assertFalse(put.has(FAMILY_01, QUALIFIER_01, TS + 1, VALUE_01));
    // Bad Value
    Assert.assertFalse(put.has(FAMILY_01, QUALIFIER_01, TS, QUALIFIER_01));
    // Bad Family
    Assert.assertFalse(put.has(QUALIFIER_01, QUALIFIER_01, TS, VALUE_01));
    // Bad Qual
    Assert.assertFalse(put.has(FAMILY_01, FAMILY_01, TS, VALUE_01));
  }
}

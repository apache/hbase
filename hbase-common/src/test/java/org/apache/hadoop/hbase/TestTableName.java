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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for various kinds of TableNames.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestTableName {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableName.class);

  private static String[] emptyNames = {"", " "};
  private static String[] invalidNamespace = {":a", "%:a"};
  private static String[] legalTableNames = {"foo", "with-dash_under.dot", "_under_start_ok",
    "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02",
    "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
    "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02"};
  private static String[] illegalTableNames = {".dot_start_illegal", "-dash_start_illegal",
    "spaces not ok", "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
    "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2"};

  static class Names {
    String ns;
    byte[] nsb;
    String tn;
    byte[] tnb;
    String nn;
    byte[] nnb;

    Names(String ns, String tn) {
      this.ns = ns;
      nsb = Bytes.toBytes(ns);
      this.tn = tn;
      tnb = Bytes.toBytes(tn);
      nn = this.ns + ":" + this.tn;
      nnb = Bytes.toBytes(nn);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Names names = (Names) o;

      if (!ns.equals(names.ns)) {
        return false;
      }
      if (!tn.equals(names.tn)) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = ns.hashCode();
      result = 31 * result + tn.hashCode();
      return result;
    }
  }

  private static Names[] names = new Names[] {
    new Names("n1", "n1"),
    new Names("n2", "n2"),
    new Names("table1", "table1"),
    new Names("table2", "table2"),
    new Names("table2", "table1"),
    new Names("table1", "table2"),
    new Names("n1", "table1"),
    new Names("n1", "table1"),
    new Names("n2", "table2"),
    new Names("n2", "table2")
  };

  @Test public void testInvalidNamespace() {
    for (String tn : invalidNamespace) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
    }
  }

  @Test public void testEmptyNamespaceName() {
    for (String nn : emptyNames) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalNamespaceName(Bytes.toBytes(nn)));
    }
  }

  @Test public void testEmptyTableName() {
    for (String tn : emptyNames) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
    }
  }

  @Test public void testLegalHTableNames() {
    for (String tn : legalTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
    }
  }

  @Test public void testIllegalHTableNames() {
    for (String tn : illegalTableNames) {
      assertThrows(Exception.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
    }
  }

  @Test public void testValueOf() {
    Map<String, TableName> inCache = new HashMap<>();
    // fill cache
    for (Names name : names) {
      inCache.put(name.nn, TableName.valueOf(name.ns, name.tn));
    }
    for (Names name : names) {
      assertSame(inCache.get(name.nn), validateNames(TableName.valueOf(name.ns, name.tn), name));
      assertSame(inCache.get(name.nn), validateNames(TableName.valueOf(name.nsb, name.tnb), name));
      assertSame(inCache.get(name.nn), validateNames(TableName.valueOf(name.nn), name));
      assertSame(inCache.get(name.nn), validateNames(TableName.valueOf(name.nnb), name));
      assertSame(inCache.get(name.nn), validateNames(TableName.valueOf(
          ByteBuffer.wrap(name.nsb), ByteBuffer.wrap(name.tnb)), name));
    }
  }

  private TableName validateNames(TableName expected, Names names) {
    assertEquals(expected.getNameAsString(), names.nn);
    assertArrayEquals(expected.getName(), names.nnb);
    assertEquals(expected.getQualifierAsString(), names.tn);
    assertArrayEquals(expected.getQualifier(), names.tnb);
    assertEquals(expected.getNamespaceAsString(), names.ns);
    assertArrayEquals(expected.getNamespace(), names.nsb);
    return expected;
  }
}

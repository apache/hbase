/**
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
package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Returns a {@code byte[]} containing the name of the currently running test method.
 */
@Category(MediumTests.class)
public class TestTableName extends TestWatcher {
  private TableName tableName;

  /**
   * Invoked when a test is about to start
   */
  @Override
  protected void starting(Description description) {
    tableName = TableName.valueOf(description.getMethodName());
  }

  public TableName getTableName() {
    return tableName;
  }
  
  String emptyTableNames[] ={"", " "};
  String invalidNamespace[] = {":a", "%:a"};
  String legalTableNames[] = { "foo", "with-dash_under.dot", "_under_start_ok",
      "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02"
      , "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
      "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02"};
  String illegalTableNames[] = { ".dot_start_illegal", "-dash_start_illegal", "spaces not ok",
      "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
      "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2"};


  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespace() {
    for (String tn : invalidNamespace) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
      fail("invalid namespace " + tn + " should have failed with IllegalArgumentException for namespace");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTableName() {
    for (String tn : emptyTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
      fail("invalid tablename " + tn + " should have failed with IllegalArgumentException");
    }
  }

  @Test
  public void testLegalHTableNames() {
    for (String tn : legalTableNames) {
      TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
    }
  }

  @Test
  public void testIllegalHTableNames() {
    for (String tn : illegalTableNames) {
      try {
        TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn));
        fail("invalid tablename " + tn + " should have failed");
      } catch (Exception e) {
        // expected
      }
    }
  }

  class Names {
    String ns;
    byte[] nsb;
    String tn;
    byte[] tnb;
    String nn;
    byte[] nnb;

    Names(String ns, String tn) {
      this.ns = ns;
      nsb = ns.getBytes();
      this.tn = tn;
      tnb = tn.getBytes();
      nn = this.ns + ":" + this.tn;
      nnb = nn.getBytes();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Names names = (Names) o;

      if (!ns.equals(names.ns)) return false;
      if (!tn.equals(names.tn)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = ns.hashCode();
      result = 31 * result + tn.hashCode();
      return result;
    }
  }

  Names[] names = new Names[] {
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

  @Test
  public void testValueOf() {

    Map<String, TableName> inCache = new HashMap<String, TableName>();
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

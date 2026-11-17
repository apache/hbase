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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for various kinds of TableNames.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestTableName {

  private static String[] emptyNames = { "", " " };
  private static String[] invalidNamespace = { ":a", "%:a" };
  private static String[] legalTableNames = { "foo", "with-dash_under.dot", "_under_start_ok",
    "with-dash.with_underscore", "02-01-2012.my_table_01-02", "xyz._mytable_", "9_9_0.table_02",
    "dot1.dot2.table", "new.-mytable", "with-dash.with.dot", "legal..t2", "legal..legal.t2",
    "trailingdots..", "trailing.dots...", "ns:mytable", "ns:_mytable_", "ns:my_table_01-02" };
  private static String[] illegalTableNames = { ".dot_start_illegal", "-dash_start_illegal",
    "spaces not ok", "-dash-.start_illegal", "new.table with space", "01 .table", "ns:-illegaldash",
    "new:.illegaldot", "new:illegalcolon1:", "new:illegalcolon1:2" };

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
      if (!(o instanceof Names)) {
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

  private static Names[] names = new Names[] { new Names("n1", "n1"), new Names("n2", "n2"),
    new Names("table1", "table1"), new Names("table2", "table2"), new Names("table2", "table1"),
    new Names("table1", "table2"), new Names("n1", "table1"), new Names("n1", "table1"),
    new Names("n2", "table2"), new Names("n2", "table2") };

  @Test
  public void testInvalidNamespace() {
    for (String tn : invalidNamespace) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
    }
  }

  @Test
  public void testEmptyNamespaceName() {
    for (String nn : emptyNames) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalNamespaceName(Bytes.toBytes(nn)));
    }
  }

  @Test
  public void testEmptyTableName() {
    for (String tn : emptyNames) {
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
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
      assertThrows(IllegalArgumentException.class,
        () -> TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(tn)));
    }
  }

  @Test
  public void testValueOf() {
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
      assertSame(inCache.get(name.nn), validateNames(
        TableName.valueOf(ByteBuffer.wrap(name.nsb), ByteBuffer.wrap(name.tnb)), name));
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

  @Test
  public void testValidMetaTableSuffix() {
    String[] validSuffixes = { "REPL1", "123", "123abc" };
    for (String suffix : validSuffixes) {
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.HBASE_META_TABLE_SUFFIX, suffix);
      TableName metaTableName = TableName.initializeHbaseMetaTableName(conf);
      assertEquals("hbase:meta_" + suffix, metaTableName.getNameAsString());
    }
  }

  @Test
  public void testInvalidMetaTableSuffix() {
    String[] invalidSuffixes = { "test_1", "test-1", "test.1", "test 1", "_test", "-test", ".test",
      "has!special", "has:colon", " " };
    for (String suffix : invalidSuffixes) {
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.HBASE_META_TABLE_SUFFIX, suffix);
      assertThrows(IllegalArgumentException.class,
        () -> TableName.initializeHbaseMetaTableName(conf),
        "Expected IllegalArgumentException for suffix: " + suffix);
    }
  }

  /**
   * Edge cases for {@link TableName#isMetaTableName(TableName)}. The recognizer must accept the
   * default qualifier {@code meta} and the cluster-specific form {@code meta_<suffix>} (where the
   * suffix matches {@link TableName#VALID_META_TABLE_SUFFIX_REGEX}), while rejecting lookalikes
   * such as a bare trailing underscore, double-underscore suffixes, {@code metadata}, and any
   * non-{@code hbase} namespace.
   */
  @Test
  public void testIsMetaTableNameTrueCases() {
    // Default meta.
    assertTrue(TableName
      .isMetaTableName(TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta")));
    // Cluster-specific suffixed meta tables with suffixes drawn from VALID_META_TABLE_SUFFIX_REGEX
    // ([a-zA-Z0-9]+). The matcher must accept each.
    String[] validSuffixedQualifiers =
      { "meta_1", "meta_a", "meta_REPL1", "meta_abc123", "meta_123abc", "meta_data" };
    for (String q : validSuffixedQualifiers) {
      TableName tn = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, q);
      assertTrue(TableName.isMetaTableName(tn), "isMetaTableName should accept hbase:" + q);
    }
  }

  @Test
  public void testIsMetaTableNameFalseCases() {
    // Bare trailing underscore — suffix is required to be at least one char from
    // VALID_META_TABLE_SUFFIX_REGEX = [a-zA-Z0-9]+, so "meta_" alone must not match.
    assertFalse(TableName
      .isMetaTableName(TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta_")));
    // Double underscore — the character immediately after "meta_" is not in [a-zA-Z0-9].
    assertFalse(TableName.isMetaTableName(
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta__suffix")));
    // Lookalikes without the "_" delimiter — must not be interpreted as meta tables.
    assertFalse(TableName.isMetaTableName(
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "metadata")));
    assertFalse(TableName.isMetaTableName(
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "metatable")));
    // Non-hbase namespace — same qualifier must not count as a meta table.
    assertFalse(TableName.isMetaTableName(TableName.valueOf("default", "meta")));
    assertFalse(TableName.isMetaTableName(TableName.valueOf("other", "meta_1")));
    // Null tolerance.
    assertFalse(TableName.isMetaTableName(null));
  }
}

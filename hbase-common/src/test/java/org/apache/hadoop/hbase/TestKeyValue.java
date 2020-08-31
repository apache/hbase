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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SmallTests.class)
public class TestKeyValue {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyValue.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestKeyValue.class);

  @Test
  public void testColumnCompare() {
    final byte[] a = Bytes.toBytes("aaa");
    byte[] family1 = Bytes.toBytes("abc");
    byte[] qualifier1 = Bytes.toBytes("def");
    byte[] family2 = Bytes.toBytes("abcd");
    byte[] qualifier2 = Bytes.toBytes("ef");

    KeyValue aaa = new KeyValue(a, family1, qualifier1, 0L, KeyValue.Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family2, qualifier2));
    assertTrue(CellUtil.matchingColumn(aaa, family1, qualifier1));
    aaa = new KeyValue(a, family2, qualifier2, 0L, KeyValue.Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family1, qualifier1));
    assertTrue(CellUtil.matchingColumn(aaa, family2,qualifier2));
    byte[] nullQualifier = new byte[0];
    aaa = new KeyValue(a, family1, nullQualifier, 0L, KeyValue.Type.Put, a);
    assertTrue(CellUtil.matchingColumn(aaa, family1,null));
    assertFalse(CellUtil.matchingColumn(aaa, family2,qualifier2));
  }

  /**
   * Test a corner case when the family qualifier is a prefix of the column qualifier.
   */
  @Test
  public void testColumnCompare_prefix() {
    final byte[] a = Bytes.toBytes("aaa");
    byte[] family1 = Bytes.toBytes("abc");
    byte[] qualifier1 = Bytes.toBytes("def");
    byte[] family2 = Bytes.toBytes("ab");
    byte[] qualifier2 = Bytes.toBytes("def");

    KeyValue aaa = new KeyValue(a, family1, qualifier1, 0L, KeyValue.Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family2, qualifier2));
  }

  @Test
  public void testBasics() {
    LOG.info("LOWKEY: " + KeyValue.LOWESTKEY.toString());
    String name = "testBasics";
    check(Bytes.toBytes(name),
      Bytes.toBytes(name), Bytes.toBytes(name), 1,
      Bytes.toBytes(name));
    // Test empty value and empty column -- both should work. (not empty fam)
    check(Bytes.toBytes(name), Bytes.toBytes(name), null, 1, null);
    check(HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes(name), null, 1, null);
    // empty qual is equivalent to null qual
    assertEquals(
      new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("fam"), null, 1, (byte[]) null),
      new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("fam"),
        HConstants.EMPTY_BYTE_ARRAY, 1, (byte[]) null));
  }

  private void check(final byte [] row, final byte [] family, byte [] qualifier,
    final long timestamp, final byte [] value) {
    KeyValue kv = new KeyValue(row, family, qualifier, timestamp, value);
    assertTrue(Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), row, 0,
      row.length) == 0);
    assertTrue(CellUtil.matchingColumn(kv, family, qualifier));
    // Call toString to make sure it works.
    LOG.info(kv.toString());
  }

  @Test
  public void testPlainCompare() {
    final byte[] a = Bytes.toBytes("aaa");
    final byte[] b = Bytes.toBytes("bbb");
    final byte[] fam = Bytes.toBytes("col");
    final byte[] qf = Bytes.toBytes("umn");
    KeyValue aaa = new KeyValue(a, fam, qf, a);
    KeyValue bbb = new KeyValue(b, fam, qf, b);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(bbb, aaa) > 0);
    // Compare breaks if passed same ByteBuffer as both left and right arguments.
    assertTrue(CellComparatorImpl.COMPARATOR.compare(bbb, bbb) == 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different timestamps.
    aaa = new KeyValue(a, fam, qf, 1, a);
    bbb = new KeyValue(a, fam, qf, 2, a);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, bbb) > 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(bbb, aaa) < 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different types.  Higher numbered types -- Delete
    // should sort ahead of lower numbers; i.e. Put
    aaa = new KeyValue(a, fam, qf, 1, KeyValue.Type.Delete, a);
    bbb = new KeyValue(a, fam, qf, 1, a);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(bbb, aaa) > 0);
    assertTrue(CellComparatorImpl.COMPARATOR.compare(aaa, aaa) == 0);
  }

  @Test
  public void testMoreComparisons() {
    long now = System.currentTimeMillis();

    // Meta compares
    KeyValue aaa = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,row_0500,1236020145502"), now);
    KeyValue bbb = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,,99999999999999"), now);
    CellComparator c = MetaCellComparator.META_COMPARATOR;
    assertTrue(c.compare(bbb, aaa) < 0);

    KeyValue aaaa = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,,1236023996656"),
        Bytes.toBytes("info"), Bytes.toBytes("regioninfo"), 1236024396271L,
        (byte[])null);
    assertTrue(c.compare(aaaa, bbb) < 0);

    KeyValue x = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,row_0500,1236034574162"),
        Bytes.toBytes("info"), Bytes.toBytes(""), 9223372036854775807L,
        (byte[])null);
    KeyValue y = new KeyValue(Bytes.toBytes("TestScanMultipleVersions,row_0500,1236034574162"),
        Bytes.toBytes("info"), Bytes.toBytes("regioninfo"), 1236034574912L,
        (byte[])null);
    assertTrue(c.compare(x, y) < 0);
    comparisons(MetaCellComparator.META_COMPARATOR);
    comparisons(CellComparatorImpl.COMPARATOR);
    metacomparisons(MetaCellComparator.META_COMPARATOR);
  }

  @Test
  public void testMetaComparatorTableKeysWithCommaOk() {
    CellComparator c = MetaCellComparator.META_COMPARATOR;
    long now = System.currentTimeMillis();
    // meta keys values are not quite right.  A users can enter illegal values
    // from shell when scanning meta.
    KeyValue a = new KeyValue(Bytes.toBytes("table,key,with,commas1,1234"), now);
    KeyValue b = new KeyValue(Bytes.toBytes("table,key,with,commas2,0123"), now);
    assertTrue(c.compare(a, b) < 0);
  }

  /**
   * Tests cases where rows keys have characters below the ','.
   * See HBASE-832
   */
  @Test
  public void testKeyValueBorderCases() {
    // % sorts before , so if we don't do special comparator, rowB would
    // come before rowA.
    KeyValue rowA = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/,1234"),
      Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    KeyValue rowB = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/%20,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(MetaCellComparator.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue(Bytes.toBytes("testtable,,1234"), Bytes.toBytes("fam"),
        Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    rowB = new KeyValue(Bytes.toBytes("testtable,$www.hbase.org/,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(MetaCellComparator.META_COMPARATOR.compare(rowA, rowB) < 0);
  }

  private void metacomparisons(final CellComparatorImpl c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,1"), now),
      new KeyValue(
          Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,1"), now)) == 0);
    KeyValue a = new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,1"), now);
    KeyValue b = new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,2"), now);
    assertTrue(c.compare(a, b) < 0);
    assertTrue(c.compare(new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,2"), now),
      new KeyValue(
          Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",a,,0,1"), now)) > 0);
  }

  private void comparisons(final CellComparatorImpl c) {
    long now = System.currentTimeMillis();
    assertTrue(c.compare(new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,1"), now),
      new KeyValue(
          Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,1"), now)) == 0);
    assertTrue(c.compare(new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,1"), now),
      new KeyValue(
          Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,2"), now)) < 0);
    assertTrue(c.compare(new KeyValue(
        Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,2"), now),
      new KeyValue(
          Bytes.toBytes(TableName.META_TABLE_NAME.getNameAsString()+",,1"), now)) > 0);
  }

  @Test
  public void testBinaryKeys() {
    Set<KeyValue> set = new TreeSet<>(CellComparatorImpl.COMPARATOR);
    final byte[] fam = Bytes.toBytes("col");
    final byte[] qf = Bytes.toBytes("umn");
    final byte[] nb = new byte[0];
    KeyValue[] keys = {new KeyValue(Bytes.toBytes("aaaaa,\u0000\u0000,2"), fam, qf, 2, nb),
      new KeyValue(Bytes.toBytes("aaaaa,\u0001,3"), fam, qf, 3, nb),
      new KeyValue(Bytes.toBytes("aaaaa,,1"), fam, qf, 1, nb),
      new KeyValue(Bytes.toBytes("aaaaa,\u1000,5"), fam, qf, 5, nb),
      new KeyValue(Bytes.toBytes("aaaaa,a,4"), fam, qf, 4, nb),
      new KeyValue(Bytes.toBytes("a,a,0"), fam, qf, 0, nb),
    };
    // Add to set with bad comparator
    Collections.addAll(set, keys);
    // This will output the keys incorrectly.
    boolean assertion = false;
    int count = 0;
    try {
      for (KeyValue k : set) {
        assertEquals(count++, k.getTimestamp());
      }
    } catch (java.lang.AssertionError e) {
      // Expected
      assertion = true;
    }
    assertTrue(assertion);
    // Make set with good comparator
    set = new TreeSet<>(MetaCellComparator.META_COMPARATOR);
    Collections.addAll(set, keys);
    count = 0;
    for (KeyValue k : set) {
      assertEquals(count++, k.getTimestamp());
    }
  }

  @Test
  public void testStackedUpKeyValue() {
    // Test multiple KeyValues in a single blob.

    // TODO actually write this test!
  }

  private final byte[] rowA = Bytes.toBytes("rowA");
  private final byte[] rowB = Bytes.toBytes("rowB");

  private final byte[] family = Bytes.toBytes("family");
  private final byte[] qualA = Bytes.toBytes("qfA");

  private void assertKVLess(CellComparator c, KeyValue less, KeyValue greater) {
    int cmp = c.compare(less,greater);
    assertTrue(cmp < 0);
    cmp = c.compare(greater,less);
    assertTrue(cmp > 0);
  }

  private void assertKVLessWithoutRow(CellComparator c, KeyValue less, KeyValue greater) {
    int cmp = c.compare(less, greater);
    assertTrue(cmp < 0);
    cmp = c.compare(greater, less);
    assertTrue(cmp > 0);
  }

  @Test
  public void testCompareWithoutRow() {
    final CellComparator c = CellComparatorImpl.COMPARATOR;
    byte[] row = Bytes.toBytes("row");

    byte[] fa = Bytes.toBytes("fa");
    byte[] fami = Bytes.toBytes("fami");
    byte[] fami1 = Bytes.toBytes("fami1");

    byte[] qual0 = Bytes.toBytes("");
    byte[] qual1 = Bytes.toBytes("qf1");
    byte[] qual2 = Bytes.toBytes("qf2");
    long ts = 1;

    // 'fa:'
    KeyValue kv_0 = new KeyValue(row, fa, qual0, ts, KeyValue.Type.Put);
    // 'fami:'
    KeyValue kv0_0 = new KeyValue(row, fami, qual0, ts, KeyValue.Type.Put);
    // 'fami:qf1'
    KeyValue kv0_1 = new KeyValue(row, fami, qual1, ts, KeyValue.Type.Put);
    // 'fami:qf2'
    KeyValue kv0_2 = new KeyValue(row, fami, qual2, ts, KeyValue.Type.Put);
    // 'fami1:'
    KeyValue kv1_0 = new KeyValue(row, fami1, qual0, ts, KeyValue.Type.Put);

    // 'fami:qf1' < 'fami:qf2'
    assertKVLessWithoutRow(c, kv0_1, kv0_2);
    // 'fami:qf1' < 'fami1:'
    assertKVLessWithoutRow(c, kv0_1, kv1_0);

    // Test comparison by skipping the same prefix bytes.
    /*
     * KeyValue Format and commonLength:
     * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
     * ------------------|-------commonLength--------|--------------
     */
    int commonLength = KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE
        + row.length;
    // 'fa:' < 'fami:'. They have commonPrefix + 2 same prefix bytes.
    assertKVLessWithoutRow(c, kv_0, kv0_0);
    // 'fami:' < 'fami:qf1'. They have commonPrefix + 4 same prefix bytes.
    assertKVLessWithoutRow(c, kv0_0, kv0_1);
    // 'fami:qf1' < 'fami1:'. They have commonPrefix + 4 same prefix bytes.
    assertKVLessWithoutRow(c, kv0_1, kv1_0);
    // 'fami:qf1' < 'fami:qf2'. They have commonPrefix + 6 same prefix bytes.
    assertKVLessWithoutRow(c, kv0_1, kv0_2);
  }

  @Test
  public void testFirstLastOnRow() {
    final CellComparator c = CellComparatorImpl.COMPARATOR;
    long ts = 1;
    byte[] bufferA = new byte[128];
    int offsetA = 0;
    byte[] bufferB = new byte[128];
    int offsetB = 7;

    // These are listed in sort order (ie: every one should be less
    // than the one on the next line).
    final KeyValue firstOnRowA = KeyValueUtil.createFirstOnRow(rowA);
    final KeyValue firstOnRowABufferFamQual = KeyValueUtil.createFirstOnRow(bufferA, offsetA,
        rowA, 0, rowA.length, family, 0, family.length, qualA, 0, qualA.length);
    final KeyValue kvA_1 = new KeyValue(rowA, null, null, ts, KeyValue.Type.Put);
    final KeyValue kvA_2 = new KeyValue(rowA, family, qualA, ts, KeyValue.Type.Put);

    final KeyValue lastOnRowA = KeyValueUtil.createLastOnRow(rowA);
    final KeyValue firstOnRowB = KeyValueUtil.createFirstOnRow(rowB);
    final KeyValue firstOnRowBBufferFam = KeyValueUtil.createFirstOnRow(bufferB, offsetB,
        rowB, 0, rowB.length, family, 0, family.length, null, 0, 0);
    final KeyValue kvB = new KeyValue(rowB, family, qualA, ts, KeyValue.Type.Put);

    assertKVLess(c, firstOnRowA, firstOnRowB);
    assertKVLess(c, firstOnRowA, firstOnRowBBufferFam);
    assertKVLess(c, firstOnRowABufferFamQual, firstOnRowB);
    assertKVLess(c, firstOnRowA, kvA_1);
    assertKVLess(c, firstOnRowA, kvA_2);
    assertKVLess(c, firstOnRowABufferFamQual, kvA_2);
    assertKVLess(c, kvA_1, kvA_2);
    assertKVLess(c, kvA_2, firstOnRowB);
    assertKVLess(c, kvA_1, firstOnRowB);
    assertKVLess(c, kvA_2, firstOnRowBBufferFam);
    assertKVLess(c, kvA_1, firstOnRowBBufferFam);

    assertKVLess(c, lastOnRowA, firstOnRowB);
    assertKVLess(c, lastOnRowA, firstOnRowBBufferFam);
    assertKVLess(c, firstOnRowB, kvB);
    assertKVLess(c, firstOnRowBBufferFam, kvB);
    assertKVLess(c, lastOnRowA, kvB);

    assertKVLess(c, kvA_2, lastOnRowA);
    assertKVLess(c, kvA_1, lastOnRowA);
    assertKVLess(c, firstOnRowA, lastOnRowA);
    assertKVLess(c, firstOnRowABufferFamQual, lastOnRowA);
  }

  @Test
  public void testCreateKeyOnly() {
    long ts = 1;
    byte[] value = Bytes.toBytes("a real value");
    byte[] evalue = new byte[0]; // empty value

    for (byte[] val : new byte[][] { value, evalue }) {
      for (boolean useLen : new boolean[]{false,true}) {
        KeyValue kv1 = new KeyValue(rowA, family, qualA, ts, val);
        KeyValue kv1ko = kv1.createKeyOnly(useLen);
        // keys are still the same
        assertTrue(kv1.equals(kv1ko));
        // but values are not
        assertTrue(kv1ko.getValueLength() == (useLen?Bytes.SIZEOF_INT:0));
        if (useLen) {
          assertEquals(kv1.getValueLength(),
            Bytes.toInt(kv1ko.getValueArray(), kv1ko.getValueOffset(), kv1ko.getValueLength()));
        }
      }
    }
  }

  @Test
  public void testCreateKeyValueFromKey() {
    KeyValue kv = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),
        Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("myValue"));
    int initialPadding = 10;
    int endingPadding = 20;
    int keyLen = kv.getKeyLength();
    byte[] tmpArr = new byte[initialPadding + endingPadding + keyLen];
    System.arraycopy(kv.getBuffer(), kv.getKeyOffset(), tmpArr,
        initialPadding, keyLen);
    KeyValue kvFromKey = KeyValueUtil.createKeyValueFromKey(tmpArr, initialPadding,
        keyLen);
    assertEquals(keyLen, kvFromKey.getKeyLength());
    assertEquals(KeyValue.ROW_OFFSET + keyLen, kvFromKey.getBuffer().length);
    System.err.println("kv=" + kv);
    System.err.println("kvFromKey=" + kvFromKey);
    assertEquals(kvFromKey.toString(),
        kv.toString().replaceAll("=[0-9]+", "=0"));
  }

  /**
   * Tests that getTimestamp() does always return the proper timestamp, even after updating it.
   * See HBASE-6265.
   */
  @Test
  public void testGetTimestamp() {
    KeyValue kv = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),
      Bytes.toBytes("myQualifier"), HConstants.LATEST_TIMESTAMP,
      Bytes.toBytes("myValue"));
    long time1 = kv.getTimestamp();
    kv.updateLatestStamp(Bytes.toBytes(12345L));
    long time2 = kv.getTimestamp();
    assertEquals(HConstants.LATEST_TIMESTAMP, time1);
    assertEquals(12345L, time2);
  }

  @Test
  public void testKVsWithTags() {
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    byte[] value = Bytes.toBytes("myValue");
    byte[] metaValue1 = Bytes.toBytes("metaValue1");
    byte[] metaValue2 = Bytes.toBytes("metaValue2");
    KeyValue kv = new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, new Tag[] {
      new ArrayBackedTag((byte) 1, metaValue1), new ArrayBackedTag((byte) 2, metaValue2)
    });
    assertTrue(kv.getTagsLength() > 0);
    assertTrue(Bytes.equals(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), row, 0,
      row.length));
    assertTrue(Bytes.equals(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), cf, 0,
      cf.length));
    assertTrue(Bytes.equals(kv.getQualifierArray(), kv.getQualifierOffset(),
      kv.getQualifierLength(), q, 0, q.length));
    assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value, 0,
      value.length));
    List<Tag> tags = PrivateCellUtil.getTags(kv);
    assertNotNull(tags);
    assertEquals(2, tags.size());
    boolean meta1Ok = false, meta2Ok = false;
    for (Tag tag : tags) {
      if (tag.getType() == (byte) 1) {
        if (Bytes.equals(Tag.cloneValue(tag), metaValue1)) {
          meta1Ok = true;
        }
      } else {
        if (Bytes.equals(Tag.cloneValue(tag), metaValue2)) {
          meta2Ok = true;
        }
      }
    }
    assertTrue(meta1Ok);
    assertTrue(meta2Ok);
    Iterator<Tag> tagItr = PrivateCellUtil.tagsIterator(kv);

    assertTrue(tagItr.hasNext());
    Tag next = tagItr.next();
    assertEquals(10, next.getValueLength());
    assertEquals((byte) 1, next.getType());
    Bytes.equals(Tag.cloneValue(next), metaValue1);
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getValueLength());
    assertEquals((byte) 2, next.getType());
    Bytes.equals(Tag.cloneValue(next), metaValue2);
    assertFalse(tagItr.hasNext());

    tagItr = PrivateCellUtil.tagsIterator(kv);
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getValueLength());
    assertEquals((byte) 1, next.getType());
    Bytes.equals(Tag.cloneValue(next), metaValue1);
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getValueLength());
    assertEquals((byte) 2, next.getType());
    Bytes.equals(Tag.cloneValue(next), metaValue2);
    assertFalse(tagItr.hasNext());
  }

  @Test
  public void testMetaKeyComparator() {
    CellComparator c = MetaCellComparator.META_COMPARATOR;
    long now = System.currentTimeMillis();

    KeyValue a = new KeyValue(Bytes.toBytes("table1"), now);
    KeyValue b = new KeyValue(Bytes.toBytes("table2"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table1,111"), now);
    b = new KeyValue(Bytes.toBytes("table2"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table1"), now);
    b = new KeyValue(Bytes.toBytes("table2,111"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111"), now);
    b = new KeyValue(Bytes.toBytes("table,2222"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111,aaaa"), now);
    b = new KeyValue(Bytes.toBytes("table,2222"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111"), now);
    b = new KeyValue(Bytes.toBytes("table,2222.bbb"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,,aaaa"), now);
    b = new KeyValue(Bytes.toBytes("table,111,bbb"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111,aaaa"), now);
    b = new KeyValue(Bytes.toBytes("table,111,bbb"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111,xxxx"), now);
    b = new KeyValue(Bytes.toBytes("table,111,222,bbb"), now);
    assertTrue(c.compare(a, b) < 0);

    a = new KeyValue(Bytes.toBytes("table,111,11,xxx"), now);
    b = new KeyValue(Bytes.toBytes("table,111,222,bbb"), now);
    assertTrue(c.compare(a, b) < 0);
  }

  @Test
  public void testEqualsAndHashCode() {
    KeyValue kvA1 = new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"),
        Bytes.toBytes("qualA"), Bytes.toBytes("1"));
    KeyValue kvA2 = new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"),
        Bytes.toBytes("qualA"), Bytes.toBytes("2"));
    // We set a different sequence id on kvA2 to demonstrate that the equals and hashCode also
    // don't take this into account.
    kvA2.setSequenceId(2);
    KeyValue kvB = new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"),
        Bytes.toBytes("qualB"), Bytes.toBytes("1"));

    assertEquals(kvA1, kvA2);
    assertNotEquals(kvA1, kvB);
    assertEquals(kvA1.hashCode(), kvA2.hashCode());
    assertNotEquals(kvA1.hashCode(), kvB.hashCode());
  }

  @Test
  public void testKeyValueSerialization() throws Exception {
    KeyValue[] keyValues = new KeyValue[] {
      new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
            Bytes.toBytes("1")),
      new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
            Bytes.toBytes("2")),
      new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
            System.currentTimeMillis(), Bytes.toBytes("2"),
            new Tag[] {
              new ArrayBackedTag((byte) 120, "tagA"),
              new ArrayBackedTag((byte) 121, Bytes.toBytes("tagB")) }),
      new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
            System.currentTimeMillis(), Bytes.toBytes("2"),
            new Tag[] { new ArrayBackedTag((byte) 0, "tagA") }),
      new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes(""),
            Bytes.toBytes("1")) };
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (KeyValue kv : keyValues) {
      DataOutputStream os = new DataOutputStream(byteArrayOutputStream);
      ByteBufferUtils.putInt(os, KeyValueUtil.getSerializedSize(kv, true));
      KeyValueUtil.oswrite(kv, os, true);
    }
    DataInputStream is =
        new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    for (int i = 0; i < keyValues.length; i++) {
      LOG.info("Case#" + i + ": deserialize the kv: " + keyValues[i]);
      KeyValue destKv = KeyValueUtil.createKeyValueFromInputStream(is, true);
      assertEquals(keyValues[i], destKv);
      assertArrayEquals(CellUtil.cloneValue(keyValues[i]), CellUtil.cloneValue(destKv));
      assertArrayEquals(PrivateCellUtil.cloneTags(keyValues[i]), PrivateCellUtil.cloneTags(destKv));
    }
  }

  @Test
  public void testNullByteArrayKeyValueFailure() {
    // can't add to testCheckKeyValueBytesFailureCase because it
    // goes through the InputStream KeyValue API which can't produce a null buffer
    try {
      new KeyValue(null, 0, 0);
    } catch (IllegalArgumentException iae) {
      assertEquals("Invalid to have null byte array in KeyValue.", iae.getMessage());
      return;
    }
    fail("Should have thrown an IllegalArgumentException when " +
        "creating a KeyValue with a null buffer");
  }

  private static class FailureCase {
    byte[] buf;
    int offset;
    int length;
    boolean withTags;
    String expectedMessage;

    public FailureCase(byte[] buf, int offset, int length, boolean withTags,
        String expectedMessage) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
      this.withTags = withTags;
      this.expectedMessage = expectedMessage;
    }

    @Override
    public String toString() {
      return "FailureCaseDetails: [buf=" + Bytes.toStringBinary(buf, offset, length) + ", offset="
          + offset + ", " + "length=" + length + ", expectedMessage=" + expectedMessage
          + ", withtags=" + withTags + "]";
    }

    public String getExpectedMessage() {
      return this.expectedMessage + KeyValueUtil.bytesToHex(buf, offset, length);
    }
  }

  @Test
  public void testCheckKeyValueBytesFailureCase() throws Exception {
    byte[][] inputs = new byte[][] { HConstants.EMPTY_BYTE_ARRAY, // case.0
      Bytes.toBytesBinary("a"), // case.1
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01"), // case.2
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00"), // case.3
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01"), // case.4
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00"), // case.5
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x01"), // case.6
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x03ROW"), // case.7
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01"), // case.8
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\xFF"
          + "\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\x03"), // case.9
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x03"), // case.10
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04"), // case.11
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04VALUE"), // case.12
    };
    String[] outputs = new String[] { "Overflow when reading key length at position=0",
      "Overflow when reading key length at position=0",
      "Invalid key length in KeyValue. keyLength=1",
      "Overflow when reading value length at position=4",
      "Invalid value length in KeyValue, valueLength=1",
      "Overflow when reading row length at position=8",
      "Invalid row length in KeyValue, rowLength=1",
      "Overflow when reading family length at position=13",
      "Invalid family length in KeyValue, familyLength=1", "Timestamp cannot be negative, ts=-1",
      "Invalid type in KeyValue, type=3", "Overflow when reading value part at position=25",
      "Invalid tags length in KeyValue at position=26"};
    byte[][] withTagsInputs = new byte[][] {
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x01"), // case.13
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x01"), // case.14
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x04\\x00\\x03\\x00A"), // case.15
      // case.16
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x0A\\x00\\x04\\x00TAG\\x00\\x04"
          + "\\xFFT"),
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x0C\\x00\\x04\\x00TAG\\x00\\x05"
          + "\\xF0COME\\x00"), // case.17
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x0C\\x00\\x04\\x00TAG\\x00\\x05"
          + "\\xF0COME"), // case.18
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x00"), // case.19
      Bytes.toBytesBinary("\\x00\\x00\\x00\\x11\\x00\\x00\\x00\\x01\\x00\\x03ROW\\x01FQ\\x00"
          + "\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x04V\\x00\\x1B\\x00\\x05\\x01TAG1\\x00\\x05"
          + "\\x02TAG2\\x00\\x05\\x03TAG3\\x00\\x05\\x04TAG4"), // case.20
    };
    String[] withTagsOutputs = new String[] { "Overflow when reading tags length at position=26",
      "Invalid tags length in KeyValue at position=26",
      "Invalid tag length at position=28, tagLength=3",
      "Invalid tag length at position=34, tagLength=4",
      "Some redundant bytes in KeyValue's buffer, startOffset=41, endOffset=42", null, null,
      null,
    };
    assertEquals(inputs.length, outputs.length);
    assertEquals(withTagsInputs.length, withTagsOutputs.length);

    FailureCase[] cases = new FailureCase[inputs.length + withTagsInputs.length];
    for (int i = 0; i < inputs.length; i++) {
      cases[i] = new FailureCase(inputs[i], 0, inputs[i].length, false, outputs[i]);
    }
    for (int i = 0; i < withTagsInputs.length; i++) {
      cases[inputs.length + i] =
          new FailureCase(withTagsInputs[i], 0, withTagsInputs[i].length, true, withTagsOutputs[i]);
    }

    for (int i = 0; i < cases.length; i++) {
      FailureCase c = cases[i];
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream os = new DataOutputStream(baos);
      ByteBufferUtils.putInt(os, c.length);
      os.write(c.buf, c.offset, c.length);
      try {
        KeyValueUtil.createKeyValueFromInputStream(
          new DataInputStream(new ByteArrayInputStream(baos.toByteArray())), c.withTags);
        if (c.expectedMessage != null) {
          fail("Should fail when parse kv from an invalid bytes, case#" + i + ". " + c);
        }
      } catch (IllegalArgumentException e) {
        assertEquals("Case#" + i + " failed," + c, c.getExpectedMessage(), e.getMessage());
      }
    }
  }
}

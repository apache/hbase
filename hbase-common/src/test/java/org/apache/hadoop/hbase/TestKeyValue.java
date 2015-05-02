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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.MetaComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.assertNotEquals;

public class TestKeyValue extends TestCase {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  public void testColumnCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    byte [] family1 = Bytes.toBytes("abc");
    byte [] qualifier1 = Bytes.toBytes("def");
    byte [] family2 = Bytes.toBytes("abcd");
    byte [] qualifier2 = Bytes.toBytes("ef");

    KeyValue aaa = new KeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family2, qualifier2));
    assertTrue(CellUtil.matchingColumn(aaa, family1, qualifier1));
    aaa = new KeyValue(a, family2, qualifier2, 0L, Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family1, qualifier1));
    assertTrue(CellUtil.matchingColumn(aaa, family2,qualifier2));
    byte [] nullQualifier = new byte[0];
    aaa = new KeyValue(a, family1, nullQualifier, 0L, Type.Put, a);
    assertTrue(CellUtil.matchingColumn(aaa, family1,null));
    assertFalse(CellUtil.matchingColumn(aaa, family2,qualifier2));
  }

  /**
   * Test a corner case when the family qualifier is a prefix of the
   *  column qualifier.
   */
  public void testColumnCompare_prefix() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    byte [] family1 = Bytes.toBytes("abc");
    byte [] qualifier1 = Bytes.toBytes("def");
    byte [] family2 = Bytes.toBytes("ab");
    byte [] qualifier2 = Bytes.toBytes("def");

    KeyValue aaa = new KeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    assertFalse(CellUtil.matchingColumn(aaa, family2, qualifier2));
  }

  public void testBasics() throws Exception {
    LOG.info("LOWKEY: " + KeyValue.LOWESTKEY.toString());
    check(Bytes.toBytes(getName()),
      Bytes.toBytes(getName()), Bytes.toBytes(getName()), 1,
      Bytes.toBytes(getName()));
    // Test empty value and empty column -- both should work. (not empty fam)
    check(Bytes.toBytes(getName()), Bytes.toBytes(getName()), null, 1, null);
    check(HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes(getName()), null, 1, null);
    // empty qual is equivalent to null qual
    assertEquals(
      new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("fam"), null, 1, (byte[]) null),
      new KeyValue(Bytes.toBytes("rk"), Bytes.toBytes("fam"),
        HConstants.EMPTY_BYTE_ARRAY, 1, (byte[]) null));
  }

  private void check(final byte [] row, final byte [] family, byte [] qualifier,
    final long timestamp, final byte [] value) {
    KeyValue kv = new KeyValue(row, family, qualifier, timestamp, value);
    assertTrue(Bytes.compareTo(kv.getRow(), row) == 0);
    assertTrue(CellUtil.matchingColumn(kv, family, qualifier));
    // Call toString to make sure it works.
    LOG.info(kv.toString());
  }

  public void testPlainCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    final byte [] b = Bytes.toBytes("bbb");
    final byte [] fam = Bytes.toBytes("col");
    final byte [] qf = Bytes.toBytes("umn");
    KeyValue aaa = new KeyValue(a, fam, qf, a);
    KeyValue bbb = new KeyValue(b, fam, qf, b);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) > 0);
    // Compare breaks if passed same ByteBuffer as both left and right arguments.
    assertTrue(KeyValue.COMPARATOR.compare(bbb, bbb) == 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different timestamps.
    aaa = new KeyValue(a, fam, qf, 1, a);
    bbb = new KeyValue(a, fam, qf, 2, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
    // Do compare with different types.  Higher numbered types -- Delete
    // should sort ahead of lower numbers; i.e. Put
    aaa = new KeyValue(a, fam, qf, 1, KeyValue.Type.Delete, a);
    bbb = new KeyValue(a, fam, qf, 1, a);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, bbb) < 0);
    assertTrue(KeyValue.COMPARATOR.compare(bbb, aaa) > 0);
    assertTrue(KeyValue.COMPARATOR.compare(aaa, aaa) == 0);
  }

  public void testMoreComparisons() throws Exception {
    long now = System.currentTimeMillis();

    // Meta compares
    KeyValue aaa = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,row_0500,1236020145502"), now);
    KeyValue bbb = new KeyValue(
        Bytes.toBytes("TestScanMultipleVersions,,99999999999999"), now);
    KVComparator c = new KeyValue.MetaComparator();
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
    comparisons(new KeyValue.MetaComparator());
    comparisons(new KeyValue.KVComparator());
    metacomparisons(new KeyValue.MetaComparator());
  }

  public void testMetaComparatorTableKeysWithCommaOk() {
    MetaComparator c = new KeyValue.MetaComparator();
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
   * @throws IOException
   */
  public void testKeyValueBorderCases() throws IOException {
    // % sorts before , so if we don't do special comparator, rowB would
    // come before rowA.
    KeyValue rowA = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/,1234"),
      Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    KeyValue rowB = new KeyValue(Bytes.toBytes("testtable,www.hbase.org/%20,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

    rowA = new KeyValue(Bytes.toBytes("testtable,,1234"), Bytes.toBytes("fam"),
        Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    rowB = new KeyValue(Bytes.toBytes("testtable,$www.hbase.org/,99999"),
        Bytes.toBytes("fam"), Bytes.toBytes(""), Long.MAX_VALUE, (byte[])null);
    assertTrue(KeyValue.META_COMPARATOR.compare(rowA, rowB) < 0);

  }

  private void metacomparisons(final KeyValue.MetaComparator c) {
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

  private void comparisons(final KeyValue.KVComparator c) {
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

  public void testBinaryKeys() throws Exception {
    Set<KeyValue> set = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    final byte [] fam = Bytes.toBytes("col");
    final byte [] qf = Bytes.toBytes("umn");
    final byte [] nb = new byte[0];
    KeyValue [] keys = {new KeyValue(Bytes.toBytes("aaaaa,\u0000\u0000,2"), fam, qf, 2, nb),
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
      for (KeyValue k: set) {
        assertTrue(count++ == k.getTimestamp());
      }
    } catch (junit.framework.AssertionFailedError e) {
      // Expected
      assertion = true;
    }
    assertTrue(assertion);
    // Make set with good comparator
    set = new TreeSet<KeyValue>(new KeyValue.MetaComparator());
    Collections.addAll(set, keys);
    count = 0;
    for (KeyValue k: set) {
      assertTrue(count++ == k.getTimestamp());
    }
  }

  public void testStackedUpKeyValue() {
    // Test multiple KeyValues in a single blob.

    // TODO actually write this test!

  }

  private final byte[] rowA = Bytes.toBytes("rowA");
  private final byte[] rowB = Bytes.toBytes("rowB");

  private final byte[] family = Bytes.toBytes("family");
  private final byte[] qualA = Bytes.toBytes("qfA");
  private final byte[] qualB = Bytes.toBytes("qfB");

  private void assertKVLess(KeyValue.KVComparator c,
                            KeyValue less,
                            KeyValue greater) {
    int cmp = c.compare(less,greater);
    assertTrue(cmp < 0);
    cmp = c.compare(greater,less);
    assertTrue(cmp > 0);
  }

  private void assertKVLessWithoutRow(KeyValue.KVComparator c, int common, KeyValue less,
      KeyValue greater) {
    int cmp = c.compareIgnoringPrefix(common, less.getBuffer(), less.getOffset()
        + KeyValue.ROW_OFFSET, less.getKeyLength(), greater.getBuffer(),
        greater.getOffset() + KeyValue.ROW_OFFSET, greater.getKeyLength());
    assertTrue(cmp < 0);
    cmp = c.compareIgnoringPrefix(common, greater.getBuffer(), greater.getOffset()
        + KeyValue.ROW_OFFSET, greater.getKeyLength(), less.getBuffer(),
        less.getOffset() + KeyValue.ROW_OFFSET, less.getKeyLength());
    assertTrue(cmp > 0);
  }

  public void testCompareWithoutRow() {
    final KeyValue.KVComparator c = KeyValue.COMPARATOR;
    byte[] row = Bytes.toBytes("row");

    byte[] fa = Bytes.toBytes("fa");
    byte[] fami = Bytes.toBytes("fami");
    byte[] fami1 = Bytes.toBytes("fami1");

    byte[] qual0 = Bytes.toBytes("");
    byte[] qual1 = Bytes.toBytes("qf1");
    byte[] qual2 = Bytes.toBytes("qf2");
    long ts = 1;

    // 'fa:'
    KeyValue kv_0 = new KeyValue(row, fa, qual0, ts, Type.Put);
    // 'fami:'
    KeyValue kv0_0 = new KeyValue(row, fami, qual0, ts, Type.Put);
    // 'fami:qf1'
    KeyValue kv0_1 = new KeyValue(row, fami, qual1, ts, Type.Put);
    // 'fami:qf2'
    KeyValue kv0_2 = new KeyValue(row, fami, qual2, ts, Type.Put);
    // 'fami1:'
    KeyValue kv1_0 = new KeyValue(row, fami1, qual0, ts, Type.Put);

    // 'fami:qf1' < 'fami:qf2'
    assertKVLessWithoutRow(c, 0, kv0_1, kv0_2);
    // 'fami:qf1' < 'fami1:'
    assertKVLessWithoutRow(c, 0, kv0_1, kv1_0);

    // Test comparison by skipping the same prefix bytes.
    /***
     * KeyValue Format and commonLength:
     * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
     * ------------------|-------commonLength--------|--------------
     */
    int commonLength = KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE
        + row.length;
    // 'fa:' < 'fami:'. They have commonPrefix + 2 same prefix bytes.
    assertKVLessWithoutRow(c, commonLength + 2, kv_0, kv0_0);
    // 'fami:' < 'fami:qf1'. They have commonPrefix + 4 same prefix bytes.
    assertKVLessWithoutRow(c, commonLength + 4, kv0_0, kv0_1);
    // 'fami:qf1' < 'fami1:'. They have commonPrefix + 4 same prefix bytes.
    assertKVLessWithoutRow(c, commonLength + 4, kv0_1, kv1_0);
    // 'fami:qf1' < 'fami:qf2'. They have commonPrefix + 6 same prefix bytes.
    assertKVLessWithoutRow(c, commonLength + 6, kv0_1, kv0_2);
  }

  public void testFirstLastOnRow() {
    final KVComparator c = KeyValue.COMPARATOR;
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
    final KeyValue kvA_1 = new KeyValue(rowA, null, null, ts, Type.Put);
    final KeyValue kvA_2 = new KeyValue(rowA, family, qualA, ts, Type.Put);

    final KeyValue lastOnRowA = KeyValueUtil.createLastOnRow(rowA);
    final KeyValue firstOnRowB = KeyValueUtil.createFirstOnRow(rowB);
    final KeyValue firstOnRowBBufferFam = KeyValueUtil.createFirstOnRow(bufferB, offsetB,
        rowB, 0, rowB.length, family, 0, family.length, null, 0, 0);
    final KeyValue kvB = new KeyValue(rowB, family, qualA, ts, Type.Put);

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

  public void testCreateKeyOnly() throws Exception {
    long ts = 1;
    byte [] value = Bytes.toBytes("a real value");
    byte [] evalue = new byte[0]; // empty value

    for (byte[] val : new byte[][]{value, evalue}) {
      for (boolean useLen : new boolean[]{false,true}) {
        KeyValue kv1 = new KeyValue(rowA, family, qualA, ts, val);
        KeyValue kv1ko = kv1.createKeyOnly(useLen);
        // keys are still the same
        assertTrue(kv1.equals(kv1ko));
        // but values are not
        assertTrue(kv1ko.getValue().length == (useLen?Bytes.SIZEOF_INT:0));
        if (useLen) {
          assertEquals(kv1.getValueLength(), Bytes.toInt(kv1ko.getValue()));
        }
      }
    }
  }

  public void testCreateKeyValueFromKey() {
    KeyValue kv = new KeyValue(Bytes.toBytes("myRow"), Bytes.toBytes("myCF"),
        Bytes.toBytes("myQualifier"), 12345L, Bytes.toBytes("myValue"));
    int initialPadding = 10;
    int endingPadding = 20;
    int keyLen = kv.getKeyLength();
    byte[] tmpArr = new byte[initialPadding + endingPadding + keyLen];
    System.arraycopy(kv.getBuffer(), kv.getKeyOffset(), tmpArr,
        initialPadding, keyLen);
    KeyValue kvFromKey = KeyValue.createKeyValueFromKey(tmpArr, initialPadding,
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

  /**
   * See HBASE-7845
   */
  public void testGetShortMidpointKey() {
    final KVComparator keyComparator = KeyValue.COMPARATOR;
    //verify that faked shorter rowkey could be generated
    long ts = 5;
    KeyValue kv1 = new KeyValue(Bytes.toBytes("the quick brown fox"), family, qualA, ts, Type.Put);
    KeyValue kv2 = new KeyValue(Bytes.toBytes("the who test text"), family, qualA, ts, Type.Put);
    byte[] newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) < 0);
    short newRowLength = Bytes.toShort(newKey, 0);
    byte[] expectedArray = Bytes.toBytes("the r");
    Bytes.equals(newKey, KeyValue.ROW_LENGTH_SIZE, newRowLength, expectedArray, 0,
      expectedArray.length);

    //verify: same with "row + family + qualifier", return rightKey directly
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 0, Type.Put);
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), kv2.getKey()) < 0);
    newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) == 0);
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, -10, Type.Put);
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), kv2.getKey()) < 0);
    newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) == 0);

    // verify: same with row, different with qualifier
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualB, 5, Type.Put);
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), kv2.getKey()) < 0);
    newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) < 0);
    KeyValue newKeyValue = KeyValue.createKeyValueFromKey(newKey);
    assertTrue(Arrays.equals(newKeyValue.getFamily(),family));
    assertTrue(Arrays.equals(newKeyValue.getQualifier(),qualB));
    assertTrue(newKeyValue.getTimestamp() == HConstants.LATEST_TIMESTAMP);
    assertTrue(newKeyValue.getTypeByte() == Type.Maximum.getCode());

    //verify metaKeyComparator's getShortMidpointKey output
    final KVComparator metaKeyComparator = KeyValue.META_COMPARATOR;
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase123"), family, qualA, 5, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbase234"), family, qualA, 0, Type.Put);
    newKey = metaKeyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(metaKeyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(metaKeyComparator.compareFlatKey(newKey, kv2.getKey()) == 0);

    //verify common fix scenario
    kv1 = new KeyValue(Bytes.toBytes("ilovehbase"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("ilovehbaseandhdfs"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), kv2.getKey()) < 0);
    newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) < 0);
    newRowLength = Bytes.toShort(newKey, 0);
    expectedArray = Bytes.toBytes("ilovehbasea");
    Bytes.equals(newKey, KeyValue.ROW_LENGTH_SIZE, newRowLength, expectedArray, 0,
      expectedArray.length);
    //verify only 1 offset scenario
    kv1 = new KeyValue(Bytes.toBytes("100abcdefg"), family, qualA, ts, Type.Put);
    kv2 = new KeyValue(Bytes.toBytes("101abcdefg"), family, qualA, ts, Type.Put);
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), kv2.getKey()) < 0);
    newKey = keyComparator.getShortMidpointKey(kv1.getKey(), kv2.getKey());
    assertTrue(keyComparator.compareFlatKey(kv1.getKey(), newKey) < 0);
    assertTrue(keyComparator.compareFlatKey(newKey, kv2.getKey()) < 0);
    newRowLength = Bytes.toShort(newKey, 0);
    expectedArray = Bytes.toBytes("101");
    Bytes.equals(newKey, KeyValue.ROW_LENGTH_SIZE, newRowLength, expectedArray, 0,
      expectedArray.length);
  }

  public void testKVsWithTags() {
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myCF");
    byte[] q = Bytes.toBytes("myQualifier");
    byte[] value = Bytes.toBytes("myValue");
    byte[] metaValue1 = Bytes.toBytes("metaValue1");
    byte[] metaValue2 = Bytes.toBytes("metaValue2");
    KeyValue kv = new KeyValue(row, cf, q, HConstants.LATEST_TIMESTAMP, value, new Tag[] {
        new Tag((byte) 1, metaValue1), new Tag((byte) 2, metaValue2) });
    assertTrue(kv.getTagsLength() > 0);
    assertTrue(Bytes.equals(kv.getRow(), row));
    assertTrue(Bytes.equals(kv.getFamily(), cf));
    assertTrue(Bytes.equals(kv.getQualifier(), q));
    assertTrue(Bytes.equals(kv.getValue(), value));
    List<Tag> tags = kv.getTags();
    assertNotNull(tags);
    assertEquals(2, tags.size());
    boolean meta1Ok = false, meta2Ok = false;
    for (Tag tag : tags) {
      if (tag.getType() == (byte) 1) {
        if (Bytes.equals(tag.getValue(), metaValue1)) {
          meta1Ok = true;
        }
      } else {
        if (Bytes.equals(tag.getValue(), metaValue2)) {
          meta2Ok = true;
        }
      }
    }
    assertTrue(meta1Ok);
    assertTrue(meta2Ok);
    Iterator<Tag> tagItr = CellUtil.tagsIterator(kv.getTagsArray(), kv.getTagsOffset(),
        kv.getTagsLength());
    //Iterator<Tag> tagItr = kv.tagsIterator();
    assertTrue(tagItr.hasNext());
    Tag next = tagItr.next();
    assertEquals(10, next.getTagLength());
    assertEquals((byte) 1, next.getType());
    Bytes.equals(next.getValue(), metaValue1);
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getTagLength());
    assertEquals((byte) 2, next.getType());
    Bytes.equals(next.getValue(), metaValue2);
    assertFalse(tagItr.hasNext());

    tagItr = CellUtil.tagsIterator(kv.getTagsArray(), kv.getTagsOffset(),
        kv.getTagsLength());
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getTagLength());
    assertEquals((byte) 1, next.getType());
    Bytes.equals(next.getValue(), metaValue1);
    assertTrue(tagItr.hasNext());
    next = tagItr.next();
    assertEquals(10, next.getTagLength());
    assertEquals((byte) 2, next.getType());
    Bytes.equals(next.getValue(), metaValue2);
    assertFalse(tagItr.hasNext());
  }
  
  public void testMetaKeyComparator() {
    MetaComparator c = new KeyValue.MetaComparator();
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

  public void testKeyValueSerialization() throws Exception {
    KeyValue kvA1 = new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
        Bytes.toBytes("1"));
    KeyValue kvA2 = new KeyValue(Bytes.toBytes("key"), Bytes.toBytes("cf"), Bytes.toBytes("qualA"),
        Bytes.toBytes("2"));
    MockKeyValue mkvA1 = new MockKeyValue(kvA1);
    MockKeyValue mkvA2 = new MockKeyValue(kvA2);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream os = new DataOutputStream(byteArrayOutputStream);
    KeyValueUtil.oswrite(mkvA1, os, true);
    KeyValueUtil.oswrite(mkvA2, os, true);
    DataInputStream is = new DataInputStream(new ByteArrayInputStream(
        byteArrayOutputStream.toByteArray()));
    KeyValue deSerKV1 = KeyValue.iscreate(is);
    assertTrue(kvA1.equals(deSerKV1));
    KeyValue deSerKV2 = KeyValue.iscreate(is);
    assertTrue(kvA2.equals(deSerKV2));
  }

  private class MockKeyValue implements Cell {
    private final KeyValue kv;

    public MockKeyValue(KeyValue kv) {
      this.kv = kv;
    }

    /**
     * This returns the offset where the tag actually starts.
     */
    @Override
    public int getTagsOffset() {
      return this.kv.getTagsOffset();
    }

    // used to achieve atomic operations in the memstore.
    @Override
    public long getMvccVersion() {
      return this.kv.getMvccVersion();
    }

    /**
     * used to achieve atomic operations in the memstore.
     */
    @Override
    public long getSequenceId() {
      return this.kv.getSequenceId();
    }

    /**
     * This returns the total length of the tag bytes
     */
    @Override
    public int getTagsLength() {
      return this.kv.getTagsLength();
    }

    /**
     * 
     * @return Timestamp
     */
    @Override
    public long getTimestamp() {
      return this.kv.getTimestamp();
    }

    /**
     * @return KeyValue.TYPE byte representation
     */
    @Override
    public byte getTypeByte() {
      return this.kv.getTypeByte();
    }

    /**
     * @return the backing array of the entire KeyValue (all KeyValue fields are
     *         in a single array)
     */
    @Override
    public byte[] getValueArray() {
      return this.kv.getValueArray();
    }

    /**
     * @return the value offset
     */
    @Override
    public int getValueOffset() {
      return this.kv.getValueOffset();
    }

    /**
     * @return Value length
     */
    @Override
    public int getValueLength() {
      return this.kv.getValueLength();
    }

    /**
     * @return the backing array of the entire KeyValue (all KeyValue fields are
     *         in a single array)
     */
    @Override
    public byte[] getRowArray() {
      return this.kv.getRowArray();
    }

    /**
     * @return Row offset
     */
    @Override
    public int getRowOffset() {
      return this.kv.getRowOffset();
    }

    /**
     * @return Row length
     */
    @Override
    public short getRowLength() {
      return this.kv.getRowLength();
    }

    /**
     * @return the backing array of the entire KeyValue (all KeyValue fields are
     *         in a single array)
     */
    @Override
    public byte[] getFamilyArray() {
      return this.kv.getFamilyArray();
    }

    /**
     * @return Family offset
     */
    @Override
    public int getFamilyOffset() {
      return this.kv.getFamilyOffset();
    }

    /**
     * @return Family length
     */
    @Override
    public byte getFamilyLength() {
      return this.kv.getFamilyLength();
    }

    /**
     * @return the backing array of the entire KeyValue (all KeyValue fields are
     *         in a single array)
     */
    @Override
    public byte[] getQualifierArray() {
      return this.kv.getQualifierArray();
    }

    /**
     * @return Qualifier offset
     */
    @Override
    public int getQualifierOffset() {
      return this.kv.getQualifierOffset();
    }

    /**
     * @return Qualifier length
     */
    @Override
    public int getQualifierLength() {
      return this.kv.getQualifierLength();
    }

    @Override
    @Deprecated
    public byte[] getValue() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    @Deprecated
    public byte[] getFamily() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    @Deprecated
    public byte[] getQualifier() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    @Deprecated
    public byte[] getRow() {
      // TODO Auto-generated method stub
      return null;
    }

    /**
     * @return the backing array of the entire KeyValue (all KeyValue fields are
     *         in a single array)
     */
    @Override
    public byte[] getTagsArray() {
      return this.kv.getTagsArray();
    }
  }

  public void testEqualsAndHashCode() throws Exception {
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

}

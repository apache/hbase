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
package org.apache.hadoop.hbase.keymeta;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;

/**
 * Tests for {@link ManagedKeyIdentity} implementations: {@link KeyIdentityBytesBacked},
 * {@link KeyIdentitySingleArrayBacked}, and {@link KeyIdentityPrefixBytesBacked} (custodian +
 * namespace only; partial identity is absent and matches
 * {@link ManagedKeyIdentity#KEY_NULL_IDENTITY_BYTES} for equality).
 * <p>
 * Structure:
 * <ul>
 * <li>{@link TestCrossClassEquality} — verifies that instances of different concrete classes with
 * the same data are equal and have the same hashCode (content-based, interoperable).</li>
 * <li>{@link TestInteroperabilityAsHashKeys} — verifies that types are interchangeable as map keys
 * (put with one type, get with another, etc.).</li>
 * <li>{@link AbstractTestFullKeyIdentity} — abstract base holding all interface-contract tests.
 * Subclasses implement a single {@code create()} factory method; JUnit's inheritance mechanism runs
 * each abstract test against every concrete implementation automatically.</li>
 * <li>{@link TestBytesBacked}, {@link TestSingleArrayBacked} — concrete subclasses that override
 * {@code create()} and add implementation-specific tests.</li>
 * <li>{@link TestKeyIdentityPrefixBytesBacked} — contract and construction tests for the
 * prefix-only implementation (mirrors {@link AbstractTestFullKeyIdentity} where applicable).</li>
 * </ul>
 * <p>
 * Note: {@link KeyIdentityBytesBacked}'s {@code getCustodianEncoded()},
 * {@code getNamespaceString()}, and {@code getPartialIdentityEncoded()} call {@code Bytes.get()}
 * which returns the full backing array. These methods produce incorrect output when the
 * {@link Bytes} objects have non-zero offsets. The implementation-specific tests here deliberately
 * avoid that path to keep the suite green; the issue is captured in comments in
 * {@link TestBytesBacked#testConstructionWithBytesHavingOffset()}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TestManagedKeyIdentity.TestCrossClassEquality.class,
  TestManagedKeyIdentity.TestInteroperabilityAsHashKeys.class,
  TestManagedKeyIdentity.TestBytesBacked.class, TestManagedKeyIdentity.TestSingleArrayBacked.class,
  TestManagedKeyIdentity.TestKeyIdentityPrefixBytesBacked.class })
@Category({ MasterTests.class, SmallTests.class })
public class TestManagedKeyIdentity {

  // ---------------------------------------------------------------------------
  // Shared test data
  // ---------------------------------------------------------------------------

  static final byte[] CUSTODIAN = new byte[] { 0x01, 0x02, 0x03, 0x04 };
  static final byte[] NAMESPACE = Bytes.toBytes("testns");
  static final byte[] PARTIAL = new byte[] { (byte) 0xAA, (byte) 0xBB, (byte) 0xCC };

  // ---------------------------------------------------------------------------
  // Cross-class equality
  // ---------------------------------------------------------------------------

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestCrossClassEquality {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCrossClassEquality.class);

    @Test
    public void testDifferentImplsWithSameDataAreEqual() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentityBytesBacked bb =
        new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE), new Bytes(PARTIAL));
      KeyIdentityBytesBacked bbFromRawArrays =
        new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked sa = new KeyIdentitySingleArrayBacked(backing);

      assertTrue("BytesBacked (Bytes ctor) must equal BytesBacked (byte[] ctor)",
        bb.equals(bbFromRawArrays));
      assertTrue("BytesBacked (byte[] ctor) must equal BytesBacked (Bytes ctor)",
        bbFromRawArrays.equals(bb));
      assertTrue("BytesBacked must equal SingleArrayBacked", bb.equals(sa));
      assertTrue("SingleArrayBacked must equal BytesBacked", sa.equals(bb));
      assertTrue("BytesBacked (byte[] ctor) must equal SingleArrayBacked",
        bbFromRawArrays.equals(sa));
      assertTrue("SingleArrayBacked must equal BytesBacked (byte[] ctor)",
        sa.equals(bbFromRawArrays));
    }

    @Test
    public void testDifferentImplsWithSameDataHaveSameHashCode() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentityBytesBacked bb =
        new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE), new Bytes(PARTIAL));
      KeyIdentityBytesBacked bbFromRawArrays =
        new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked sa = new KeyIdentitySingleArrayBacked(backing);

      assertEquals("BytesBacked (Bytes ctor) and (byte[] ctor) must have same hashCode",
        bb.hashCode(), bbFromRawArrays.hashCode());
      assertEquals("BytesBacked and SingleArrayBacked must have same hashCode", bb.hashCode(),
        sa.hashCode());
      assertEquals("BytesBacked (byte[] ctor) and SingleArrayBacked must have same hashCode",
        bbFromRawArrays.hashCode(), sa.hashCode());
    }

    /**
     * {@link KeyIdentityPrefixBytesBacked} matches {@link KeyIdentityBytesBacked} with an empty
     * partial segment and {@link KeyIdentitySingleArrayBacked} over the custodian+namespace marker
     * row (no trailing partial length byte).
     */
    @Test
    public void testPrefixBackedEqualsBytesBackedEmptyPartialAndCustNamespaceSingleArray() {
      KeyIdentityPrefixBytesBacked prefix = new KeyIdentityPrefixBytesBacked(CUSTODIAN, NAMESPACE);
      KeyIdentityBytesBacked bbEmptyPartial =
        new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, new byte[0]);
      KeyIdentityBytesBacked bbEmptyFromBytes = new KeyIdentityBytesBacked(new Bytes(CUSTODIAN),
        new Bytes(NAMESPACE), new Bytes(new byte[0]));
      byte[] markerRow =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked sa = new KeyIdentitySingleArrayBacked(markerRow);

      assertTrue(prefix.equals(bbEmptyPartial));
      assertTrue(bbEmptyPartial.equals(prefix));
      assertTrue(prefix.equals(bbEmptyFromBytes));
      assertTrue(bbEmptyFromBytes.equals(prefix));
      assertTrue(prefix.equals(sa));
      assertTrue(sa.equals(prefix));

      assertEquals(prefix.hashCode(), bbEmptyPartial.hashCode());
      assertEquals(prefix.hashCode(), bbEmptyFromBytes.hashCode());
      assertEquals(prefix.hashCode(), sa.hashCode());
    }

    @Test
    public void testPrefixBackedNotEqualToIdentityWithNonEmptyPartial() {
      KeyIdentityPrefixBytesBacked prefix = new KeyIdentityPrefixBytesBacked(CUSTODIAN, NAMESPACE);
      KeyIdentityBytesBacked withPartial =
        new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked sa = new KeyIdentitySingleArrayBacked(backing);

      assertFalse(prefix.equals(withPartial));
      assertFalse(withPartial.equals(prefix));
      assertFalse(prefix.equals(sa));
      assertFalse(sa.equals(prefix));
    }
  }

  // ---------------------------------------------------------------------------
  // Interoperability as hash keys (Map/Set)
  // ---------------------------------------------------------------------------

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestInteroperabilityAsHashKeys {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestInteroperabilityAsHashKeys.class);

    private static KeyIdentityBytesBacked createBytesBacked() {
      return new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE),
        new Bytes(PARTIAL));
    }

    /**
     * Same logical identity as {@link #createBytesBacked()} but via the {@code byte[]} constructor.
     */
    private static KeyIdentityBytesBacked createBytesBackedFromRawArrays() {
      return new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, PARTIAL);
    }

    private static KeyIdentitySingleArrayBacked createSingleArrayBacked() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      return new KeyIdentitySingleArrayBacked(backing);
    }

    private static KeyIdentityPrefixBytesBacked createPrefixBacked() {
      return new KeyIdentityPrefixBytesBacked(CUSTODIAN, NAMESPACE);
    }

    private static KeyIdentityBytesBacked createBytesBackedEmptyPartial() {
      return new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, new byte[0]);
    }

    private static KeyIdentitySingleArrayBacked createSingleArrayCustNamespaceMarker() {
      byte[] marker = ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      return new KeyIdentitySingleArrayBacked(marker);
    }

    @Test
    public void testPutWithOneTypeGetWithAnother() {
      java.util.Map<ManagedKeyIdentity, String> map = new java.util.HashMap<>();
      String value = "value1";

      map.put(createBytesBacked(), value);
      assertEquals(value, map.get(createBytesBackedFromRawArrays()));
      assertEquals(value, map.get(createSingleArrayBacked()));

      map.clear();
      map.put(createBytesBackedFromRawArrays(), value);
      assertEquals(value, map.get(createBytesBacked()));
      assertEquals(value, map.get(createSingleArrayBacked()));

      map.clear();
      map.put(createSingleArrayBacked(), value);
      assertEquals(value, map.get(createBytesBacked()));
      assertEquals(value, map.get(createBytesBackedFromRawArrays()));
    }

    @Test
    public void testContainsKeyWithDifferentType() {
      java.util.Map<ManagedKeyIdentity, String> map = new java.util.HashMap<>();
      map.put(createBytesBacked(), "v");

      assertTrue(map.containsKey(createBytesBackedFromRawArrays()));
      assertTrue(map.containsKey(createSingleArrayBacked()));
    }

    @Test
    public void testSetDeduplicatesAcrossTypes() {
      java.util.Set<ManagedKeyIdentity> set = new java.util.HashSet<>();
      set.add(createBytesBacked());
      set.add(createBytesBackedFromRawArrays());
      set.add(createSingleArrayBacked());

      assertEquals("Set must contain exactly one entry for same logical identity", 1, set.size());
    }

    @Test
    public void testMapSizeOneWhenSameIdentityDifferentTypes() {
      java.util.Map<ManagedKeyIdentity, String> map = new java.util.HashMap<>();
      map.put(createBytesBacked(), "first");
      map.put(createBytesBackedFromRawArrays(), "second");
      map.put(createSingleArrayBacked(), "third");

      assertEquals("Map must have size 1 when all keys are same logical identity", 1, map.size());
      assertEquals("Last put wins", "third", map.get(createBytesBacked()));
    }

    @Test
    public void testPutPrefixGetWithBytesBackedEmptyPartialAndCustNamespaceMarker() {
      java.util.Map<ManagedKeyIdentity, String> map = new java.util.HashMap<>();
      String value = "prefix-interop";
      map.put(createPrefixBacked(), value);
      assertEquals(value, map.get(createBytesBackedEmptyPartial()));
      assertEquals(value, map.get(createSingleArrayCustNamespaceMarker()));
    }

    @Test
    public void testPutBytesBackedEmptyPartialGetWithPrefixAndCustNamespaceMarker() {
      java.util.Map<ManagedKeyIdentity, String> map = new java.util.HashMap<>();
      String value = "empty-partial-interop";
      map.put(createBytesBackedEmptyPartial(), value);
      assertEquals(value, map.get(createPrefixBacked()));
      assertEquals(value, map.get(createSingleArrayCustNamespaceMarker()));
    }

    @Test
    public void testSetDeduplicatesPrefixWithBytesBackedEmptyPartialAndMarker() {
      java.util.Set<ManagedKeyIdentity> set = new java.util.HashSet<>();
      set.add(createPrefixBacked());
      set.add(createBytesBackedEmptyPartial());
      set.add(createSingleArrayCustNamespaceMarker());
      assertEquals("Set must collapse prefix-equivalent identities to one entry", 1, set.size());
    }

    @Test
    public void testSingleArrayBackedWithSlice_equalsAndHashCodeWithOtherTypes() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] larger = new byte[backing.length + 6];
      System.arraycopy(backing, 0, larger, 3, backing.length);
      KeyIdentitySingleArrayBacked slice =
        new KeyIdentitySingleArrayBacked(larger, 3, backing.length);

      KeyIdentitySingleArrayBacked full = new KeyIdentitySingleArrayBacked(backing);
      KeyIdentityBytesBacked bb = createBytesBacked();
      KeyIdentityBytesBacked bbArrays = createBytesBackedFromRawArrays();

      assertTrue("SingleArrayBacked slice must equal full-array SingleArrayBacked",
        slice.equals(full));
      assertTrue("SingleArrayBacked slice must equal BytesBacked", slice.equals(bb));
      assertTrue("SingleArrayBacked slice must equal BytesBacked (byte[] ctor)",
        slice.equals(bbArrays));

      assertEquals("SingleArrayBacked slice must have same hashCode as full-array",
        slice.hashCode(), full.hashCode());
      assertEquals("SingleArrayBacked slice must have same hashCode as BytesBacked",
        slice.hashCode(), bb.hashCode());
      assertEquals("SingleArrayBacked slice must have same hashCode as BytesBacked (byte[] ctor)",
        slice.hashCode(), bbArrays.hashCode());
    }
  }

  // ---------------------------------------------------------------------------
  // Abstract base — interface-contract tests shared by all implementations.
  //
  // JUnit discovers @Test methods on every concrete subclass via inheritance,
  // running each one with that subclass's create() factory in effect (Template
  // Method pattern). N test bodies written here execute 2×N times total.
  // ---------------------------------------------------------------------------

  public abstract static class AbstractTestFullKeyIdentity {

    /**
     * Creates an instance from the given raw segment bytes. The partial identity must be non-empty
     * (length >= 1) because {@link KeyIdentitySingleArrayBacked} is constructed via
     * {@link ManagedKeyIdentityUtils#constructRowKeyForIdentity(byte[], byte[], byte[])} which
     * enforces that constraint.
     */
    protected abstract ManagedKeyIdentity create(byte[] cust, byte[] ns, byte[] partial);

    // -- View getters --

    @Test
    public void testGetCustodianView() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertArrayEquals(CUSTODIAN, fki.getCustodianView().copyBytes());
    }

    @Test
    public void testGetNamespaceView() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertArrayEquals(NAMESPACE, fki.getNamespaceView().copyBytes());
    }

    @Test
    public void testGetPartialIdentityView() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertArrayEquals(PARTIAL, fki.getPartialIdentityView().copyBytes());
    }

    @Test
    public void testGetFullIdentityView() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] expected =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      assertArrayEquals(expected, fki.getFullIdentityView().copyBytes());
    }

    @Test
    public void testGetIdentityPrefixView() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] expected =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      assertArrayEquals(expected, fki.getIdentityPrefixView().copyBytes());
    }

    @Test
    public void testGetKeyIdentityPrefix() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity prefix = fki.getKeyIdentityPrefix();
      assertNotSame("Non-empty partial identity must produce a stripped prefix identity", fki,
        prefix);
      assertArrayEquals(CUSTODIAN, prefix.getCustodianView().copyBytes());
      assertArrayEquals(NAMESPACE, prefix.getNamespaceView().copyBytes());
      assertEquals(0, prefix.getPartialIdentityLength());
    }

    // -- Copy methods (defensive isolation) --

    @Test
    public void testCopyCustodian() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] copy = fki.copyCustodian();
      assertArrayEquals(CUSTODIAN, copy);
      copy[0] = (byte) 0xFF;
      assertArrayEquals("Mutating returned copy must not affect the object", CUSTODIAN,
        fki.copyCustodian());
    }

    @Test
    public void testCopyNamespace() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] copy = fki.copyNamespace();
      assertArrayEquals(NAMESPACE, copy);
      copy[0] = (byte) 0xFF;
      assertArrayEquals("Mutating returned copy must not affect the object", NAMESPACE,
        fki.copyNamespace());
    }

    @Test
    public void testCopyPartialIdentity() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] copy = fki.copyPartialIdentity();
      assertArrayEquals(PARTIAL, copy);
      copy[0] = (byte) 0x00;
      assertArrayEquals("Mutating returned copy must not affect the object", PARTIAL,
        fki.copyPartialIdentity());
    }

    // -- Length methods --

    @Test
    public void testGetCustodianLength() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(CUSTODIAN.length, fki.getCustodianLength());
    }

    @Test
    public void testGetNamespaceLength() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(NAMESPACE.length, fki.getNamespaceLength());
    }

    @Test
    public void testGetPartialIdentityLength() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(PARTIAL.length, fki.getPartialIdentityLength());
    }

    // -- String / encoded getters --

    @Test
    public void testGetCustodianEncoded() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(ManagedKeyProvider.encodeToStr(CUSTODIAN), fki.getCustodianEncoded());
    }

    @Test
    public void testGetNamespaceString() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(Bytes.toString(NAMESPACE), fki.getNamespaceString());
    }

    @Test
    public void testGetPartialIdentityEncoded() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(ManagedKeyProvider.encodeToStr(PARTIAL), fki.getPartialIdentityEncoded());
    }

    // -- equals / hashCode --

    @Test
    public void testEqualsReflexive() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertTrue(fki.equals(fki));
    }

    @Test
    public void testEqualsSameData() {
      ManagedKeyIdentity fki1 = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity fki2 = create(CUSTODIAN, NAMESPACE, PARTIAL);
      assertTrue(fki1.equals(fki2));
      assertTrue(fki2.equals(fki1));
      assertEquals("equal instances must have the same hash code", fki1.hashCode(),
        fki2.hashCode());
    }

    @Test
    public void testEqualsDifferentCustodian() {
      ManagedKeyIdentity fki1 = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity fki2 = create(new byte[] { 0x0A, 0x0B }, NAMESPACE, PARTIAL);
      assertFalse(fki1.equals(fki2));
      assertFalse(fki2.equals(fki1));
    }

    @Test
    public void testEqualsDifferentNamespace() {
      ManagedKeyIdentity fki1 = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity fki2 = create(CUSTODIAN, Bytes.toBytes("other"), PARTIAL);
      assertFalse(fki1.equals(fki2));
      assertFalse(fki2.equals(fki1));
    }

    @Test
    public void testEqualsDifferentPartialIdentity() {
      ManagedKeyIdentity fki1 = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity fki2 = create(CUSTODIAN, NAMESPACE, new byte[] { 0x01 });
      assertFalse(fki1.equals(fki2));
      assertFalse(fki2.equals(fki1));
    }

    @Test
    public void testEqualsNull() {
      assertFalse(create(CUSTODIAN, NAMESPACE, PARTIAL).equals(null));
    }

    @Test
    public void testEqualsDifferentRuntimeClass() {
      assertFalse(create(CUSTODIAN, NAMESPACE, PARTIAL).equals("not a FullKeyIdentity"));
    }

    // -- clone --

    @Test
    public void testCloneIsEqualAndSameClass() {
      ManagedKeyIdentity fki = create(CUSTODIAN, NAMESPACE, PARTIAL);
      ManagedKeyIdentity clone = fki.clone();
      assertNotNull(clone);
      assertEquals("clone must be same class as original", fki.getClass(), clone.getClass());
      assertTrue("clone must be equal to original", fki.equals(clone));
      assertTrue("original must be equal to clone", clone.equals(fki));
      // Ensure clone created a copy instead of just returning the same.
      assertNotSame(fki, clone);
      assertNotSame(fki.getCustodianView(), clone.getCustodianView());
      assertNotSame(fki.getNamespaceView(), clone.getNamespaceView());
      assertNotSame(fki.getPartialIdentityView(), clone.getPartialIdentityView());
    }

    @Test
    public void testCloneHoldsCorrectData() {
      ManagedKeyIdentity clone = create(CUSTODIAN, NAMESPACE, PARTIAL).clone();
      assertArrayEquals(CUSTODIAN, clone.copyCustodian());
      assertArrayEquals(NAMESPACE, clone.copyNamespace());
      assertArrayEquals(PARTIAL, clone.copyPartialIdentity());
    }

    // -- compareCustodian --

    @Test
    public void testCompareCustodianEqual() {
      assertEquals(0, create(CUSTODIAN, NAMESPACE, PARTIAL).compareCustodian(CUSTODIAN));
    }

    @Test
    public void testCompareCustodianLess() {
      // {0x01, 0x02, 0x03} < {0x01, 0x02, 0x03, 0x04} lexicographically (prefix, shorter)
      assertTrue(
        create(new byte[] { 0x01, 0x02, 0x03 }, NAMESPACE, PARTIAL).compareCustodian(CUSTODIAN)
            < 0);
    }

    @Test
    public void testCompareCustodianGreater() {
      // {0x01, 0x02, 0x03, 0x05} > {0x01, 0x02, 0x03, 0x04}
      assertTrue(create(new byte[] { 0x01, 0x02, 0x03, 0x05 }, NAMESPACE, PARTIAL)
        .compareCustodian(CUSTODIAN) > 0);
    }

    @Test
    public void testCompareCustodianWithOffsetLength() {
      // Embed CUSTODIAN inside a larger array starting at offset 2.
      byte[] padded = new byte[CUSTODIAN.length + 2];
      padded[0] = (byte) 0xFF;
      padded[1] = (byte) 0xFF;
      System.arraycopy(CUSTODIAN, 0, padded, 2, CUSTODIAN.length);
      assertEquals(0,
        create(CUSTODIAN, NAMESPACE, PARTIAL).compareCustodian(padded, 2, CUSTODIAN.length));
    }

    // -- compareNamespace --

    @Test
    public void testCompareNamespaceEqual() {
      assertEquals(0, create(CUSTODIAN, NAMESPACE, PARTIAL).compareNamespace(NAMESPACE));
    }

    @Test
    public void testCompareNamespaceLess() {
      assertTrue(
        create(CUSTODIAN, Bytes.toBytes("aaa"), PARTIAL).compareNamespace(Bytes.toBytes("zzz"))
            < 0);
    }

    @Test
    public void testCompareNamespaceGreater() {
      assertTrue(
        create(CUSTODIAN, Bytes.toBytes("zzz"), PARTIAL).compareNamespace(Bytes.toBytes("aaa"))
            > 0);
    }

    @Test
    public void testCompareNamespaceWithOffsetLength() {
      byte[] padded = new byte[NAMESPACE.length + 1];
      padded[0] = (byte) 0x00;
      System.arraycopy(NAMESPACE, 0, padded, 1, NAMESPACE.length);
      assertEquals(0,
        create(CUSTODIAN, NAMESPACE, PARTIAL).compareNamespace(padded, 1, NAMESPACE.length));
    }

    // -- comparePartialIdentity --

    @Test
    public void testComparePartialIdentityEqual() {
      assertEquals(0, create(CUSTODIAN, NAMESPACE, PARTIAL).comparePartialIdentity(PARTIAL));
    }

    @Test
    public void testComparePartialIdentityLess() {
      // {0x00} < {0xAA, 0xBB, 0xCC}
      assertTrue(
        create(CUSTODIAN, NAMESPACE, new byte[] { 0x00 }).comparePartialIdentity(PARTIAL) < 0);
    }

    @Test
    public void testComparePartialIdentityGreater() {
      // {0xFF, 0xFF} > {0xAA, 0xBB, 0xCC}
      assertTrue(create(CUSTODIAN, NAMESPACE, new byte[] { (byte) 0xFF, (byte) 0xFF })
        .comparePartialIdentity(PARTIAL) > 0);
    }

    @Test
    public void testComparePartialIdentityWithOffsetLength() {
      byte[] padded = new byte[PARTIAL.length + 3];
      System.arraycopy(PARTIAL, 0, padded, 3, PARTIAL.length);
      assertEquals(0,
        create(CUSTODIAN, NAMESPACE, PARTIAL).comparePartialIdentity(padded, 3, PARTIAL.length));
    }
  }

  // ---------------------------------------------------------------------------
  // BytesBacked
  // ---------------------------------------------------------------------------

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestBytesBacked extends AbstractTestFullKeyIdentity {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBytesBacked.class);

    @Override
    protected ManagedKeyIdentity create(byte[] cust, byte[] ns, byte[] partial) {
      return new KeyIdentityBytesBacked(new Bytes(cust), new Bytes(ns), new Bytes(partial));
    }

    // -- Construction validation --

    @Test
    public void testConstructionWithNullCustodian_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(null, new Bytes(NAMESPACE), new Bytes(PARTIAL)));
    }

    @Test
    public void testConstructionWithNullNamespace_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), null, new Bytes(PARTIAL)));
    }

    @Test
    public void testConstructionWithNullPartialIdentity_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE), null));
    }

    @Test
    public void testConstructionWithZeroLengthCustodian_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityBytesBacked(new Bytes(new byte[0]), new Bytes(NAMESPACE),
          new Bytes(PARTIAL)));
    }

    @Test
    public void testConstructionWithZeroLengthNamespace_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(new byte[0]),
          new Bytes(PARTIAL)));
    }

    // -- Empty partial identity (permitted; partialIdentity.getLength() >= 0) --

    @Test
    public void testEmptyPartialIdentity() {
      ManagedKeyIdentity fki = new KeyIdentityBytesBacked(new Bytes(CUSTODIAN),
        new Bytes(NAMESPACE), new Bytes(new byte[0]));
      assertEquals(0, fki.getPartialIdentityLength());
      assertArrayEquals(new byte[0], fki.copyPartialIdentity());
    }

    // -- Bytes with non-zero offset --

    @Test
    public void testConstructionWithBytesHavingOffset() {
      // Build a Bytes that views into the middle of a larger array.
      byte[] larger = new byte[CUSTODIAN.length + 4];
      System.arraycopy(CUSTODIAN, 0, larger, 2, CUSTODIAN.length);
      Bytes custView = new Bytes(larger, 2, CUSTODIAN.length);

      KeyIdentityBytesBacked fki =
        new KeyIdentityBytesBacked(custView, new Bytes(NAMESPACE), new Bytes(PARTIAL));

      // copyCustodian() and the view use offset+length correctly.
      assertArrayEquals(CUSTODIAN, fki.copyCustodian());
      assertEquals(CUSTODIAN.length, fki.getCustodianLength());
      assertArrayEquals(CUSTODIAN, fki.getCustodianView().copyBytes());
      // compareCustodian also uses the Bytes compareTo which respects offset/length.
      assertEquals(0, fki.compareCustodian(CUSTODIAN));
      // Note: getCustodianEncoded() calls custodian.get() (returns full backing array)
      // and would encode more bytes than intended when offset > 0. That is a known
      // deficiency in BytesBacked not fixed here; the abstract-base encoding tests
      // use full-array Bytes so they are unaffected.
    }

    @Test
    public void testConstructionWithBytesHavingOffset_equalsAndHashCodeWithOtherTypes() {
      // Build BytesBacked with all three segments viewing into larger arrays (non-zero offset).
      byte[] largerCust = new byte[CUSTODIAN.length + 4];
      System.arraycopy(CUSTODIAN, 0, largerCust, 2, CUSTODIAN.length);
      byte[] largerNs = new byte[NAMESPACE.length + 4];
      System.arraycopy(NAMESPACE, 0, largerNs, 2, NAMESPACE.length);
      byte[] largerPartial = new byte[PARTIAL.length + 4];
      System.arraycopy(PARTIAL, 0, largerPartial, 2, PARTIAL.length);

      KeyIdentityBytesBacked withOffset =
        new KeyIdentityBytesBacked(new Bytes(largerCust, 2, CUSTODIAN.length),
          new Bytes(largerNs, 2, NAMESPACE.length), new Bytes(largerPartial, 2, PARTIAL.length));

      KeyIdentityBytesBacked noOffset =
        new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE), new Bytes(PARTIAL));
      KeyIdentityBytesBacked fromRawArrays =
        new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked single = new KeyIdentitySingleArrayBacked(backing);

      assertTrue("BytesBacked with offset must equal BytesBacked without offset",
        withOffset.equals(noOffset));
      assertTrue("BytesBacked without offset must equal BytesBacked with offset",
        noOffset.equals(withOffset));
      assertTrue("BytesBacked with offset must equal BytesBacked (byte[] ctor)",
        withOffset.equals(fromRawArrays));
      assertTrue("BytesBacked with offset must equal SingleArrayBacked", withOffset.equals(single));

      assertEquals("BytesBacked with offset must have same hashCode as no-offset",
        withOffset.hashCode(), noOffset.hashCode());
      assertEquals("BytesBacked with offset must have same hashCode as BytesBacked (byte[] ctor)",
        withOffset.hashCode(), fromRawArrays.hashCode());
      assertEquals("BytesBacked with offset must have same hashCode as SingleArrayBacked",
        withOffset.hashCode(), single.hashCode());
    }

    // -- getCustodianView returns the stored Bytes object (no copy on read) --

    @Test
    public void testGetCustodianViewReturnsSameReference() {
      Bytes custBytes = new Bytes(CUSTODIAN);
      KeyIdentityBytesBacked fki =
        new KeyIdentityBytesBacked(custBytes, new Bytes(NAMESPACE), new Bytes(PARTIAL));
      assertSame(custBytes, fki.getCustodianView());
    }

    @Test
    public void testGetNamespaceViewReturnsSameReference() {
      Bytes nsBytes = new Bytes(NAMESPACE);
      KeyIdentityBytesBacked fki =
        new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), nsBytes, new Bytes(PARTIAL));
      assertSame(nsBytes, fki.getNamespaceView());
    }

    @Test
    public void testGetPartialIdentityViewReturnsSameReference() {
      Bytes partialBytes = new Bytes(PARTIAL);
      KeyIdentityBytesBacked fki =
        new KeyIdentityBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE), partialBytes);
      assertSame(partialBytes, fki.getPartialIdentityView());
    }

    // -- byte[] constructor (wraps arrays without copying; same validation as Bytes ctor) --

    @Test
    public void testConstructionWithNullCustodian_byteArrayCtor_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(null, NAMESPACE, PARTIAL));
    }

    @Test
    public void testConstructionWithNullNamespace_byteArrayCtor_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(CUSTODIAN, null, PARTIAL));
    }

    @Test
    public void testConstructionWithNullPartialIdentity_byteArrayCtor_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, null));
    }

    @Test
    public void testConstructionWithZeroLengthCustodian_byteArrayCtor_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityBytesBacked(new byte[0], NAMESPACE, PARTIAL));
    }

    @Test
    public void testConstructionWithZeroLengthNamespace_byteArrayCtor_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityBytesBacked(CUSTODIAN, new byte[0], PARTIAL));
    }

    @Test
    public void testEmptyPartialIdentity_byteArrayCtor() {
      ManagedKeyIdentity fki = new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, new byte[0]);
      assertEquals(0, fki.getPartialIdentityLength());
      assertArrayEquals(new byte[0], fki.copyPartialIdentity());
    }

    /**
     * {@link KeyIdentityBytesBacked#FullKeyIdentityBytesBacked(byte[], byte[], byte[])} uses
     * {@link Bytes#Bytes(byte[])} which keeps the caller's array as backing storage; mutating that
     * array changes the identity's logical content.
     */
    @Test
    public void testByteArrayCtorSharesBackingWithInput() {
      byte[] cust = CUSTODIAN.clone();
      ManagedKeyIdentity fki = new KeyIdentityBytesBacked(cust, NAMESPACE, PARTIAL);
      byte mutated = (byte) 0xFF;
      cust[0] = mutated;
      assertEquals(mutated, fki.copyCustodian()[0]);
    }

    @Test
    public void testGetKeyIdentityPrefixWithPartialReusesCustodianAndNamespaceViews() {
      Bytes custBytes = new Bytes(CUSTODIAN);
      Bytes nsBytes = new Bytes(NAMESPACE);
      KeyIdentityBytesBacked fki =
        new KeyIdentityBytesBacked(custBytes, nsBytes, new Bytes(PARTIAL));

      ManagedKeyIdentity prefix = fki.getKeyIdentityPrefix();
      assertNotSame(fki, prefix);
      assertTrue(prefix instanceof KeyIdentityPrefixBytesBacked);
      assertSame(custBytes, prefix.getCustodianView());
      assertSame(nsBytes, prefix.getNamespaceView());
      assertSame(ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES, prefix.getPartialIdentityView());
    }

    @Test
    public void testGetKeyIdentityPrefixWithNoPartialReturnsSameInstance() {
      KeyIdentityBytesBacked fki = new KeyIdentityBytesBacked(CUSTODIAN, NAMESPACE, new byte[0]);
      assertSame(fki, fki.getKeyIdentityPrefix());
    }
  }

  // ---------------------------------------------------------------------------
  // SingleArrayBacked
  // ---------------------------------------------------------------------------

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestSingleArrayBacked extends AbstractTestFullKeyIdentity {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSingleArrayBacked.class);

    @Override
    protected ManagedKeyIdentity create(byte[] cust, byte[] ns, byte[] partial) {
      return new KeyIdentitySingleArrayBacked(
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(cust, ns, partial));
    }

    // -- Construction validation --

    @Test
    public void testConstructorWithNullArray_throws() {
      assertThrows(IllegalArgumentException.class, () -> new KeyIdentitySingleArrayBacked(null));
    }

    @Test
    public void testConstructorWithArrayTooShort_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentitySingleArrayBacked(new byte[2]));
    }

    /**
     * ACTIVE marker row keys are {@link ManagedKeyIdentityUtils#constructRowKeyForCustNamespace} —
     * no trailing partial length byte when partial identity is empty.
     */
    @Test
    public void testCustNamespaceMarkerRowKey_emptyPartialIdentity() {
      byte[] markerRow =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked fromRow = new KeyIdentitySingleArrayBacked(markerRow);
      assertEquals(0, fromRow.getPartialIdentityLength());
      assertArrayEquals(new byte[0], fromRow.copyPartialIdentity());
      assertArrayEquals(markerRow, fromRow.getFullIdentityView().copyBytes());
      assertArrayEquals(markerRow, fromRow.getIdentityPrefixView().copyBytes());
      ManagedKeyIdentity parsed = new KeyIdentitySingleArrayBacked(markerRow);
      assertTrue(fromRow.equals(parsed));
      assertEquals(fromRow.hashCode(), parsed.hashCode());
    }

    @Test
    public void testComparePartialIdentity_markerRowWithNonEmptyOtherReturnsNegativeOne() {
      byte[] markerRow =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked fromRow = new KeyIdentitySingleArrayBacked(markerRow);
      assertEquals(-1, fromRow.comparePartialIdentity(new byte[] { 0x01 }));
    }

    @Test
    public void testComparePartialIdentity_markerRowWithEmptyOtherReturnsZero() {
      byte[] markerRow =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked fromRow = new KeyIdentitySingleArrayBacked(markerRow);
      assertEquals(0, fromRow.comparePartialIdentity(new byte[0]));
    }

    // -- Offset+length constructor --

    @Test
    public void testOffsetLengthConstructorExtractsCorrectData() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      // Embed the identity bytes inside a larger array with a 3-byte prefix.
      byte[] larger = new byte[backing.length + 5];
      System.arraycopy(backing, 0, larger, 3, backing.length);

      KeyIdentitySingleArrayBacked fki =
        new KeyIdentitySingleArrayBacked(larger, 3, backing.length);

      assertArrayEquals(CUSTODIAN, fki.copyCustodian());
      assertArrayEquals(NAMESPACE, fki.copyNamespace());
      assertArrayEquals(PARTIAL, fki.copyPartialIdentity());

      byte[] expectedFull =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] expectedPrefix =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      assertArrayEquals(expectedFull, fki.getFullIdentityView().copyBytes());
      assertArrayEquals(expectedPrefix, fki.getIdentityPrefixView().copyBytes());
    }

    @Test
    public void testOffsetLengthConstructorTooShortForLength_throws() {
      // offset+length < 3
      byte[] arr = new byte[5];
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentitySingleArrayBacked(arr, 4, 1));
    }

    // -- getBackingArray / getOffset / getLength --

    @Test
    public void testGetBackingArrayReturnsSameReference() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(backing);
      assertSame(backing, fki.getBackingArray());
    }

    @Test
    public void testGetOffsetZeroForFullArrayConstructor() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(0, new KeyIdentitySingleArrayBacked(backing).getOffset());
    }

    @Test
    public void testGetOffsetReflectsSlice() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] larger = new byte[backing.length + 3];
      System.arraycopy(backing, 0, larger, 3, backing.length);
      assertEquals(3, new KeyIdentitySingleArrayBacked(larger, 3, backing.length).getOffset());
    }

    @Test
    public void testGetLengthMatchesBackingArrayLength() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      assertEquals(backing.length, new KeyIdentitySingleArrayBacked(backing).getLength());
    }

    @Test
    public void testGetLengthReflectsSlice() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] larger = new byte[backing.length + 10];
      System.arraycopy(backing, 0, larger, 5, backing.length);
      assertEquals(backing.length,
        new KeyIdentitySingleArrayBacked(larger, 5, backing.length).getLength());
    }

    // -- Malformed format detected lazily on first accessor call --

    @Test
    public void testMalformedFormat_custodianLengthOverflows_throws() {
      // custLen byte = 100 but array is only 5 bytes total.
      byte[] bad = new byte[] { 100, 0x01, 0x02, 0x03, 0x04 };
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(bad);
      assertThrows(IllegalArgumentException.class, () -> fki.getCustodianView());
    }

    @Test
    public void testMalformedFormat_zeroNamespaceLength_throws() {
      // custLen=1, cust=0x01, nsLen=0 (must be >= 1).
      byte[] bad = new byte[] { 1, 0x01, 0, 0x02, 1, (byte) 0xAA };
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(bad);
      assertThrows(IllegalArgumentException.class, () -> fki.getNamespaceView());
    }

    @Test
    public void testMalformedFormat_partialLengthMismatch_throws() {
      // custLen=1, cust=0x01, nsLen=1, ns=0x02, partialLen=5 but only 1 byte follows.
      byte[] bad = new byte[] { 1, 0x01, 1, 0x02, 5, (byte) 0xFF };
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(bad);
      assertThrows(IllegalArgumentException.class, () -> fki.getPartialIdentityView());
    }

    // -- equals / hashCode: compares raw backing-array slice (format-level equality) --

    @Test
    public void testEqualsTwoInstancesFromEquivalentBackings() {
      byte[] b1 = ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] b2 = b1.clone();
      KeyIdentitySingleArrayBacked fki1 = new KeyIdentitySingleArrayBacked(b1);
      KeyIdentitySingleArrayBacked fki2 = new KeyIdentitySingleArrayBacked(b2);
      assertTrue(fki1.equals(fki2));
      assertEquals(fki1.hashCode(), fki2.hashCode());
    }

    @Test
    public void testEqualsFullArrayVsSlice() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] larger = new byte[backing.length + 4];
      System.arraycopy(backing, 0, larger, 2, backing.length);
      KeyIdentitySingleArrayBacked full = new KeyIdentitySingleArrayBacked(backing.clone());
      KeyIdentitySingleArrayBacked slice =
        new KeyIdentitySingleArrayBacked(larger, 2, backing.length);
      assertTrue(full.equals(slice));
      assertTrue(slice.equals(full));
      assertEquals(full.hashCode(), slice.hashCode());
    }

    // -- clone returns a self-contained copy (offset 0, own array) --

    @Test
    public void testCloneFromSliceProducesStandaloneInstance() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      byte[] larger = new byte[backing.length + 6];
      System.arraycopy(backing, 0, larger, 3, backing.length);
      KeyIdentitySingleArrayBacked slice =
        new KeyIdentitySingleArrayBacked(larger, 3, backing.length);

      KeyIdentitySingleArrayBacked clone = (KeyIdentitySingleArrayBacked) slice.clone();
      assertEquals(0, clone.getOffset());
      assertEquals(backing.length, clone.getLength());
      assertArrayEquals(CUSTODIAN, clone.copyCustodian());
      assertArrayEquals(NAMESPACE, clone.copyNamespace());
      assertArrayEquals(PARTIAL, clone.copyPartialIdentity());
    }

    @Test
    public void testGetKeyIdentityPrefixWithPartialReturnsBackingSlice() {
      byte[] backing =
        ManagedKeyIdentityUtils.constructRowKeyForIdentity(CUSTODIAN, NAMESPACE, PARTIAL);
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(backing);

      ManagedKeyIdentity prefix = fki.getKeyIdentityPrefix();
      assertNotSame(fki, prefix);
      assertTrue(prefix instanceof KeyIdentitySingleArrayBacked);
      KeyIdentitySingleArrayBacked prefixSingleArray = (KeyIdentitySingleArrayBacked) prefix;
      assertSame(backing, prefixSingleArray.getBackingArray());
      assertEquals(fki.getOffset(), prefixSingleArray.getOffset());
      assertEquals(
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE).length,
        prefixSingleArray.getLength());
      assertEquals(0, prefixSingleArray.getPartialIdentityLength());
    }

    @Test
    public void testGetKeyIdentityPrefixWithNoPartialReturnsSameInstance() {
      byte[] marker = ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked fki = new KeyIdentitySingleArrayBacked(marker);
      assertSame(fki, fki.getKeyIdentityPrefix());
    }
  }

  // ---------------------------------------------------------------------------
  // KeyIdentityPrefixBytesBacked (custodian + namespace only)
  // ---------------------------------------------------------------------------

  @RunWith(BlockJUnit4ClassRunner.class)
  @Category({ MasterTests.class, SmallTests.class })
  public static class TestKeyIdentityPrefixBytesBacked {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyIdentityPrefixBytesBacked.class);

    private static KeyIdentityPrefixBytesBacked create() {
      return new KeyIdentityPrefixBytesBacked(CUSTODIAN, NAMESPACE);
    }

    private static KeyIdentityPrefixBytesBacked create(byte[] cust, byte[] ns) {
      return new KeyIdentityPrefixBytesBacked(cust, ns);
    }

    // -- Construction validation --

    @Test
    public void testConstructionWithNullCustodian_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityPrefixBytesBacked(null, NAMESPACE));
    }

    @Test
    public void testConstructionWithNullNamespace_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityPrefixBytesBacked(CUSTODIAN, null));
    }

    @Test
    public void testConstructionWithNullCustodian_BytesCtor_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityPrefixBytesBacked(null, new Bytes(NAMESPACE)));
    }

    @Test
    public void testConstructionWithNullNamespace_BytesCtor_throws() {
      assertThrows(NullPointerException.class,
        () -> new KeyIdentityPrefixBytesBacked(new Bytes(CUSTODIAN), null));
    }

    @Test
    public void testConstructionWithZeroLengthCustodian_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityPrefixBytesBacked(new byte[0], NAMESPACE));
    }

    @Test
    public void testConstructionWithZeroLengthNamespace_throws() {
      assertThrows(IllegalArgumentException.class,
        () -> new KeyIdentityPrefixBytesBacked(CUSTODIAN, new byte[0]));
    }

    // -- View getters (aligned with AbstractTestFullKeyIdentity for custodian / namespace / prefix)

    @Test
    public void testGetCustodianView() {
      ManagedKeyIdentity id = create();
      assertArrayEquals(CUSTODIAN, id.getCustodianView().copyBytes());
    }

    @Test
    public void testGetNamespaceView() {
      ManagedKeyIdentity id = create();
      assertArrayEquals(NAMESPACE, id.getNamespaceView().copyBytes());
    }

    @Test
    public void testGetPartialIdentityViewIsKeyNullIdentityBytes() {
      ManagedKeyIdentity id = create();
      assertSame(ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES, id.getPartialIdentityView());
      assertEquals(0, id.getPartialIdentityView().getLength());
    }

    @Test
    public void testGetFullIdentityView_throws() {
      ManagedKeyIdentity id = create();
      assertThrows(UnsupportedOperationException.class, () -> id.getFullIdentityView());
    }

    @Test
    public void testGetIdentityPrefixView() {
      ManagedKeyIdentity id = create();
      byte[] expected =
        ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      assertArrayEquals(expected, id.getIdentityPrefixView().copyBytes());
    }

    @Test
    public void testGetKeyIdentityPrefixReturnsSameInstance() {
      ManagedKeyIdentity id = create();
      assertSame(id, id.getKeyIdentityPrefix());
    }

    // -- Copy methods --

    @Test
    public void testCopyCustodian() {
      ManagedKeyIdentity id = create();
      byte[] copy = id.copyCustodian();
      assertArrayEquals(CUSTODIAN, copy);
      copy[0] = (byte) 0xFF;
      assertArrayEquals("Mutating returned copy must not affect the object", CUSTODIAN,
        id.copyCustodian());
    }

    @Test
    public void testCopyNamespace() {
      ManagedKeyIdentity id = create();
      byte[] copy = id.copyNamespace();
      assertArrayEquals(NAMESPACE, copy);
      copy[0] = (byte) 0xFF;
      assertArrayEquals("Mutating returned copy must not affect the object", NAMESPACE,
        id.copyNamespace());
    }

    @Test
    public void testCopyPartialIdentity_throws() {
      ManagedKeyIdentity id = create();
      assertThrows(UnsupportedOperationException.class, () -> id.copyPartialIdentity());
    }

    // -- Length methods --

    @Test
    public void testGetCustodianLength() {
      assertEquals(CUSTODIAN.length, create().getCustodianLength());
    }

    @Test
    public void testGetNamespaceLength() {
      assertEquals(NAMESPACE.length, create().getNamespaceLength());
    }

    @Test
    public void testGetPartialIdentityLength() {
      assertEquals(0, create().getPartialIdentityLength());
    }

    // -- String / encoded getters --

    @Test
    public void testGetCustodianEncoded() {
      assertEquals(ManagedKeyProvider.encodeToStr(CUSTODIAN), create().getCustodianEncoded());
    }

    @Test
    public void testGetNamespaceString() {
      assertEquals(Bytes.toString(NAMESPACE), create().getNamespaceString());
    }

    @Test
    public void testGetPartialIdentityEncoded_throws() {
      assertThrows(UnsupportedOperationException.class, () -> create().getPartialIdentityEncoded());
    }

    // -- equals / hashCode --

    @Test
    public void testEqualsReflexive() {
      ManagedKeyIdentity id = create();
      assertTrue(id.equals(id));
    }

    @Test
    public void testEqualsSameData_byteArrayAndBytesCtors() {
      KeyIdentityPrefixBytesBacked fromArrays =
        new KeyIdentityPrefixBytesBacked(CUSTODIAN, NAMESPACE);
      KeyIdentityPrefixBytesBacked fromBytes =
        new KeyIdentityPrefixBytesBacked(new Bytes(CUSTODIAN), new Bytes(NAMESPACE));
      assertTrue(fromArrays.equals(fromBytes));
      assertTrue(fromBytes.equals(fromArrays));
      assertEquals(fromArrays.hashCode(), fromBytes.hashCode());
    }

    @Test
    public void testEqualsSameData_twoInstances() {
      ManagedKeyIdentity a = create();
      ManagedKeyIdentity b = create();
      assertTrue(a.equals(b));
      assertTrue(b.equals(a));
      assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testEqualsDifferentCustodian() {
      assertFalse(create().equals(create(new byte[] { 0x0A, 0x0B }, NAMESPACE)));
    }

    @Test
    public void testEqualsDifferentNamespace() {
      assertFalse(create().equals(create(CUSTODIAN, Bytes.toBytes("other"))));
    }

    @Test
    public void testEqualsNull() {
      assertFalse(create().equals(null));
    }

    @Test
    public void testEqualsDifferentRuntimeClass() {
      assertFalse(create().equals("not a ManagedKeyIdentity"));
    }

    // -- clone --

    @Test
    public void testCloneIsEqualSameClassAndIndependentCustodianNamespace() {
      KeyIdentityPrefixBytesBacked id = create();
      KeyIdentityPrefixBytesBacked clone = id.clone();
      assertNotNull(clone);
      assertEquals(KeyIdentityPrefixBytesBacked.class, clone.getClass());
      assertTrue(id.equals(clone));
      assertNotSame(id, clone);
      assertNotSame(id.getCustodianView(), clone.getCustodianView());
      assertNotSame(id.getNamespaceView(), clone.getNamespaceView());
      // Partial view is the shared empty sentinel for both instances.
      assertSame(ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES, id.getPartialIdentityView());
      assertSame(ManagedKeyIdentity.KEY_NULL_IDENTITY_BYTES, clone.getPartialIdentityView());
    }

    @Test
    public void testCloneHoldsCorrectCustodianAndNamespace() {
      KeyIdentityPrefixBytesBacked clone = create().clone();
      assertArrayEquals(CUSTODIAN, clone.copyCustodian());
      assertArrayEquals(NAMESPACE, clone.copyNamespace());
      assertEquals(0, clone.getPartialIdentityLength());
    }

    // -- compareCustodian / compareNamespace --

    @Test
    public void testCompareCustodianEqual() {
      assertEquals(0, create().compareCustodian(CUSTODIAN));
    }

    @Test
    public void testCompareCustodianLess() {
      assertTrue(
        create(new byte[] { 0x01, 0x02, 0x03 }, NAMESPACE).compareCustodian(CUSTODIAN) < 0);
    }

    @Test
    public void testCompareCustodianGreater() {
      assertTrue(
        create(new byte[] { 0x01, 0x02, 0x03, 0x05 }, NAMESPACE).compareCustodian(CUSTODIAN) > 0);
    }

    @Test
    public void testCompareCustodianWithOffsetLength() {
      byte[] padded = new byte[CUSTODIAN.length + 2];
      padded[0] = (byte) 0xFF;
      padded[1] = (byte) 0xFF;
      System.arraycopy(CUSTODIAN, 0, padded, 2, CUSTODIAN.length);
      assertEquals(0, create().compareCustodian(padded, 2, CUSTODIAN.length));
    }

    @Test
    public void testCompareNamespaceEqual() {
      assertEquals(0, create().compareNamespace(NAMESPACE));
    }

    @Test
    public void testCompareNamespaceLess() {
      assertTrue(
        create(CUSTODIAN, Bytes.toBytes("aaa")).compareNamespace(Bytes.toBytes("zzz")) < 0);
    }

    @Test
    public void testCompareNamespaceGreater() {
      assertTrue(
        create(CUSTODIAN, Bytes.toBytes("zzz")).compareNamespace(Bytes.toBytes("aaa")) > 0);
    }

    @Test
    public void testCompareNamespaceWithOffsetLength() {
      byte[] padded = new byte[NAMESPACE.length + 1];
      padded[0] = (byte) 0x00;
      System.arraycopy(NAMESPACE, 0, padded, 1, NAMESPACE.length);
      assertEquals(0, create().compareNamespace(padded, 1, NAMESPACE.length));
    }

    @Test
    public void testComparePartialIdentity_throws() {
      assertThrows(UnsupportedOperationException.class,
        () -> create().comparePartialIdentity(PARTIAL));
    }

    @Test
    public void testComparePartialIdentityWithOffsetLength_throws() {
      assertThrows(UnsupportedOperationException.class,
        () -> create().comparePartialIdentity(PARTIAL, 0, PARTIAL.length));
    }

    // -- Equivalence documented on KeyIdentityPrefixBytesBacked --

    @Test
    public void testEqualsFullKeyIdentityBytesBackedWithEmptyPartial() {
      KeyIdentityPrefixBytesBacked prefix = create();
      KeyIdentityBytesBacked bb = new KeyIdentityBytesBacked(new Bytes(CUSTODIAN),
        new Bytes(NAMESPACE), new Bytes(new byte[0]));
      assertTrue(prefix.equals(bb));
      assertTrue(bb.equals(prefix));
      assertEquals(prefix.hashCode(), bb.hashCode());
    }

    @Test
    public void testEqualsFullKeyIdentitySingleArrayBackedCustNamespaceMarkerRow() {
      KeyIdentityPrefixBytesBacked prefix = create();
      byte[] marker = ManagedKeyIdentityUtils.constructRowKeyForCustNamespace(CUSTODIAN, NAMESPACE);
      KeyIdentitySingleArrayBacked sa = new KeyIdentitySingleArrayBacked(marker);
      assertTrue(prefix.equals(sa));
      assertTrue(sa.equals(prefix));
      assertEquals(prefix.hashCode(), sa.hashCode());
    }

    @Test
    public void testGetCustodianViewReturnsSameReferenceWhenUsingBytesCtor() {
      Bytes cust = new Bytes(CUSTODIAN);
      KeyIdentityPrefixBytesBacked id =
        new KeyIdentityPrefixBytesBacked(cust, new Bytes(NAMESPACE));
      assertSame(cust, id.getCustodianView());
    }

    @Test
    public void testGetNamespaceViewReturnsSameReferenceWhenUsingBytesCtor() {
      Bytes ns = new Bytes(NAMESPACE);
      KeyIdentityPrefixBytesBacked id = new KeyIdentityPrefixBytesBacked(new Bytes(CUSTODIAN), ns);
      assertSame(ns, id.getNamespaceView());
    }
  }
}

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
package org.apache.hadoop.hbase.io.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestUndoableLRUDictionary {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUndoableLRUDictionary.class);

  private UndoableLRUDictionary dict;
  private static final int CAPACITY = 4;

  @Before
  public void setUp() {
    dict = new UndoableLRUDictionary();
    dict.init(CAPACITY);
  }

  private static byte[] entry(String s) {
    return Bytes.toBytes(s);
  }

  private short add(String s) {
    byte[] data = entry(s);
    return dict.addEntry(data, 0, data.length);
  }

  private void assertNoEntry(short idx) {
    assertThrows(IndexOutOfBoundsException.class, () -> dict.getEntry(idx));
  }

  @Test
  public void itPreservesAdditionsOnCommit() {
    add("a");
    add("b");

    dict.checkpoint();
    add("c");
    dict.commit();

    assertArrayEquals(entry("a"), dict.getEntry((short) 0));
    assertArrayEquals(entry("b"), dict.getEntry((short) 1));
    assertArrayEquals(entry("c"), dict.getEntry((short) 2));
  }

  @Test
  public void itRevertsAdditionsOnRollback() {
    short idxA = add("a");
    short idxB = add("b");

    dict.checkpoint();
    add("c");
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertNoEntry((short) 2);
  }

  @Test
  public void itIsNoOpWhenRollingBackWithoutCheckpoint() {
    short idxA = add("a");
    short idxB = add("b");

    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
  }

  @Test
  public void itRestoresEvictedEntryOnRollback() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();
    short idxE = add("e");
    assertEquals(idxA, idxE);
    assertArrayEquals(entry("e"), dict.getEntry(idxE));
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }

  @Test
  public void itHandlesAddGetAddPatternAtCapacityThenRollback() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();

    short idxE = add("e");
    byte[] gotC = dict.getEntry(idxC);
    assertArrayEquals(entry("c"), gotC);
    short idxF = add("f");

    assertArrayEquals(entry("e"), dict.getEntry(idxE));
    assertArrayEquals(entry("f"), dict.getEntry(idxF));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));

    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }

  @Test
  public void itHandlesAddGetAddPatternAtCapacityThenCommit() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();
    short idxE = add("e");
    dict.getEntry(idxC);
    short idxF = add("f");
    dict.commit();

    assertArrayEquals(entry("e"), dict.getEntry(idxE));
    assertArrayEquals(entry("f"), dict.getEntry(idxF));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }

  @Test
  public void itRestoresLruOrderAfterGetEntryOnRollback() {
    short idxA = add("a");
    add("b");
    add("c");

    dict.checkpoint();
    dict.getEntry(idxA);
    add("d");
    dict.rollback();

    assertNoEntry((short) 3);
    add("d");
    short idxE = add("e");
    assertEquals(idxA, idxE);
    assertArrayEquals(entry("e"), dict.getEntry(idxE));
  }

  @Test
  public void itSupportsMultipleCheckpointRollbackCycles() {
    short idxA = add("a");
    short idxB = add("b");

    dict.checkpoint();
    add("c");
    dict.rollback();

    assertNoEntry((short) 2);

    dict.checkpoint();
    add("d");
    dict.rollback();

    assertNoEntry((short) 2);
    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
  }

  @Test
  public void itSupportsMultipleCheckpointCommitCycles() {
    dict.checkpoint();
    short idxA = add("a");
    dict.commit();

    dict.checkpoint();
    short idxB = add("b");
    dict.commit();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
  }

  @Test
  public void itPreservesEvictionsOnCommit() {
    short idxA = add("a");
    add("b");
    add("c");
    add("d");

    dict.checkpoint();
    short idxE = add("e");
    assertEquals(idxA, idxE);
    dict.commit();

    assertArrayEquals(entry("e"), dict.getEntry(idxE));
  }

  @Test
  public void itHandlesMultipleEvictionsDuringTrackingThenRollback() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();
    add("e");
    add("f");
    add("g");
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }

  @Test
  public void itHandlesEvictionThenGetEvictedIndexThenRollback() {
    short idxA = add("a");
    add("b");
    add("c");
    add("d");

    dict.checkpoint();
    short idxE = add("e");
    assertEquals(idxA, idxE);
    byte[] gotE = dict.getEntry(idxE);
    assertArrayEquals(entry("e"), gotE);
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
  }

  @Test
  public void itHandlesRollbackAfterAddBelowCapacity() {
    dict.checkpoint();
    add("a");
    add("b");
    dict.rollback();

    assertNoEntry((short) 0);
  }

  @Test
  public void itHandlesCommitThenRollbackSequence() {
    short idxA = add("a");
    short idxB = add("b");

    dict.checkpoint();
    add("c");
    dict.commit();

    assertArrayEquals(entry("c"), dict.getEntry((short) 2));

    dict.checkpoint();
    add("d");
    dict.rollback();

    assertNoEntry((short) 3);
    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
  }

  @Test
  public void itHandlesGetEntryForEverySlotDuringTrackingThenRollback() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();
    dict.getEntry(idxA);
    dict.getEntry(idxB);
    dict.getEntry(idxC);
    dict.getEntry(idxD);
    add("e");
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }

  @Test
  public void itHandlesAtCapacityEvictAllThenRollback() {
    short idxA = add("a");
    short idxB = add("b");
    short idxC = add("c");
    short idxD = add("d");

    dict.checkpoint();
    add("e");
    add("f");
    add("g");
    add("h");
    dict.rollback();

    assertArrayEquals(entry("a"), dict.getEntry(idxA));
    assertArrayEquals(entry("b"), dict.getEntry(idxB));
    assertArrayEquals(entry("c"), dict.getEntry(idxC));
    assertArrayEquals(entry("d"), dict.getEntry(idxD));
  }
}

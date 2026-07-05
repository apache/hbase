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
package org.apache.hadoop.hbase.security.visibility;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link VisibilityScanDeleteTracker} never reports a delete marker as redundant.
 * <p>
 * HBASE-30036 added {@link DeleteTracker#isRedundantDelete(Cell)} so that minor compaction
 * can drop a delete marker already covered by a previously tracked delete of equal or broader
 * scope. The base {@link org.apache.hadoop.hbase.regionserver.querymatcher.ScanDeleteTracker}
 * implements it purely from delete type / timestamp / qualifier, with no regard to visibility
 * labels.
 * <p>
 * On cell-visibility tables a delete marker only shadows cells whose visibility expression matches
 * (see {@link VisibilityScanDeleteTracker#isDeleted}). Two markers carrying different labels cover
 * disjoint cells, so neither is redundant w.r.t. the other. Reusing the label-blind base logic
 * would wrongly declare such a marker redundant; minor compaction would then drop it and resurrect
 * cells that must stay deleted. {@link VisibilityScanDeleteTracker} must therefore conservatively
 * report no delete as redundant.
 */
@org.junit.jupiter.api.Tag(SecurityTests.TAG)
@org.junit.jupiter.api.Tag(SmallTests.TAG)
public class TestVisibilityScanDeleteTracker {

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  // Placeholder visibility-tag payloads. The exact bytes are irrelevant here: the tracker only
  // needs the tag to be of type VISIBILITY_TAG_TYPE, and the two markers to carry different ones.
  private static final byte[] LABEL_A = new byte[] { 1 };
  private static final byte[] LABEL_B = new byte[] { 2 };

  /**
   * A column-level delete must not be considered redundant just because a same-qualifier
   * DeleteColumn with a higher timestamp was already tracked: if they carry different labels they
   * shadow disjoint cells. The base tracker's {@code coveredByColumn} branch would return true.
   */
  @Test
  public void differentLabelColumnDeleteIsNotRedundant() {
    VisibilityScanDeleteTracker tracker =
      new VisibilityScanDeleteTracker(CellComparatorImpl.COMPARATOR);
    // Tracked first (newest ts, as a compaction scan would present it).
    tracker.add(deleteMarker(QUALIFIER, 100L, KeyValue.Type.DeleteColumn, LABEL_A));

    assertFalse(
      tracker.isRedundantDelete(deleteMarker(QUALIFIER, 50L, KeyValue.Type.DeleteColumn, LABEL_B)));
    assertFalse(
      tracker.isRedundantDelete(deleteMarker(QUALIFIER, 50L, KeyValue.Type.Delete, LABEL_B)));
  }

  /**
   * A family-level delete must not be considered redundant just because a DeleteFamily with a
   * higher timestamp set the family stamp: a label-less family delete only shadows label-less
   * cells, while a labeled one shadows that label's cells. The base tracker's
   * {@code coveredByFamily} branch would return true.
   */
  @Test
  public void differentLabelFamilyDeleteIsNotRedundant() {
    VisibilityScanDeleteTracker tracker =
      new VisibilityScanDeleteTracker(CellComparatorImpl.COMPARATOR);
    // A label-less DeleteFamily is the only thing that advances familyStamp (see add()).
    tracker.add(deleteMarker(null, 100L, KeyValue.Type.DeleteFamily, null));

    assertFalse(
      tracker.isRedundantDelete(deleteMarker(null, 50L, KeyValue.Type.DeleteFamily, LABEL_B)));
    assertFalse(tracker
      .isRedundantDelete(deleteMarker(null, 50L, KeyValue.Type.DeleteFamilyVersion, LABEL_B)));
  }

  /**
   * Even a same-label marker is kept: the base {@code coveredByColumn} branch would (safely) treat
   * a same-qualifier, same-label DeleteColumn of lower timestamp as redundant, but the visibility
   * tracker forgoes that optimization so the label-blind base logic can never run. This pins the
   * deliberately conservative contract against a future change that re-enables it.
   */
  @Test
  public void sameLabelColumnDeleteIsNotRedundant() {
    VisibilityScanDeleteTracker tracker =
      new VisibilityScanDeleteTracker(CellComparatorImpl.COMPARATOR);
    tracker.add(deleteMarker(QUALIFIER, 100L, KeyValue.Type.DeleteColumn, LABEL_A));

    assertFalse(
      tracker.isRedundantDelete(deleteMarker(QUALIFIER, 50L, KeyValue.Type.DeleteColumn, LABEL_A)));
  }

  /**
   * Even where the base answer would actually be correct (a label-less DeleteColumn covered by a
   * label-less one of higher timestamp), the tracker still reports no redundancy, keeping the
   * contract uniform across labeled and label-less markers.
   */
  @Test
  public void labelLessColumnDeleteIsNotRedundant() {
    VisibilityScanDeleteTracker tracker =
      new VisibilityScanDeleteTracker(CellComparatorImpl.COMPARATOR);
    tracker.add(deleteMarker(QUALIFIER, 100L, KeyValue.Type.DeleteColumn, null));

    assertFalse(
      tracker.isRedundantDelete(deleteMarker(QUALIFIER, 50L, KeyValue.Type.DeleteColumn, null)));
  }

  private static Cell deleteMarker(byte[] qualifier, long ts, KeyValue.Type type, byte[] label) {
    List<Tag> tags = label == null
      ? null
      : Collections.singletonList(new ArrayBackedTag(TagType.VISIBILITY_TAG_TYPE, label));
    return new KeyValue(ROW, FAMILY, qualifier, ts, type, null, tags);
  }
}

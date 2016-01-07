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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Multimap;

import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionSplitCalculator {
  private static final Log LOG = LogFactory.getLog(TestRegionSplitCalculator.class);

  /**
   * This is range uses a user specified start and end keys. It also has an
   * extra tiebreaker so that different ranges with the same start/end key pair
   * count as different regions.
   */
  static class SimpleRange implements KeyRange {
    byte[] start, end;
    UUID tiebreaker;

    SimpleRange(byte[] start, byte[] end) {
      this.start = start;
      this.end = end;
      this.tiebreaker = UUID.randomUUID();
    }

    @Override
    public byte[] getStartKey() {
      return start;
    }

    @Override
    public byte[] getEndKey() {
      return end;
    }

    public String toString() {
      return "[" + Bytes.toString(start) + ", " + Bytes.toString(end) + "]";
    }
  }

  Comparator<SimpleRange> cmp = new Comparator<SimpleRange>() {
    @Override
    public int compare(SimpleRange sr1, SimpleRange sr2) {
      ComparisonChain cc = ComparisonChain.start();
      cc = cc.compare(sr1.getStartKey(), sr2.getStartKey(),
          Bytes.BYTES_COMPARATOR);
      cc = cc.compare(sr1.getEndKey(), sr2.getEndKey(),
          RegionSplitCalculator.BYTES_COMPARATOR);
      cc = cc.compare(sr1.tiebreaker, sr2.tiebreaker);
      return cc.result();
    }
  };

  /**
   * Check the "depth" (number of regions included at a split) of a generated
   * split calculation
   */
  void checkDepths(SortedSet<byte[]> splits,
      Multimap<byte[], SimpleRange> regions, Integer... depths) {
    assertEquals(splits.size(), depths.length);
    int i = 0;
    for (byte[] k : splits) {
      Collection<SimpleRange> rs = regions.get(k);
      int sz = rs == null ? 0 : rs.size();
      assertEquals((int) depths[i], sz);
      i++;
    }
  }

  /**
   * This dumps data in a visually reasonable way for visual debugging. It has
   * the basic iteration structure.
   */
  String dump(SortedSet<byte[]> splits, Multimap<byte[], SimpleRange> regions) {
    // we display this way because the last end key should be displayed as well.
    StringBuilder sb = new StringBuilder();
    for (byte[] k : splits) {
      sb.append(Bytes.toString(k)).append(":\t");
      for (SimpleRange r : regions.get(k)) {
        sb.append(r.toString()).append("\t");
      }
      sb.append("\n");
    }
    String s = sb.toString();
    LOG.info("\n" + s);
    return s;
  }

  @Test
  public void testSplitCalculator() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C"));
    SimpleRange c = new SimpleRange(Bytes.toBytes("C"), Bytes.toBytes("D"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);
    sc.add(c);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Standard");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 1, 1, 0);
    assertEquals(res, "A:\t[A, B]\t\n" + "B:\t[B, C]\t\n" + "C:\t[C, D]\t\n"
        + "D:\t\n");
  }

  @Test
  public void testSplitCalculatorNoEdge() {
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Empty");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions);
    assertEquals("", res);
  }

  @Test
  public void testSplitCalculatorSingleEdge() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Single edge");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 0);
    assertEquals("A:\t[A, B]\t\n" + "B:\t\n", res);
  }

  @Test
  public void testSplitCalculatorDegenerateEdge() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("A"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Single empty edge");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1);
    assertEquals("A:\t[A, A]\t\n", res);
  }

  @Test
  public void testSplitCalculatorCoverSplit() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C"));
    SimpleRange c = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);
    sc.add(c);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AC covers AB, BC");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 2, 2, 0);
    assertEquals("A:\t[A, B]\t[A, C]\t\n" + "B:\t[A, C]\t[B, C]\t\n"
        + "C:\t\n", res);
  }

  @Test
  public void testSplitCalculatorOverEndpoint() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C"));
    SimpleRange c = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("D"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);
    sc.add(c);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AB, BD covers BC");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 2, 1, 0);
    assertEquals("A:\t[A, B]\t\n" + "B:\t[B, C]\t[B, D]\t\n"
        + "C:\t[B, D]\t\n" + "D:\t\n", res);
  }

  @Test
  public void testSplitCalculatorHoles() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C"));
    SimpleRange c = new SimpleRange(Bytes.toBytes("E"), Bytes.toBytes("F"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);
    sc.add(c);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Hole between C and E");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 1, 0, 1, 0);
    assertEquals("A:\t[A, B]\t\n" + "B:\t[B, C]\t\n" + "C:\t\n"
        + "E:\t[E, F]\t\n" + "F:\t\n", res);
  }

  @Test
  public void testSplitCalculatorOverreach() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("D"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AC and BD overlap but share no start/end keys");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 2, 1, 0);
    assertEquals("A:\t[A, C]\t\n" + "B:\t[A, C]\t[B, D]\t\n"
        + "C:\t[B, D]\t\n" + "D:\t\n", res);
  }

  @Test
  public void testSplitCalculatorFloor() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AC and AB overlap in the beginning");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 2, 1, 0);
    assertEquals("A:\t[A, B]\t[A, C]\t\n" + "B:\t[A, C]\t\n" + "C:\t\n", res);
  }

  @Test
  public void testSplitCalculatorCeil() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AC and BC overlap in the end");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 2, 0);
    assertEquals("A:\t[A, C]\t\n" + "B:\t[A, C]\t[B, C]\t\n" + "C:\t\n", res);
  }

  @Test
  public void testSplitCalculatorEq() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));
    SimpleRange b = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));

    LOG.info(a.tiebreaker + " - " + b.tiebreaker);
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);
    sc.add(b);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("AC and AC overlap completely");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 2, 0);
    assertEquals("A:\t[A, C]\t[A, C]\t\n" + "C:\t\n", res);
  }

  @Test
  public void testSplitCalculatorBackwards() {
    SimpleRange a = new SimpleRange(Bytes.toBytes("C"), Bytes.toBytes("A"));
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(a);

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("CA is backwards");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions); // expect nothing
    assertEquals("", res);
  }

  @Test
  public void testComplex() {
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("Am")));
    sc.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C")));
    sc.add(new SimpleRange(Bytes.toBytes("Am"), Bytes.toBytes("C")));
    sc.add(new SimpleRange(Bytes.toBytes("D"), Bytes.toBytes("E")));
    sc.add(new SimpleRange(Bytes.toBytes("F"), Bytes.toBytes("G")));
    sc.add(new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("E")));
    sc.add(new SimpleRange(Bytes.toBytes("H"), Bytes.toBytes("I")));
    sc.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B")));

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Something fairly complex");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 3, 3, 3, 1, 2, 0, 1, 0, 1, 0);
    assertEquals("A:\t[A, Am]\t[A, B]\t[A, C]\t\n"
        + "Am:\t[A, B]\t[A, C]\t[Am, C]\t\n"
        + "B:\t[A, C]\t[Am, C]\t[B, E]\t\n" + "C:\t[B, E]\t\n"
        + "D:\t[B, E]\t[D, E]\t\n" + "E:\t\n" + "F:\t[F, G]\t\n" + "G:\t\n"
        + "H:\t[H, I]\t\n" + "I:\t\n", res);
  }

  @Test
  public void testBeginEndMarker() {
    RegionSplitCalculator<SimpleRange> sc = new RegionSplitCalculator<SimpleRange>(
        cmp);
    sc.add(new SimpleRange(Bytes.toBytes(""), Bytes.toBytes("A")));
    sc.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B")));
    sc.add(new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("")));

    Multimap<byte[], SimpleRange> regions = sc.calcCoverage();
    LOG.info("Special cases -- empty");
    String res = dump(sc.getSplits(), regions);
    checkDepths(sc.getSplits(), regions, 1, 1, 1, 0);
    assertEquals(":\t[, A]\t\n" + "A:\t[A, B]\t\n" + "B:\t[B, ]\t\n"
        + "null:\t\n", res);
  }

  @Test
  public void testBigRanges() {
    SimpleRange ai = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("I"));
    SimpleRange ae = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("E"));
    SimpleRange ac = new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C"));

    Collection<SimpleRange> bigOverlap = new ArrayList<SimpleRange>();
    bigOverlap.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("E")));
    bigOverlap.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("C")));
    bigOverlap.add(new SimpleRange(Bytes.toBytes("A"), Bytes.toBytes("B")));
    bigOverlap.add(new SimpleRange(Bytes.toBytes("B"), Bytes.toBytes("C")));
    bigOverlap.add(new SimpleRange(Bytes.toBytes("E"), Bytes.toBytes("H")));
    bigOverlap.add(ai);
    bigOverlap.add(ae);
    bigOverlap.add(ac);

    // Expect 1 range to be returned: ai
    List<SimpleRange> bigRanges = RegionSplitCalculator.findBigRanges(bigOverlap, 1);
    assertEquals(1, bigRanges.size());
    assertEquals(ai, bigRanges.get(0));

    // Expect 3 ranges to be returned: ai, ae and ac
    bigRanges = RegionSplitCalculator.findBigRanges(bigOverlap, 3);
    assertEquals(3, bigRanges.size());
    assertEquals(ai, bigRanges.get(0));

    SimpleRange r1 = bigRanges.get(1);
    SimpleRange r2 = bigRanges.get(2);
    assertEquals("A", Bytes.toString(r1.start));
    assertEquals("A", Bytes.toString(r2.start));
    String r1e = Bytes.toString(r1.end);
    String r2e = Bytes.toString(r2.end);
    assertTrue((r1e.equals("C") && r2e.equals("E"))
      || (r1e.equals("E") && r2e.equals("C")));
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}


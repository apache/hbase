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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ FilterTests.class, SmallTests.class })
public class TestFuzzyRowFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFuzzyRowFilter.class);

  @Test
  public void testSatisfiesNoUnsafeForward() {

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
      FuzzyRowFilter.satisfiesNoUnsafe(false, new byte[] { 1, (byte) -128, 1, 0, 1 }, 0, 5,
        new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(false, new byte[] { 1, (byte) -128, 2, 0, 1 }, 0, 5,
        new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfiesNoUnsafe(false,
      new byte[] { 1, 2, 1, 3, 3 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(false, new byte[] { 1, 1, 1, 3, 0 }, // row to check
        0, 5, new byte[] { 1, 2, 0, 3 }, // fuzzy row
        new byte[] { 0, 0, 1, 0 })); // mask

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(false, new byte[] { 1, 1, 1, 3, 0 }, 0, 5,
        new byte[] { 1, (byte) 245, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfiesNoUnsafe(
      false, new byte[] { 1, 2, 1, 0, 1 }, 0, 5, new byte[] { 0, 1, 2 }, new byte[] { 1, 0, 0 }));
  }

  @Test
  public void testSatisfiesForward() {

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfies(false,
      new byte[] { 1, (byte) -128, 1, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(false,
      new byte[] { 1, (byte) -128, 2, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfies(false,
      new byte[] { 1, 2, 1, 3, 3 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfies(false, new byte[] { 1, 1, 1, 3, 0 }, // row to check
        new byte[] { 1, 2, 0, 3 }, // fuzzy row
        new byte[] { -1, -1, 0, -1 })); // mask

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfies(false, new byte[] { 1, 1, 1, 3, 0 },
        new byte[] { 1, (byte) 245, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(false,
      new byte[] { 1, 2, 1, 0, 1 }, new byte[] { 0, 1, 2 }, new byte[] { 0, -1, -1 }));
  }

  @Test
  public void testSatisfiesReverse() {
    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfies(true,
      new byte[] { 1, (byte) -128, 1, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(true,
      new byte[] { 1, (byte) -128, 2, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(true,
      new byte[] { 2, 3, 1, 1, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfies(true,
      new byte[] { 1, 2, 1, 3, 3 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfies(true, new byte[] { 1, (byte) 245, 1, 3, 0 },
        new byte[] { 1, 1, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(true,
      new byte[] { 1, 3, 1, 3, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(true,
      new byte[] { 2, 1, 1, 1, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfies(true,
      new byte[] { 1, 2, 1, 0, 1 }, new byte[] { 0, 1, 2 }, new byte[] { 0, -1, -1 }));
  }

  @Test
  public void testSatisfiesNoUnsafeReverse() {
    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
      FuzzyRowFilter.satisfiesNoUnsafe(true, new byte[] { 1, (byte) -128, 1, 0, 1 }, 0, 5,
        new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(true, new byte[] { 1, (byte) -128, 2, 0, 1 }, 0, 5,
        new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfiesNoUnsafe(
      true, new byte[] { 2, 3, 1, 1, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES, FuzzyRowFilter.satisfiesNoUnsafe(true,
      new byte[] { 1, 2, 1, 3, 3 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(true, new byte[] { 1, (byte) 245, 1, 3, 0 }, 0, 5,
        new byte[] { 1, 1, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(true, new byte[] { 1, 3, 1, 3, 0 }, 0, 5,
        new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
      FuzzyRowFilter.satisfiesNoUnsafe(true, new byte[] { 2, 1, 1, 1, 0 }, 0, 5,
        new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilter.satisfiesNoUnsafe(
      true, new byte[] { 1, 2, 1, 0, 1 }, 0, 5, new byte[] { 0, 1, 2 }, new byte[] { 1, 0, 0 }));
  }

  @Test
  public void testGetNextForFuzzyRuleForward() {
    assertNext(false, new byte[] { 0, 1, 2 }, // fuzzy row
      new byte[] { 0, -1, -1 }, // mask
      new byte[] { 1, 2, 1, 0, 1 }, // current
      new byte[] { 2, 1, 2 }); // expected next

    assertNext(false, new byte[] { 0, 1, 2 }, // fuzzy row
      new byte[] { 0, -1, -1 }, // mask
      new byte[] { 1, 1, 2, 0, 1 }, // current
      new byte[] { 1, 1, 2, 0, 2 }); // expected next

    assertNext(false, new byte[] { 0, 1, 0, 2, 0 }, // fuzzy row
      new byte[] { 0, -1, 0, -1, 0 }, // mask
      new byte[] { 1, 0, 2, 0, 1 }, // current
      new byte[] { 1, 1, 0, 2 }); // expected next

    assertNext(false, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 },
      new byte[] { 1, (byte) 128, 2, 0, 1 }, new byte[] { 1, (byte) 129, 1 });

    assertNext(false, new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 },
      new byte[] { 5, 1, 0, 1 }, new byte[] { 5, 1, 1, 1 });

    assertNext(false, new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 },
      new byte[] { 5, 1, 0, 1, 1 }, new byte[] { 5, 1, 0, 1, 2 });

    assertNext(false, new byte[] { 0, 1, 0, 0 }, // fuzzy row
      new byte[] { 0, -1, 0, 0 }, // mask
      new byte[] { 5, 1, (byte) 255, 1 }, // current
      new byte[] { 5, 1, (byte) 255, 2 }); // expected next

    assertNext(false, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, (byte) 255, 1 }, // current
      new byte[] { 6, 1, 0, 1 }); // expected next

    assertNext(false, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, (byte) 255, 0 }, // current
      new byte[] { 5, 1, (byte) 255, 1 }); // expected next

    assertNext(false, new byte[] { 5, 1, 1, 0 }, new byte[] { -1, -1, 0, 0 },
      new byte[] { 5, 1, (byte) 255, 1 }, new byte[] { 5, 1, (byte) 255, 2 });

    assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { -1, -1, 0, 0 },
      new byte[] { 1, 1, 2, 2 }, new byte[] { 1, 1, 2, 3 });

    assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { -1, -1, 0, 0 },
      new byte[] { 1, 1, 3, 2 }, new byte[] { 1, 1, 3, 3 });

    assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { 0, 0, 0, 0 },
      new byte[] { 1, 1, 2, 3 }, new byte[] { 1, 1, 2, 4 });

    assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { 0, 0, 0, 0 },
      new byte[] { 1, 1, 3, 2 }, new byte[] { 1, 1, 3, 3 });

    assertNext(false, new byte[] { 1, 1, 0, 0 }, new byte[] { -1, -1, 0, 0 },
      new byte[] { 0, 1, 3, 2 }, new byte[] { 1, 1 });

    // No next for this one
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(new byte[] { 2, 3, 1, 1, 1 }, // row to
                                                                                       // check
      new byte[] { 1, 0, 1 }, // fuzzy row
      new byte[] { -1, 0, -1 })); // mask
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(new byte[] { 1, (byte) 245, 1, 3, 0 },
      new byte[] { 1, 1, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(new byte[] { 1, 3, 1, 3, 0 },
      new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(new byte[] { 2, 1, 1, 1, 0 },
      new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
  }

  @Test
  public void testGetNextForFuzzyRuleReverse() {
    // ?12
    // 12101
    // 112 -> 113
    assertNext(true, new byte[] { 0, 1, 2 }, // fuzzy row
      new byte[] { 0, -1, -1 }, // mask
      new byte[] { 1, 2, 1, 0, 1 }, // current
      new byte[] { 1, 1, 3 }); // expected next

    // ?1?2?
    // 12131
    // 11(0xFF)2 -> 11(0xFF)3
    assertNext(true, new byte[] { 0, 1, 0, 2, 0 }, // fuzzy row
      new byte[] { 0, -1, 0, -1, 0 }, // mask
      new byte[] { 1, 2, 1, 3, 1 }, // current
      new byte[] { 1, 1, (byte) 255, 3 }); // expected next

    // 1?1
    // 1(128)201
    // 1(128)1 -> 1(128)2
    assertNext(true, new byte[] { 1, 0, 1 }, // fuzzy row
      new byte[] { -1, 0, -1 }, // mask
      new byte[] { 1, (byte) 128, 2, 0, 1 }, // current
      new byte[] { 1, (byte) 128, 2 }); // expected next

    // ?1?1
    // 51021
    // 5101 -> 5102
    assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, 0, 2, 1 }, // current
      new byte[] { 5, 1, 0, 2 }); // expected next

    // ?1??
    // 51(255)1
    // 51(255)0 -> 51(255)1
    assertNext(true, new byte[] { 0, 1, 0, 0 }, // fuzzy row
      new byte[] { 0, -1, 0, 0 }, // mask
      new byte[] { 5, 1, (byte) 255, 1 }, // current
      new byte[] { 5, 1, (byte) 255, 1 }); // expected next

    // ?1?1
    // 5101
    // 41(255)1 -> 41(255)2
    assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, 0, 1 }, // current
      new byte[] { 4, 1, (byte) 255, 2 }); // expected next

    // ?1?1
    // 51(255)0
    // 51(254)1 -> 51(254)2
    assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, (byte) 255, 0 }, // current
      new byte[] { 5, 1, (byte) 254, 2 }); // expected next

    // 11??
    // 2132
    // 11(0xFF)(0xFF) -> 12
    assertNext(true, new byte[] { 1, 1, 0, 0 }, // fuzzy row
      new byte[] { -1, -1, 0, 0 }, // mask
      new byte[] { 2, 1, 3, 2 }, // current
      new byte[] { 1, 2 }); // expected next

    // 1?1
    // 23111
    // 1(0xFF)1 -> 1(0xFF)2
    assertNext(true, new byte[] { 1, 0, 1 }, // fuzzy row
      new byte[] { -1, 0, -1 }, // mask
      new byte[] { 2, 3, 1, 1, 1 }, // row to check
      new byte[] { 1, (byte) 255, 2 }); // expected next

    // 11?3
    // 1(245)130
    // 11(0xFF)3 -> 11(0xFF)4
    assertNext(true, new byte[] { 1, 1, 0, 3 }, // fuzzy row
      new byte[] { -1, -1, 0, -1 }, // mask
      new byte[] { 1, (byte) 245, 1, 3, 0 }, // row to check
      new byte[] { 1, 1, (byte) 255, 4 }); // expected next

    // 12?3
    // 13130
    // 12(0xFF)3 -> 12(0xFF)4
    assertNext(true, new byte[] { 1, 2, 0, 3 }, // fuzzy row
      new byte[] { -1, -1, 0, -1 }, // mask
      new byte[] { 1, 3, 1, 3, 0 }, // row to check
      new byte[] { 1, 2, (byte) 255, 4 }); // expected next

    // 12?3
    // 21110
    // 12(0xFF)3 -> 12(0xFF)4
    assertNext(true, new byte[] { 1, 2, 0, 3 }, // fuzzy row
      new byte[] { -1, -1, 0, -1 }, // mask
      new byte[] { 2, 1, 1, 1, 0 }, // row to check
      new byte[] { 1, 2, (byte) 255, 4 }); // expected next

    // 1?1
    // 1(128)2
    // 1(128)1 -> 1(128)2
    assertNext(true, new byte[] { 1, 0, 1 }, // fuzzy row
      new byte[] { -1, 0, -1 }, // mask
      new byte[] { 1, (byte) 128, 2 }, // row to check
      new byte[] { 1, (byte) 128, 2 }); // expected next

    // ?1?1
    // 5102
    // 5101 -> 5102 or null?
    assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
      new byte[] { 0, -1, 0, -1 }, // mask
      new byte[] { 5, 1, 0, 2 }, // row to check
      new byte[] { 5, 1, 0, 2 }); // expected next

    // 51??
    // 51(0xFF)1
    // 51(0xFF)0 -> 51(0xFF)1
    assertNext(true, new byte[] { 5, 1, 1, 0 }, // fuzzy row
      new byte[] { -1, -1, 0, 0 }, // mask
      new byte[] { 5, 1, (byte) 0xFF, 1 }, // row to check
      new byte[] { 5, 1, (byte) 0xFF, 1 }); // expected next

    // 11??
    // 1122
    // 1121 -> 1122
    assertNext(true, new byte[] { 1, 1, 1, 1 }, // fuzzy row
      new byte[] { -1, -1, 0, 0 }, // mask
      new byte[] { 1, 1, 2, 2 }, // row to check
      new byte[] { 1, 1, 2, 2 }); // expected next

    // ????
    // 1123
    // 1122 -> 1123
    assertNext(true, new byte[] { 1, 1, 1, 1 }, // fuzzy row
      new byte[] { 0, 0, 0, 0 }, // mask
      new byte[] { 1, 1, 2, 3 }, // row to check
      new byte[] { 1, 1, 2, 3 }); // expected next

    // 12?3
    // 11130
    // no before cell than current which satisfies the fuzzy row -> null
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(true, new byte[] { 1, 1, 1, 3, 0 },
      new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
  }

  private static void assertNext(boolean reverse, byte[] fuzzyRow, byte[] mask, byte[] current,
    byte[] expected) {
    KeyValue kv = KeyValueUtil.createFirstOnRow(current);
    byte[] nextForFuzzyRule = FuzzyRowFilter.getNextForFuzzyRule(reverse, kv.getRowArray(),
      kv.getRowOffset(), kv.getRowLength(), fuzzyRow, mask);
    Assert.assertEquals(Bytes.toStringBinary(expected), Bytes.toStringBinary(nextForFuzzyRule));
  }
}

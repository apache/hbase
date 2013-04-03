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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFuzzyRowFilter {
  @Test
  public void testSatisfies() {
    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
            FuzzyRowFilter.satisfies(new byte[]{1, (byte) -128, 0, 0, 1}, // row to check
                                     new byte[]{1, 0, 1}, // fuzzy row
                                     new byte[]{0, 1, 0})); // mask

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
            FuzzyRowFilter.satisfies(new byte[]{1, (byte) -128, 1, 0, 1},
                                     new byte[]{1, 0, 1},
                                     new byte[]{0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
            FuzzyRowFilter.satisfies(new byte[]{1, (byte) -128, 2, 0, 1},
                                     new byte[]{1, 0, 1},
                                     new byte[]{0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
            FuzzyRowFilter.satisfies(new byte[]{2, 3, 1, 1, 1},
                                     new byte[]{1, 0, 1},
                                     new byte[]{0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.YES,
            FuzzyRowFilter.satisfies(new byte[]{1, 2, 1, 3, 3},
                                     new byte[]{1, 2, 0, 3},
                                     new byte[]{0, 0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
            FuzzyRowFilter.satisfies(new byte[]{1, 1, 1, 3, 0}, // row to check
                                     new byte[]{1, 2, 0, 3}, // fuzzy row
                                     new byte[]{0, 0, 1, 0})); // mask

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
            FuzzyRowFilter.satisfies(new byte[]{1, 1, 1, 3, 0},
                                     new byte[]{1, (byte) 245, 0, 3},
                                     new byte[]{0, 0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
            FuzzyRowFilter.satisfies(new byte[]{1, (byte) 245, 1, 3, 0},
                                     new byte[]{1, 1, 0, 3},
                                     new byte[]{0, 0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
            FuzzyRowFilter.satisfies(new byte[]{1, 3, 1, 3, 0},
                                     new byte[]{1, 2, 0, 3},
                                     new byte[]{0, 0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NO_NEXT,
            FuzzyRowFilter.satisfies(new byte[]{2, 1, 1, 1, 0},
                                     new byte[]{1, 2, 0, 3},
                                     new byte[]{0, 0, 1, 0}));

    Assert.assertEquals(FuzzyRowFilter.SatisfiesCode.NEXT_EXISTS,
            FuzzyRowFilter.satisfies(new byte[]{1, 2, 1, 0, 1},
                                     new byte[]{0, 1, 2},
                                     new byte[]{1, 0, 0}));
  }

  @Test
  public void testGetNextForFuzzyRule() {
    assertNext(
            new byte[]{0, 1, 2}, // fuzzy row
            new byte[]{1, 0, 0}, // mask
            new byte[]{1, 2, 1, 0, 1}, // current
            new byte[]{2, 1, 2, 0, 0}); // expected next

    assertNext(
            new byte[]{0, 1, 2}, // fuzzy row
            new byte[]{1, 0, 0}, // mask
            new byte[]{1, 1, 2, 0, 1}, // current
            new byte[]{1, 1, 2, 0, 2}); // expected next

    assertNext(
            new byte[]{0, 1, 0, 2, 0}, // fuzzy row
            new byte[]{1, 0, 1, 0, 1}, // mask
            new byte[]{1, 0, 2, 0, 1}, // current
            new byte[]{1, 1, 0, 2, 0}); // expected next

    assertNext(
            new byte[]{1, 0, 1},
            new byte[]{0, 1, 0},
            new byte[]{1, (byte) 128, 2, 0, 1},
            new byte[]{1, (byte) 129, 1, 0, 0});

    assertNext(
            new byte[]{0, 1, 0, 1},
            new byte[]{1, 0, 1, 0},
            new byte[]{5, 1, 0, 1},
            new byte[]{5, 1, 1, 1});

    assertNext(
            new byte[]{0, 1, 0, 1},
            new byte[]{1, 0, 1, 0},
            new byte[]{5, 1, 0, 1, 1},
            new byte[]{5, 1, 0, 1, 2});

    assertNext(
            new byte[]{0, 1, 0, 0}, // fuzzy row
            new byte[]{1, 0, 1, 1}, // mask
            new byte[]{5, 1, (byte) 255, 1}, // current
            new byte[]{5, 1, (byte) 255, 2}); // expected next

    assertNext(
            new byte[]{0, 1, 0, 1}, // fuzzy row
            new byte[]{1, 0, 1, 0}, // mask
            new byte[]{5, 1, (byte) 255, 1}, // current
            new byte[]{6, 1, 0, 1}); // expected next

    assertNext(
            new byte[]{0, 1, 0, 1}, // fuzzy row
            new byte[]{1, 0, 1, 0}, // mask
            new byte[]{5, 1, (byte) 255, 0}, // current
            new byte[]{5, 1, (byte) 255, 1}); // expected next

    assertNext(
            new byte[]{5, 1, 1, 0},
            new byte[]{0, 0, 1, 1},
            new byte[]{5, 1, (byte) 255, 1},
            new byte[]{5, 1, (byte) 255, 2});

    assertNext(
            new byte[]{1, 1, 1, 1},
            new byte[]{0, 0, 1, 1},
            new byte[]{1, 1, 2, 2},
            new byte[]{1, 1, 2, 3});

    assertNext(
            new byte[]{1, 1, 1, 1},
            new byte[]{0, 0, 1, 1},
            new byte[]{1, 1, 3, 2},
            new byte[]{1, 1, 3, 3});

    assertNext(
            new byte[]{1, 1, 1, 1},
            new byte[]{1, 1, 1, 1},
            new byte[]{1, 1, 2, 3},
            new byte[]{1, 1, 2, 4});

    assertNext(
            new byte[]{1, 1, 1, 1},
            new byte[]{1, 1, 1, 1},
            new byte[]{1, 1, 3, 2},
            new byte[]{1, 1, 3, 3});

    assertNext(
            new byte[]{1, 1, 0, 0},
            new byte[]{0, 0, 1, 1},
            new byte[]{0, 1, 3, 2},
            new byte[]{1, 1, 0, 0});

    // No next for this one
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(
            new byte[]{2, 3, 1, 1, 1}, // row to check
            new byte[]{1, 0, 1}, // fuzzy row
            new byte[]{0, 1, 0})); // mask
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(
            new byte[]{1, (byte) 245, 1, 3, 0},
            new byte[]{1, 1, 0, 3},
            new byte[]{0, 0, 1, 0}));
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(
            new byte[]{1, 3, 1, 3, 0},
            new byte[]{1, 2, 0, 3},
            new byte[]{0, 0, 1, 0}));
    Assert.assertNull(FuzzyRowFilter.getNextForFuzzyRule(
            new byte[]{2, 1, 1, 1, 0},
            new byte[]{1, 2, 0, 3},
            new byte[]{0, 0, 1, 0}));
  }

  private void assertNext(byte[] fuzzyRow, byte[] mask, byte[] current, byte[] expected) {
    byte[] nextForFuzzyRule = FuzzyRowFilter.getNextForFuzzyRule(current, fuzzyRow, mask);
    Assert.assertArrayEquals(expected, nextForFuzzyRule);
  }

}

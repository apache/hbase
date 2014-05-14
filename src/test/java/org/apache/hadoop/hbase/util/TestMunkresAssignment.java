/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.MunkresAssignment;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link MunkresAssignment} which solves the assignment problem.
 */
@Category(SmallTests.class)
public class TestMunkresAssignment {

  /**
   * Verify that a given assignment result is correct for a given cost matrix.
   * @param cost the cost matrix input to the algorithm being tested
   * @param assignment the result from the algorithm being tested
   * @param expectedTotalCost the expected total cost for the input
   */
  private void verify(float[][] cost, int[] assignment,
      float expectedTotalCost) {
    // The result matrix must have the same number of rows as the cost matrix.
    assertTrue(assignment.length == cost.length);

    int assignmentCount = 0;
    float actualTotalCost = 0;
    for (int r = 0; r < assignment.length; r++) {
      // Each entry in the assignment vector must be in the range of indexes of
      // columns or -1 indicating no assignment.
      assertTrue(assignment[r] < cost[r].length && assignment[r] >= -1);
      if (assignment[r] != -1) {
        actualTotalCost += cost[r][assignment[r]];
        assignmentCount++;
      }
    }

    assertTrue(assignmentCount == Math.min(cost.length, cost[0].length));

    // Assignments must be unique, i.e. no two rows may be assigned to the same
    // column.
    Arrays.sort(assignment);
    for (int i = 0; i < assignment.length - 1; i++) {
      if (assignment[i] > -1) {
        assertTrue(assignment[i] < assignment[i + 1]);
      }
    }

    // The total cost of the computed assignment must be the same as expected.
    // Floating-point arithmetic may have some error, so compare to epsilon.
    // Need both clauses because cost could be infinite, which would not satisfy
    // the second clause.
    assertTrue(actualTotalCost == expectedTotalCost ||
        Math.abs(actualTotalCost - expectedTotalCost) < 0.00001);
  }

  /**
   * Make a deep copy of the given cost matrix in case the algorithm modifies
   * its input arguments.
   * @param cost the input cost matrix
   * @return an identical deep copy of the cost matrix
   */
  private float[][] copy(float[][] cost) {
    float[][] copy = new float[cost.length][cost[0].length];
    for (int r = 0; r < cost.length; r++) {
      for (int c = 0; c < cost[0].length; c++) {
        copy[r][c] = cost[r][c];
      }
    }
    return copy;
  }

  private void doTest(float[][] cost, float expectedTotalCost) {
    verify(cost, new MunkresAssignment(copy(cost)).solve(), expectedTotalCost);
  }

  @Test (timeout=100)
  public void testTrivial() throws Exception {
    doTest(new float[][]{
        {0, 0, 0},
        {0, 0, 0},
        {0, 0, 0}}, 0);
  }

  @Test (timeout=100)
  public void testUniform() throws Exception {
    doTest(new float[][]{
        {4.4f, 4.4f, 4.4f},
        {4.4f, 4.4f, 4.4f},
        {4.4f, 4.4f, 4.4f}}, 13.2f);
  }

  @Test (timeout=100)
  public void testMultiplicative() throws Exception {
    doTest(new float[][]{
        {1, 2, 3},
        {2, 4, 6},
        {3, 6, 9}}, 10);
  }

  @Test (timeout=100)
  public void testMoreRows() throws Exception {
    doTest(new float[][]{
        {1, 2, 3},
        {2, 4, 6},
        {4, 8, 12},
        {3, 6, 9}}, 10);
  }

  @Test (timeout=100)
  public void testMoreColumns() throws Exception {
    doTest(new float[][]{
        {1, 2, 4, 3},
        {2, 4, 8, 6},
        {3, 6, 12, 9}}, 10);
  }

  @Test (timeout=100)
  public void testMedium() throws Exception {
    doTest(new float[][]{
        {5, 6, 7, 8},
        {7, 8, 7, 9},
        {6, 8, 6, 3},
        {8, 6, 9, 4}}, 21);
  }

  /**
   * The algorithm must not get caught in infinite loops because of infinite
   * costs.
   */
  @Test (timeout=1000)
  public void testInfinityy() throws Exception {
    doTest(new float[][]{
        {0, 6, 7, Float.POSITIVE_INFINITY},
        {7, 8, 7, Float.POSITIVE_INFINITY},
        {6, 8, 6, Float.POSITIVE_INFINITY},
        {8, 6, 9, Float.POSITIVE_INFINITY}}, Float.POSITIVE_INFINITY);
  }

  /**
   * The example from John Clark's book "A First Look at Graph Theory". The
   * example in that book aimed to maximize profit, not minimize cost, so this
   * cost matrix was obtained by subtracting each profit value from 10. The
   * positions of the optimal assignments should be the same as the book.
   */
  @Test (timeout=100)
  public void testJohnClark() throws Exception {
    doTest(new float[][]{
        {7,  5, 5, 6,  9, 8, 7},
        {5,  3, 3, 4,  5, 6, 4},
        {8, 10, 8, 8,  8, 8, 9},
        {8,  6, 6, 6,  7, 8, 6},
        {9,  8, 9, 7,  9, 7, 9},
        {4,  3, 3, 5,  2, 3, 2},
        {8,  6, 6, 9, 10, 7, 7}}, 38);
  }

  /**
   * Test a larger instance of the matrix in testMultiplicative. This is a
   * worst case example.
   */
  @Test
  public void testLarge() throws Exception {
    int size = 800;
    float[][] cost = new float[size][size];
    for (int r = 0; r < size; r++) {
      for (int c = 0; c < size; c++) {
        cost[r][c] = (r + 1) * (c + 1);
      }
    }
    float expected = 0;
    for (int i = 0; i < size; i++) {
      expected += cost[size - 1 - i][i];
    }
    doTest(cost, expected);
  }

  /**
   * Test the worst case example for Munkres' original algorithm as given in
   * Jin Kue Wong's paper "A New Implementation of an Algorithm for the Optimal
   * Assignment Problem: An Improved Version of Munkres' Algorithm".
   * @throws Exception
   */
  @Test
  public void testJinKueWong() throws Exception {
    int size = 800;
    float[][] cost = new float[size][size];
    float expected = 0;
    for (int r = 0; r < size; r++) {
      for (int c = 0; c < size; c++) {
        if (r <= c) {
          cost[r][c] = r * c - r * (r - 1) / 2;
        } else {
          cost[r][c] = Float.POSITIVE_INFINITY;
        }
        // The optimal assignment is along the diagonal.
        if (r == c) {
          expected += cost[r][c];
        }
      }
    }
    doTest(cost, expected);
  }
}

/*
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

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Computes the optimal (minimal cost) assignment of jobs to workers (or other
 * analogous) concepts given a cost matrix of each pair of job and worker, using
 * the algorithm by James Munkres in "Algorithms for the Assignment and
 * Transportation Problems", with additional optimizations as described by Jin
 * Kue Wong in "A New Implementation of an Algorithm for the Optimal Assignment
 * Problem: An Improved Version of Munkres' Algorithm". The algorithm runs in
 * O(n^3) time and need O(n^2) auxiliary space where n is the number of jobs or
 * workers, whichever is greater.
 */
@InterfaceAudience.Private
public class MunkresAssignment {

  // The original algorithm by Munkres uses the terms STAR and PRIME to denote
  // different states of zero values in the cost matrix. These values are
  // represented as byte constants instead of enums to save space in the mask
  // matrix by a factor of 4n^2 where n is the size of the problem.
  private static final byte NONE = 0;
  private static final byte STAR = 1;
  private static final byte PRIME = 2;

  // The algorithm requires that the number of column is at least as great as
  // the number of rows. If that is not the case, then the cost matrix should
  // be transposed before computation, and the solution matrix transposed before
  // returning to the caller.
  private final boolean transposed;

  // The number of rows of internal matrices.
  private final int rows;

  // The number of columns of internal matrices.
  private final int cols;

  // The cost matrix, the cost of assigning each row index to column index.
  private float[][] cost;

  // Mask of zero cost assignment states.
  private byte[][] mask;

  // Covering some rows of the cost matrix.
  private boolean[] rowsCovered;

  // Covering some columns of the cost matrix.
  private boolean[] colsCovered;

  // The alternating path between starred zeroes and primed zeroes
  private Deque<Pair<Integer, Integer>> path;

  // The solution, marking which rows should be assigned to which columns. The
  // positions of elements in this array correspond to the rows of the cost
  // matrix, and the value of each element correspond to the columns of the cost
  // matrix, i.e. assignments[i] = j indicates that row i should be assigned to
  // column j.
  private int[] assignments;

  // Improvements described by Jin Kue Wong cache the least value in each row,
  // as well as the column index of the least value in each row, and the pending
  // adjustments to each row and each column.
  private float[] leastInRow;
  private int[] leastInRowIndex;
  private float[] rowAdjust;
  private float[] colAdjust;

  /**
   * Construct a new problem instance with the specified cost matrix. The cost
   * matrix must be rectangular, though not necessarily square. If one dimension
   * is greater than the other, some elements in the greater dimension will not
   * be assigned. The input cost matrix will not be modified.
   * @param costMatrix
   */
  public MunkresAssignment(float[][] costMatrix) {
    // The algorithm assumes that the number of columns is at least as great as
    // the number of rows. If this is not the case of the input matrix, then
    // all internal structures must be transposed relative to the input.
    this.transposed = costMatrix.length > costMatrix[0].length;
    if (this.transposed) {
      this.rows = costMatrix[0].length;
      this.cols = costMatrix.length;
    } else {
      this.rows = costMatrix.length;
      this.cols = costMatrix[0].length;
    }

    cost = new float[rows][cols];
    mask = new byte[rows][cols];
    rowsCovered = new boolean[rows];
    colsCovered = new boolean[cols];
    path = new LinkedList<Pair<Integer, Integer>>();

    leastInRow = new float[rows];
    leastInRowIndex = new int[rows];
    rowAdjust = new float[rows];
    colAdjust = new float[cols];

    assignments = null;

    // Copy cost matrix.
    if (transposed) {
      for (int r = 0; r < rows; r++) {
        for (int c = 0; c < cols; c++) {
          cost[r][c] = costMatrix[c][r];
        }
      }
    } else {
      for (int r = 0; r < rows; r++) {
        System.arraycopy(costMatrix[r], 0, cost[r], 0, cols);
      }
    }

    // Costs must be finite otherwise the matrix can get into a bad state where
    // no progress can be made. If your use case depends on a distinction
    // between costs of MAX_VALUE and POSITIVE_INFINITY, you're doing it wrong.
    for (int r = 0; r < rows; r++) {
      for (int c = 0; c < cols; c++) {
        if (cost[r][c] == Float.POSITIVE_INFINITY) {
          cost[r][c] = Float.MAX_VALUE;
        }
      }
    }
  }

  /**
   * Get the optimal assignments. The returned array will have the same number
   * of elements as the number of elements as the number of rows in the input
   * cost matrix. Each element will indicate which column should be assigned to
   * that row or -1 if no column should be assigned, i.e. if result[i] = j then
   * row i should be assigned to column j. Subsequent invocations of this method
   * will simply return the same object without additional computation.
   * @return an array with the optimal assignments
   */
  public int[] solve() {
    // If this assignment problem has already been solved, return the known
    // solution
    if (assignments != null) {
      return assignments;
    }

    preliminaries();

    // Find the optimal assignments.
    while (!testIsDone()) {
      while (!stepOne()) {
        stepThree();
      }
      stepTwo();
    }

    // Extract the assignments from the mask matrix.
    if (transposed) {
      assignments = new int[cols];
      outer:
      for (int c = 0; c < cols; c++) {
        for (int r = 0; r < rows; r++) {
          if (mask[r][c] == STAR) {
            assignments[c] = r;
            continue outer;
          }
        }
        // There is no assignment for this row of the input/output.
        assignments[c] = -1;
      }
    } else {
      assignments = new int[rows];
      outer:
      for (int r = 0; r < rows; r++) {
        for (int c = 0; c < cols; c++) {
          if (mask[r][c] == STAR) {
            assignments[r] = c;
            continue outer;
          }
        }
      }
    }

    // Once the solution has been computed, there is no need to keep any of the
    // other internal structures. Clear all unnecessary internal references so
    // the garbage collector may reclaim that memory.
    cost = null;
    mask = null;
    rowsCovered = null;
    colsCovered = null;
    path = null;
    leastInRow = null;
    leastInRowIndex = null;
    rowAdjust = null;
    colAdjust = null;

    return assignments;
  }

  /**
   * Corresponds to the "preliminaries" step of the original algorithm.
   * Guarantees that the matrix is an equivalent non-negative matrix with at
   * least one zero in each row.
   */
  private void preliminaries() {
    for (int r = 0; r < rows; r++) {
      // Find the minimum cost of each row.
      float min = Float.POSITIVE_INFINITY;
      for (int c = 0; c < cols; c++) {
        min = Math.min(min, cost[r][c]);
      }

      // Subtract that minimum cost from each element in the row.
      for (int c = 0; c < cols; c++) {
        cost[r][c] -= min;

        // If the element is now zero and there are no zeroes in the same row
        // or column which are already starred, then star this one. There
        // must be at least one zero because of subtracting the min cost.
        if (cost[r][c] == 0 && !rowsCovered[r] && !colsCovered[c]) {
          mask[r][c] = STAR;
          // Cover this row and column so that no other zeroes in them can be
          // starred.
          rowsCovered[r] = true;
          colsCovered[c] = true;
        }
      }
    }

    // Clear the covered rows and columns.
    Arrays.fill(rowsCovered, false);
    Arrays.fill(colsCovered, false);
  }

  /**
   * Test whether the algorithm is done, i.e. we have the optimal assignment.
   * This occurs when there is exactly one starred zero in each row.
   * @return true if the algorithm is done
   */
  private boolean testIsDone() {
    // Cover all columns containing a starred zero. There can be at most one
    // starred zero per column. Therefore, a covered column has an optimal
    // assignment.
    for (int r = 0; r < rows; r++) {
      for (int c = 0; c < cols; c++) {
        if (mask[r][c] == STAR) {
          colsCovered[c] = true;
        }
      }
    }

    // Count the total number of covered columns.
    int coveredCols = 0;
    for (int c = 0; c < cols; c++) {
      coveredCols += colsCovered[c] ? 1 : 0;
    }

    // Apply an row and column adjustments that are pending.
    for (int r = 0; r < rows; r++) {
      for (int c = 0; c < cols; c++) {
        cost[r][c] += rowAdjust[r];
        cost[r][c] += colAdjust[c];
      }
    }

    // Clear the pending row and column adjustments.
    Arrays.fill(rowAdjust, 0);
    Arrays.fill(colAdjust, 0);

    // The covers on columns and rows may have been reset, recompute the least
    // value for each row.
    for (int r = 0; r < rows; r++) {
      leastInRow[r] = Float.POSITIVE_INFINITY;
      for (int c = 0; c < cols; c++) {
        if (!rowsCovered[r] && !colsCovered[c] && cost[r][c] < leastInRow[r]) {
          leastInRow[r] = cost[r][c];
          leastInRowIndex[r] = c;
        }
      }
    }

    // If all columns are covered, then we are done. Since there may be more
    // columns than rows, we are also done if the number of covered columns is
    // at least as great as the number of rows.
    return (coveredCols == cols || coveredCols >= rows);
  }

  /**
   * Corresponds to step 1 of the original algorithm.
   * @return false if all zeroes are covered
   */
  private boolean stepOne() {
    while (true) {
      Pair<Integer, Integer> zero = findUncoveredZero();
      if (zero == null) {
        // No uncovered zeroes, need to manipulate the cost matrix in step
        // three.
        return false;
      } else {
        // Prime the uncovered zero and find a starred zero in the same row.
        mask[zero.getFirst()][zero.getSecond()] = PRIME;
        Pair<Integer, Integer> star = starInRow(zero.getFirst());
        if (star != null) {
          // Cover the row with both the newly primed zero and the starred zero.
          // Since this is the only place where zeroes are primed, and we cover
          // it here, and rows are only uncovered when primes are erased, then
          // there can be at most one primed uncovered zero.
          rowsCovered[star.getFirst()] = true;
          colsCovered[star.getSecond()] = false;
          updateMin(star.getFirst(), star.getSecond());
        } else {
          // Will go to step two after, where a path will be constructed,
          // starting from the uncovered primed zero (there is only one). Since
          // we have already found it, save it as the first node in the path.
          path.clear();
          path.offerLast(new Pair<Integer, Integer>(zero.getFirst(),
              zero.getSecond()));
          return true;
        }
      }
    }
  }

  /**
   * Corresponds to step 2 of the original algorithm.
   */
  private void stepTwo() {
    // Construct a path of alternating starred zeroes and primed zeroes, where
    // each starred zero is in the same column as the previous primed zero, and
    // each primed zero is in the same row as the previous starred zero. The
    // path will always end in a primed zero.
    while (true) {
      Pair<Integer, Integer> star = starInCol(path.getLast().getSecond());
      if (star != null) {
        path.offerLast(star);
      } else {
        break;
      }
      Pair<Integer, Integer> prime = primeInRow(path.getLast().getFirst());
      path.offerLast(prime);
    }

    // Augment path - unmask all starred zeroes and star all primed zeroes. All
    // nodes in the path will be either starred or primed zeroes. The set of
    // starred zeroes is independent and now one larger than before.
    for (Pair<Integer, Integer> p : path) {
      if (mask[p.getFirst()][p.getSecond()] == STAR) {
        mask[p.getFirst()][p.getSecond()] = NONE;
      } else {
        mask[p.getFirst()][p.getSecond()] = STAR;
      }
    }

    // Clear all covers from rows and columns.
    Arrays.fill(rowsCovered, false);
    Arrays.fill(colsCovered, false);

    // Remove the prime mask from all primed zeroes.
    for (int r = 0; r < rows; r++) {
      for (int c = 0; c < cols; c++) {
        if (mask[r][c] == PRIME) {
          mask[r][c] = NONE;
        }
      }
    }
  }

  /**
   * Corresponds to step 3 of the original algorithm.
   */
  private void stepThree() {
    // Find the minimum uncovered cost.
    float min = leastInRow[0];
    for (int r = 1; r < rows; r++) {
      if (leastInRow[r] < min) {
        min = leastInRow[r];
      }
    }

    // Add the minimum cost to each of the costs in a covered row, or subtract
    // the minimum cost from each of the costs in an uncovered column. As an
    // optimization, do not actually modify the cost matrix yet, but track the
    // adjustments that need to be made to each row and column.
    for (int r = 0; r < rows; r++) {
      if (rowsCovered[r]) {
        rowAdjust[r] += min;
      }
    }
    for (int c = 0; c < cols; c++) {
      if (!colsCovered[c]) {
        colAdjust[c] -= min;
      }
    }

    // Since the cost matrix is not being updated yet, the minimum uncovered
    // cost per row must be updated.
    for (int r = 0; r < rows; r++) {
      if (!colsCovered[leastInRowIndex[r]]) {
        // The least value in this row was in an uncovered column, meaning that
        // it would have had the minimum value subtracted from it, and therefore
        // will still be the minimum value in that row.
        leastInRow[r] -= min;
      } else {
        // The least value in this row was in a covered column and would not
        // have had the minimum value subtracted from it, so the minimum value
        // could be some in another column.
        for (int c = 0; c < cols; c++) {
          if (cost[r][c] + colAdjust[c] + rowAdjust[r] < leastInRow[r]) {
            leastInRow[r] = cost[r][c] + colAdjust[c] + rowAdjust[r];
            leastInRowIndex[r] = c;
          }
        }
      }
    }
  }

  /**
   * Find a zero cost assignment which is not covered. If there are no zero cost
   * assignments which are uncovered, then null will be returned.
   * @return pair of row and column indices of an uncovered zero or null
   */
  private Pair<Integer, Integer> findUncoveredZero() {
    for (int r = 0; r < rows; r++) {
      if (leastInRow[r] == 0) {
        return new Pair<Integer, Integer>(r, leastInRowIndex[r]);
      }
    }
    return null;
  }

  /**
   * A specified row has become covered, and a specified column has become
   * uncovered. The least value per row may need to be updated.
   * @param row the index of the row which was just covered
   * @param col the index of the column which was just uncovered
   */
  private void updateMin(int row, int col) {
    // If the row is covered we want to ignore it as far as least values go.
    leastInRow[row] = Float.POSITIVE_INFINITY;

    for (int r = 0; r < rows; r++) {
      // Since the column has only just been uncovered, it could not have any
      // pending adjustments. Only covered rows can have pending adjustments
      // and covered costs do not count toward row minimums. Therefore, we do
      // not need to consider rowAdjust[r] or colAdjust[col].
      if (!rowsCovered[r] && cost[r][col] < leastInRow[r]) {
        leastInRow[r] = cost[r][col];
        leastInRowIndex[r] = col;
      }
    }
  }

  /**
   * Find a starred zero in a specified row. If there are no starred zeroes in
   * the specified row, then null will be returned.
   * @param r the index of the row to be searched
   * @return pair of row and column indices of starred zero or null
   */
  private Pair<Integer, Integer> starInRow(int r) {
    for (int c = 0; c < cols; c++) {
      if (mask[r][c] == STAR) {
        return new Pair<Integer, Integer>(r, c);
      }
    }
    return null;
  }

  /**
   * Find a starred zero in the specified column. If there are no starred zeroes
   * in the specified row, then null will be returned.
   * @param c the index of the column to be searched
   * @return pair of row and column indices of starred zero or null
   */
  private Pair<Integer, Integer> starInCol(int c) {
    for (int r = 0; r < rows; r++) {
      if (mask[r][c] == STAR) {
        return new Pair<Integer, Integer>(r, c);
      }
    }
    return null;
  }

  /**
   * Find a primed zero in the specified row. If there are no primed zeroes in
   * the specified row, then null will be returned.
   * @param r the index of the row to be searched
   * @return pair of row and column indices of primed zero or null
   */
  private Pair<Integer, Integer> primeInRow(int r) {
    for (int c = 0; c < cols; c++) {
      if (mask[r][c] == PRIME) {
        return new Pair<Integer, Integer>(r, c);
      }
    }
    return null;
  }
}

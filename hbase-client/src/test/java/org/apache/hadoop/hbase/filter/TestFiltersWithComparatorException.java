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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFiltersWithComparatorException {

  byte[] cf = Bytes.toBytes("cf");
  byte[] row = Bytes.toBytes("row1");
  byte[] cq = Bytes.toBytes("q");
  long ts = 12345L;
  byte[] value = Bytes.toBytes("value");
  Cell testCell = new KeyValue(row, cf, cq, ts, value);

  @FunctionalInterface
  interface FilterFunctionThrowable {
    void run(Filter filter) throws IOException;
  }

  // Every filterX method that Filter implements to test
  List<FilterFunctionThrowable> filterFunctionsToTest = Arrays.asList((Filter filter) -> {
    filter.filterRowKey(testCell);
  }, (Filter filter) -> {
    filter.filterAllRemaining();
  }, (Filter filter) -> {
    filter.filterCell(testCell);
  }, (Filter filter) -> {
    filter.filterRowCells(new ArrayList<>(Collections.singletonList(testCell)));
  }, (Filter filter) -> {
    filter.filterRow();
  });

  /**
   * Comparator which throws RuntimeException for every `compareTo` method that
   * `ByteArrayComparable` implements + keeps a counter for number of compareTo invocations /
   * RuntimeExceptions thrown
   */
  static class BadComparator extends ByteArrayComparable {

    public int compareToInvokations = 0;

    public BadComparator() {
      super(new byte[1]);
    }

    @Override
    public byte[] toByteArray() {
      return new byte[1];
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
      compareToInvokations++;
      throw new RuntimeException("comparator runtime exception");
    }

    @Override
    public int compareTo(ByteBuffer value, int offset, int length) {
      compareToInvokations++;
      throw new RuntimeException("comparator runtime exception");
    }

    @Override
    public int compareTo(byte[] value) {
      compareToInvokations++;
      throw new RuntimeException("comparator runtime exception");
    }

  }

  /**
   * Verifies that a given {@link Filter} correctly triggers comparator logic and wraps any runtime
   * exceptions happening at comparison time in {@link org.apache.hadoop.hbase.HBaseIOException}
   * <p>
   * This method runs a set of predefined filter functions against the provided filter instance and
   * checks that if a comparator invocation occurs which throws a RuntimeException, it gets wrapped
   * in a {@code HBaseIOException} by the filter implementation The test fails if:
   * <ul>
   * <li>The comparator is never invoked by any tested function, or</li>
   * <li>A comparator invocation does not result in the expected exception.</li>
   * </ul>
   * @param filter        the filter instance under test
   * @param badComparator the comparator the filter was constructed with
   **/
  private void testFilter(Filter filter, BadComparator badComparator) {
    for (FilterFunctionThrowable filterFunction : filterFunctionsToTest) {
      int invocationsBefore = badComparator.compareToInvokations;
      boolean ioExceptionThrown = false;
      try {
        filterFunction.run(filter);
      } catch (HBaseIOException e) {
        ioExceptionThrown = true;
      }
      catch (IOException ignored) {}
      if (invocationsBefore != badComparator.compareToInvokations) {
        Assert.assertTrue("IOException should have been thrown", ioExceptionThrown);
      }
    }
    if (badComparator.compareToInvokations == 0) {
      Assert
        .fail(String.format(
          "Filter %s never invoked the comparator for any of the functions tested - "
            + "intended behavior was not tested, this is not expected",
          filter.getClass().getName()));
    }
  }

  @Test
  public void testColumnValueFilter() {
    BadComparator comparator = new BadComparator();
    ColumnValueFilter columnValueFilter =
      new ColumnValueFilter(cf, cq, CompareOperator.EQUAL, comparator);
    testFilter(columnValueFilter, comparator);
  }

  @Test
  public void testRowFilter() {
    BadComparator comparator = new BadComparator();
    RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, comparator);
    testFilter(rowFilter, comparator);
  }

  @Test
  public void testDependentColumnFilter() {
    BadComparator comparator = new BadComparator();
    DependentColumnFilter filter =
      new DependentColumnFilter(cf, cq, false, CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

  @Test
  public void testFamilyFilter() {
    BadComparator comparator = new BadComparator();
    FamilyFilter filter = new FamilyFilter(CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

  @Test
  public void testQualifierFilter() {
    BadComparator comparator = new BadComparator();
    QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

  @Test
  public void testSingleColumnValueExcludeFilter() {
    BadComparator comparator = new BadComparator();
    SingleColumnValueExcludeFilter filter =
      new SingleColumnValueExcludeFilter(cf, cq, CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

  @Test
  public void testSingleColumnValueFilter() {
    BadComparator comparator = new BadComparator();
    SingleColumnValueFilter filter =
      new SingleColumnValueFilter(cf, cq, CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

  @Test
  public void testValueFilter() {
    BadComparator comparator = new BadComparator();
    ValueFilter filter = new ValueFilter(CompareOperator.EQUAL, comparator);
    testFilter(filter, comparator);
  }

}

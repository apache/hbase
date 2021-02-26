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

package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.CompareType;

/**
 * This is a generic filter to be used to filter by comparison.  It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator.
 * <p>
 * To filter by row key, use {@link RowFilter}.
 * <p>
 * To filter by column family, use {@link FamilyFilter}.
 * <p>
 * To filter by column qualifier, use {@link QualifierFilter}.
 * <p>
 * To filter by value, use {@link ValueFilter}.
 * <p>
 * These filters can be wrapped with {@link SkipFilter} and {@link WhileMatchFilter}
 * to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 */
@InterfaceAudience.Public
public abstract class CompareFilter extends FilterBase {
  /**
   * Comparison operators. For filters only!
   * Use {@link CompareOperator} otherwise.
   * It (intentionally) has at least the below enums with same names.
   * @deprecated  since 2.0.0. Will be removed in 3.0.0. Use {@link CompareOperator} instead.
   */
  @Deprecated
  @InterfaceAudience.Public
  public enum CompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER,
    /** no operation */
    NO_OP,
  }

  protected CompareOperator op;
  protected ByteArrayComparable comparator;

  /**
   * Constructor.
   * @param compareOp the compare op for row matching
   * @param comparator the comparator for row matching
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use other constructor.
   */
  @Deprecated
  public CompareFilter(final CompareOp compareOp,
      final ByteArrayComparable comparator) {
    this(CompareOperator.valueOf(compareOp.name()), comparator);
  }

  /**
   * Constructor.
   * @param op the compare op for row matching
   * @param comparator the comparator for row matching
   */
  public CompareFilter(final CompareOperator op,
                       final ByteArrayComparable comparator) {
    this.op = op;
    this.comparator = comparator;
  }

  /**
   * @return operator
   * @deprecated  since 2.0.0. Will be removed in 3.0.0. Use {@link #getCompareOperator()} instead.
   */
  @Deprecated
  public CompareOp getOperator() {
    return CompareOp.valueOf(op.name());
  }

  public CompareOperator getCompareOperator() {
    return op;
  }

  /**
   * @return the comparator
   */
  public ByteArrayComparable getComparator() {
    return comparator;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  /**
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * Use {@link #compareRow(CompareOperator, ByteArrayComparable, Cell)}
   */
  @Deprecated
  protected boolean compareRow(final CompareOp compareOp, final ByteArrayComparable comparator,
      final Cell cell) {
    if (compareOp == CompareOp.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareRow(cell, comparator);
    return compare(compareOp, compareResult);
  }

  protected boolean compareRow(final CompareOperator op, final ByteArrayComparable comparator,
                               final Cell cell) {
    if (op == CompareOperator.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareRow(cell, comparator);
    return compare(op, compareResult);
  }

  /**
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * Use {@link #compareFamily(CompareOperator, ByteArrayComparable, Cell)}
   */
  @Deprecated
  protected boolean compareFamily(final CompareOp compareOp, final ByteArrayComparable comparator,
      final Cell cell) {
    if (compareOp == CompareOp.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareFamily(cell, comparator);
    return compare(compareOp, compareResult);
  }

  protected boolean compareFamily(final CompareOperator op, final ByteArrayComparable comparator,
                                  final Cell cell) {
    if (op == CompareOperator.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareFamily(cell, comparator);
    return compare(op, compareResult);
  }

  /**
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * Use {@link #compareQualifier(CompareOperator, ByteArrayComparable, Cell)}
   */
  @Deprecated
  protected boolean compareQualifier(final CompareOp compareOp,
      final ByteArrayComparable comparator, final Cell cell) {
    // We do not call through to the non-deprecated method for perf reasons.
    if (compareOp == CompareOp.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareQualifier(cell, comparator);
    return compare(compareOp, compareResult);
  }

  protected boolean compareQualifier(final CompareOperator op,
                                     final ByteArrayComparable comparator, final Cell cell) {
    // We do not call through to the non-deprecated method for perf reasons.
    if (op == CompareOperator.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareQualifier(cell, comparator);
    return compare(op, compareResult);
  }

  /**
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * Use {@link #compareValue(CompareOperator, ByteArrayComparable, Cell)}
   */
  @Deprecated
  protected boolean compareValue(final CompareOp compareOp, final ByteArrayComparable comparator,
      final Cell cell) {
    // We do not call through to the non-deprecated method for perf reasons.
    if (compareOp == CompareOp.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareValue(cell, comparator);
    return compare(compareOp, compareResult);
  }

  protected boolean compareValue(final CompareOperator op, final ByteArrayComparable comparator,
                                 final Cell cell) {
    if (op == CompareOperator.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareValue(cell, comparator);
    return compare(op, compareResult);
  }

  static boolean compare(final CompareOp op, int compareResult) {
    switch (op) {
    case LESS:
      return compareResult <= 0;
    case LESS_OR_EQUAL:
      return compareResult < 0;
    case EQUAL:
      return compareResult != 0;
    case NOT_EQUAL:
      return compareResult == 0;
    case GREATER_OR_EQUAL:
      return compareResult > 0;
    case GREATER:
      return compareResult >= 0;
    default:
      throw new RuntimeException("Unknown Compare op " + op.name());
    }
  }

  static boolean compare(final CompareOperator op, int compareResult) {
    switch (op) {
      case LESS:
        return compareResult <= 0;
      case LESS_OR_EQUAL:
        return compareResult < 0;
      case EQUAL:
        return compareResult != 0;
      case NOT_EQUAL:
        return compareResult == 0;
      case GREATER_OR_EQUAL:
        return compareResult > 0;
      case GREATER:
        return compareResult >= 0;
      default:
        throw new RuntimeException("Unknown Compare op " + op.name());
    }
  }

  // returns an array of heterogeneous objects
  public static ArrayList<Object> extractArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 2,
                                "Expected 2 but got: %s", filterArguments.size());
    CompareOperator op = ParseFilter.createCompareOperator(filterArguments.get(0));
    ByteArrayComparable comparator = ParseFilter.createComparator(
      ParseFilter.removeQuotesFromByteArray(filterArguments.get(1)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (op != CompareOperator.EQUAL &&
          op != CompareOperator.NOT_EQUAL) {
        throw new IllegalArgumentException ("A regexstring comparator and substring comparator" +
                                            " can only be used with EQUAL and NOT_EQUAL");
      }
    }
    ArrayList<Object> arguments = new ArrayList<>(2);
    arguments.add(op);
    arguments.add(comparator);
    return arguments;
  }

  /**
   * @return A pb instance to represent this instance.
   */
  FilterProtos.CompareFilter convert() {
    FilterProtos.CompareFilter.Builder builder =
      FilterProtos.CompareFilter.newBuilder();
    HBaseProtos.CompareType compareOp = CompareType.valueOf(this.op.name());
    builder.setCompareOp(compareOp);
    if (this.comparator != null) builder.setComparator(ProtobufUtil.toComparator(this.comparator));
    return builder.build();
  }

  /**
   *
   * @param o
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof CompareFilter)) return false;
    CompareFilter other = (CompareFilter)o;
    return this.getCompareOperator().equals(other.getCompareOperator()) &&
      (this.getComparator() == other.getComparator()
        || this.getComparator().areSerializedFieldsEqual(other.getComparator()));
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s)",
        this.getClass().getSimpleName(),
        this.op.name(),
        Bytes.toStringBinary(this.comparator.getValue()));
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getComparator(), this.getCompareOperator());
  }
}

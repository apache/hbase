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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Used to perform CheckAndRowMutate operations on a single row.
 */
@InterfaceAudience.Public
public class CheckAndRowMutate implements Row {
  private final byte[] row;
  private final byte[] family;
  private byte[] qualifier;
  private TimeRange timeRange = null;
  private CompareOperator op;
  private byte[] value;
  private Mutation mutation = null;

  /**
   * Create a CheckAndRowMutate operation for the specified row.
   *
   * @param row    row key
   * @param family family
   */
  public CheckAndRowMutate(byte[] row, byte[] family) {
    this.row = Bytes.copy(Mutation.checkRow(row));
    this.family = Preconditions.checkNotNull(family, "family is null");
  }

  /**
   * Create a CheckAndRowMutate operation for the specified row,
   * and an existing row lock.
   *
   * @param row       row key
   * @param family    family
   * @param qualifier qualifier
   * @param value     value
   * @param mutation  mutation
   */
  public CheckAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator compareOp,
      byte[] value, Mutation mutation) {
    this.row = Bytes.copy(Mutation.checkRow(row));
    this.family = Preconditions.checkNotNull(family, "family is null");
    this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null");
    this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
    this.value = Preconditions.checkNotNull(value, "value is null");
    this.mutation = mutation;
  }

  public CheckAndRowMutate qualifier(byte[] qualifier) {
    this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using"
        + " an empty byte array, or just do not call this method if you want a null qualifier");
    return this;
  }

  public CheckAndRowMutate timeRange(TimeRange timeRange) {
    this.timeRange = timeRange;
    return this;
  }

  public CheckAndRowMutate ifNotExists() {
    this.op = CompareOperator.EQUAL;
    this.value = null;
    return this;
  }

  public CheckAndRowMutate ifMatches(CompareOperator compareOp, byte[] value) {
    this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
    this.value = Preconditions.checkNotNull(value, "value is null");
    return this;
  }

  public CheckAndRowMutate addMutation(Mutation mutation) throws IOException {
    this.mutation = mutation;
    return this;
  }

  @Override
  public int compareTo(Row i) {
    return Bytes.compareTo(this.getRow(), i.getRow());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof CheckAndRowMutate) {
      CheckAndRowMutate other = (CheckAndRowMutate)obj;
      return compareTo(other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode(){
    return Arrays.hashCode(row);
  }
  /**
   * Method for retrieving the delete's row
   *
   * @return row
   */
  @Override
  public byte[] getRow() {
    return this.row;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public CompareOperator getOp() {
    return op;
  }

  public byte[] getValue() {
    return value;
  }

  public Mutation getMutation() {
    return mutation;
  }
}

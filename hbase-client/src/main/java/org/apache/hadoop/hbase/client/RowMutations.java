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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Performs multiple mutations atomically on a single row.
 *
 * The mutations are performed in the order in which they
 * were added.
 *
 * <p>We compare and equate mutations based off their row so be careful putting RowMutations
 * into Sets or using them as keys in Maps.
 */
@InterfaceAudience.Public
public class RowMutations implements Row {

  /**
   * Create a {@link RowMutations} with the specified mutations.
   * @param mutations the mutations to send
   * @return RowMutations
   * @throws IOException if any row in mutations is different to another
   */
  public static RowMutations of(List<? extends Mutation> mutations) throws IOException {
    if (CollectionUtils.isEmpty(mutations)) {
      throw new IllegalArgumentException("Cannot instantiate a RowMutations by empty list");
    }
    return new RowMutations(mutations.get(0).getRow(), mutations.size())
        .add(mutations);
  }

  private final List<Mutation> mutations;
  private final byte [] row;

  public RowMutations(byte [] row) {
    this(row, -1);
  }
  /**
   * Create an atomic mutation for the specified row.
   * @param row row key
   * @param initialCapacity the initial capacity of the RowMutations
   */
  public RowMutations(byte [] row, int initialCapacity) {
    this.row = Bytes.copy(Mutation.checkRow(row));
    if (initialCapacity <= 0) {
      this.mutations = new ArrayList<>();
    } else {
      this.mutations = new ArrayList<>(initialCapacity);
    }
  }

  /**
   * Add a {@link Put} operation to the list of mutations
   * @param p The {@link Put} to add
   * @throws IOException if the row of added mutation doesn't match the original row
   * @deprecated since 2.0 version and will be removed in 3.0 version.
   *             use {@link #add(Mutation)}
   */
  @Deprecated
  public void add(Put p) throws IOException {
    add((Mutation) p);
  }

  /**
   * Add a {@link Delete} operation to the list of mutations
   * @param d The {@link Delete} to add
   * @throws IOException if the row of added mutation doesn't match the original row
   * @deprecated since 2.0 version and will be removed in 3.0 version.
   *             use {@link #add(Mutation)}
   */
  @Deprecated
  public void add(Delete d) throws IOException {
    add((Mutation) d);
  }

  /**
   * Currently only supports {@link Put} and {@link Delete} mutations.
   *
   * @param mutation The data to send.
   * @throws IOException if the row of added mutation doesn't match the original row
   */
  public RowMutations add(Mutation mutation) throws IOException {
    return add(Collections.singletonList(mutation));
  }

  /**
   * @param mutations The data to send.
   * @throws IOException if the row of added mutation doesn't match the original row
   */
  public RowMutations add(List<? extends Mutation> mutations) throws IOException {
    for (Mutation mutation : mutations) {
      if (!Bytes.equals(row, mutation.getRow())) {
        throw new WrongRowIOException("The row in the recently added Mutation <" +
          Bytes.toStringBinary(mutation.getRow()) + "> doesn't match the original one <" +
          Bytes.toStringBinary(this.row) + ">");
      }
    }
    this.mutations.addAll(mutations);
    return this;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link Row#COMPARATOR} instead
   */
  @Deprecated
  @Override
  public int compareTo(Row i) {
    return Bytes.compareTo(this.getRow(), i.getRow());
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             No replacement
   */
  @Deprecated
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj instanceof RowMutations) {
      RowMutations other = (RowMutations)obj;
      return compareTo(other) == 0;
    }
    return false;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             No replacement
   */
  @Deprecated
  @Override
  public int hashCode(){
    return Arrays.hashCode(row);
  }

  @Override
  public byte[] getRow() {
    return row;
  }

  /**
   * @return An unmodifiable list of the current mutations.
   */
  public List<Mutation> getMutations() {
    return Collections.unmodifiableList(mutations);
  }

  public int getMaxPriority() {
    int maxPriority = Integer.MIN_VALUE;
    for (Mutation mutation : mutations) {
      maxPriority = Math.max(maxPriority, mutation.getPriority());
    }
    return maxPriority;
  }
}

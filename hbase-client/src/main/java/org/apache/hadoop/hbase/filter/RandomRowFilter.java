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

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter that includes rows based on a chance.
 * 
 */
@InterfaceAudience.Public
public class RandomRowFilter extends FilterBase {

  protected float chance;
  protected boolean filterOutRow;

  /**
   * Create a new filter with a specified chance for a row to be included.
   * 
   * @param chance
   */
  public RandomRowFilter(float chance) {
    this.chance = chance;
  }

  /**
   * @return The chance that a row gets included.
   */
  public float getChance() {
    return chance;
  }

  /**
   * Set the chance that a row is included.
   * 
   * @param chance
   */
  public void setChance(float chance) {
    this.chance = chance;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if (filterOutRow) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    return filterOutRow;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) {
    if (chance < 0) {
      // with a zero chance, the rows is always excluded
      filterOutRow = true;
    } else if (chance > 1) {
      // always included
      filterOutRow = false;
    } else {
      // roll the dice
      filterOutRow = !(ThreadLocalRandom.current().nextFloat() < chance);
    }
    return filterOutRow;
  }

  @Override
  public void reset() {
    filterOutRow = false;
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.RandomRowFilter.Builder builder =
      FilterProtos.RandomRowFilter.newBuilder();
    builder.setChance(this.chance);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link RandomRowFilter} instance
   * @return An instance of {@link RandomRowFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static RandomRowFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.RandomRowFilter proto;
    try {
      proto = FilterProtos.RandomRowFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new RandomRowFilter(proto.getChance());
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof RandomRowFilter)) return false;

    RandomRowFilter other = (RandomRowFilter)o;
    return this.getChance() == other.getChance();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getChance());
  }
}

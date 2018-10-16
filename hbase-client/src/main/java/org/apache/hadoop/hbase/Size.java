/**
 * Copyright The Apache Software Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import java.math.BigDecimal;
import java.util.Objects;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * It is used to represent the size with different units.
 * This class doesn't serve for the precise computation.
 */
@InterfaceAudience.Public
public final class Size implements Comparable<Size> {
  public static final Size ZERO = new Size(0, Unit.KILOBYTE);
  private static final BigDecimal SCALE_BASE = BigDecimal.valueOf(1024D);

  public enum Unit {
    // keep the room to add more units for HBase 10.x
    PETABYTE(100, "PB"),
    TERABYTE(99, "TB"),
    GIGABYTE(98, "GB"),
    MEGABYTE(97, "MB"),
    KILOBYTE(96, "KB"),
    BYTE(95, "B");
    private final int orderOfSize;
    private final String simpleName;

    Unit(int orderOfSize, String simpleName) {
      this.orderOfSize = orderOfSize;
      this.simpleName = simpleName;
    }

    public int getOrderOfSize() {
      return orderOfSize;
    }

    public String getSimpleName() {
      return simpleName;
    }
  }

  private final double value;
  private final Unit unit;

  public Size(double value, Unit unit) {
    if (value < 0) {
      throw new IllegalArgumentException("The value:" + value + " can't be negative");
    }
    this.value = value;
    this.unit = Preconditions.checkNotNull(unit);
  }

  /**
   * @return size unit
   */
  public Unit getUnit() {
    return unit;
  }

  /**
   * get the value
   */
  public long getLongValue() {
    return (long) value;
  }

  /**
   * get the value
   */
  public double get() {
    return value;
  }

  /**
   * get the value which is converted to specified unit.
   *
   * @param unit size unit
   * @return the converted value
   */
  public double get(Unit unit) {
    if (value == 0) {
      return value;
    }
    int diff = this.unit.getOrderOfSize() - unit.getOrderOfSize();
    if (diff == 0) {
      return value;
    }

    BigDecimal rval = BigDecimal.valueOf(value);
    for (int i = 0; i != Math.abs(diff); ++i) {
      rval = diff > 0 ? rval.multiply(SCALE_BASE) : rval.divide(SCALE_BASE);
    }
    return rval.doubleValue();
  }

  @Override
  public int compareTo(Size other) {
    int diff = unit.getOrderOfSize() - other.unit.getOrderOfSize();
    if (diff == 0) {
      return Double.compare(value, other.value);
    }

    BigDecimal thisValue = BigDecimal.valueOf(value);
    BigDecimal otherValue = BigDecimal.valueOf(other.value);
    if (diff > 0) {
      for (int i = 0; i != Math.abs(diff); ++i) {
        thisValue = thisValue.multiply(SCALE_BASE);
      }
    } else {
      for (int i = 0; i != Math.abs(diff); ++i) {
        otherValue = otherValue.multiply(SCALE_BASE);
      }
    }
    return thisValue.compareTo(otherValue);
  }

  @Override
  public String toString() {
    return value + unit.getSimpleName();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj instanceof Size) {
      return compareTo((Size)obj) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, unit);
  }
}

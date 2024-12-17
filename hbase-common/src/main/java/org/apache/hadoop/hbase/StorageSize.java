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
package org.apache.hadoop.hbase;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Locale;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This class is adapted from the Hadoop 3.x source code to HBase as part of a backporting effort to
 * support storage size parsing functionality in older versions of HBase.
 * <p>
 * Source: <a href=
 * "https://github.com/apache/hadoop/blob/branch-3.1.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/StorageSize.java">
 * Hadoop 3.1.0 StorageSize.java </a>
 * </p>
 */
@InterfaceAudience.Private
public class StorageSize {
  private final StorageUnit unit;
  private final double value;

  /**
   * Constucts a Storage Measure, which contains the value and the unit of measure.
   * @param unit  - Unit of Measure
   * @param value - Numeric value.
   */
  public StorageSize(StorageUnit unit, double value) {
    this.unit = unit;
    this.value = value;
  }

  private static void checkState(boolean state, String errorString) {
    if (!state) {
      throw new IllegalStateException(errorString);
    }
  }

  public static double getStorageSize(String value, double defaultValue, StorageUnit targetUnit) {
    Preconditions.checkNotNull(targetUnit, "Conversion unit cannot be null.");

    if (isBlank(value)) {
      return targetUnit.getDefault(defaultValue);
    }

    final StorageSize measure = parse(value);
    double byteValue = measure.getUnit().toBytes(measure.getValue());
    return targetUnit.fromBytes(byteValue);
  }

  public static StorageSize parse(String value) {
    checkState(isNotBlank(value), "value cannot be blank");
    String sanitizedValue = value.trim().toLowerCase(Locale.ENGLISH);
    StorageUnit parsedUnit = null;
    for (StorageUnit unit : StorageUnit.values()) {
      if (
        sanitizedValue.endsWith(unit.getShortName()) || sanitizedValue.endsWith(unit.getLongName())
          || sanitizedValue.endsWith(unit.getSuffixChar())
      ) {
        parsedUnit = unit;
        break;
      }
    }

    if (parsedUnit == null) {
      throw new IllegalArgumentException(
        value + " is not in expected format." + "Expected format is <number><unit>. e.g. 1000MB");
    }

    String suffix = "";
    boolean found = false;

    // We are trying to get the longest match first, so the order of
    // matching is getLongName, getShortName and then getSuffixChar.
    if (!found && sanitizedValue.endsWith(parsedUnit.getLongName())) {
      found = true;
      suffix = parsedUnit.getLongName();
    }

    if (!found && sanitizedValue.endsWith(parsedUnit.getShortName())) {
      found = true;
      suffix = parsedUnit.getShortName();
    }

    if (!found && sanitizedValue.endsWith(parsedUnit.getSuffixChar())) {
      found = true;
      suffix = parsedUnit.getSuffixChar();
    }

    checkState(found, "Something is wrong, we have to find a " + "match. Internal error.");

    String valString = sanitizedValue.substring(0, value.length() - suffix.length());
    return new StorageSize(parsedUnit, Double.parseDouble(valString));

  }

  public StorageUnit getUnit() {
    return unit;
  }

  public double getValue() {
    return value;
  }
}

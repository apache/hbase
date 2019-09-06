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
package org.apache.hadoop.hbase.hbtop.field;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import org.apache.hadoop.hbase.Size;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a value of a field.
 *
 * The type of a value is defined by {@link FieldValue}.
 */
@InterfaceAudience.Private
public final class FieldValue implements Comparable<FieldValue> {

  private final Object value;
  private final FieldValueType type;

  FieldValue(Object value, FieldValueType type) {
    Objects.requireNonNull(value);
    this.type = Objects.requireNonNull(type);

    switch (type) {
      case STRING:
        if (value instanceof String) {
          this.value = value;
          break;
        }
        throw new IllegalArgumentException("invalid type");

      case INTEGER:
        if (value instanceof Integer) {
          this.value = value;
          break;
        } else if (value instanceof String) {
          this.value = Integer.valueOf((String) value);
          break;
        }
        throw new IllegalArgumentException("invalid type");

      case LONG:
        if (value instanceof Long) {
          this.value = value;
          break;
        } else if (value instanceof String) {
          this.value = Long.valueOf((String) value);
          break;
        }
        throw new IllegalArgumentException("invalid type");

      case FLOAT:
        if (value instanceof Float) {
          this.value = value;
          break;
        } else if (value instanceof String) {
          this.value = Float.valueOf((String) value);
          break;
        }
        throw new IllegalArgumentException("invalid type");

      case SIZE:
        if (value instanceof Size) {
          this.value = optimizeSize((Size) value);
          break;
        } else if (value instanceof String) {
          this.value = optimizeSize(parseSizeString((String) value));
          break;
        }
        throw new IllegalArgumentException("invalid type");

      case PERCENT:
        if (value instanceof Float) {
          this.value = value;
          break;
        } else if (value instanceof String) {
          this.value = parsePercentString((String) value);
          break;
        }
        throw new IllegalArgumentException("invalid type");

      default:
        throw new AssertionError();
    }
  }

  private Size optimizeSize(Size size) {
    if (size.get(Size.Unit.BYTE) < 1024d) {
      return size.getUnit() == Size.Unit.BYTE ?
        size : new Size(size.get(Size.Unit.BYTE), Size.Unit.BYTE);
    } else if (size.get(Size.Unit.KILOBYTE) < 1024d) {
      return size.getUnit() == Size.Unit.KILOBYTE ?
        size : new Size(size.get(Size.Unit.KILOBYTE), Size.Unit.KILOBYTE);
    } else if (size.get(Size.Unit.MEGABYTE) < 1024d) {
      return size.getUnit() == Size.Unit.MEGABYTE ?
        size : new Size(size.get(Size.Unit.MEGABYTE), Size.Unit.MEGABYTE);
    } else if (size.get(Size.Unit.GIGABYTE) < 1024d) {
      return size.getUnit() == Size.Unit.GIGABYTE ?
        size : new Size(size.get(Size.Unit.GIGABYTE), Size.Unit.GIGABYTE);
    } else if (size.get(Size.Unit.TERABYTE) < 1024d) {
      return size.getUnit() == Size.Unit.TERABYTE ?
        size : new Size(size.get(Size.Unit.TERABYTE), Size.Unit.TERABYTE);
    }
    return size.getUnit() == Size.Unit.PETABYTE ?
      size : new Size(size.get(Size.Unit.PETABYTE), Size.Unit.PETABYTE);
  }

  private Size parseSizeString(String sizeString) {
    if (sizeString.length() < 3) {
      throw new IllegalArgumentException("invalid size");
    }

    String valueString = sizeString.substring(0, sizeString.length() - 2);
    String unitSimpleName = sizeString.substring(sizeString.length() - 2);
    return new Size(Double.parseDouble(valueString), convertToUnit(unitSimpleName));
  }

  private Size.Unit convertToUnit(String unitSimpleName) {
    for (Size.Unit unit: Size.Unit.values()) {
      if (unitSimpleName.equals(unit.getSimpleName())) {
        return unit;
      }
    }
    throw new IllegalArgumentException("invalid size");
  }

  private Float parsePercentString(String percentString) {
    if (percentString.endsWith("%")) {
      percentString = percentString.substring(0, percentString.length() - 1);
    }
    return Float.valueOf(percentString);
  }

  public String asString() {
    return toString();
  }

  public int asInt() {
    return (Integer) value;
  }

  public long asLong() {
    return (Long) value;
  }

  public float asFloat() {
    return (Float) value;
  }

  public Size asSize() {
    return (Size) value;
  }

  @Override
  public String toString() {
    switch (type) {
      case STRING:
      case INTEGER:
      case LONG:
      case FLOAT:
      case SIZE:
        return value.toString();

      case PERCENT:
        return String.format("%.2f", (Float) value) + "%";

      default:
        throw new AssertionError();
    }
  }

  @Override
  public int compareTo(@NonNull FieldValue o) {
    if (type != o.type) {
      throw new IllegalArgumentException("invalid type");
    }

    switch (type) {
      case STRING:
        return ((String) value).compareTo((String) o.value);

      case INTEGER:
        return ((Integer) value).compareTo((Integer) o.value);

      case LONG:
        return ((Long) value).compareTo((Long) o.value);

      case FLOAT:
      case PERCENT:
        return ((Float) value).compareTo((Float) o.value);

      case SIZE:
        return ((Size) value).compareTo((Size) o.value);

      default:
        throw new AssertionError();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FieldValue)) {
      return false;
    }
    FieldValue that = (FieldValue) o;
    return value.equals(that.value) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, type);
  }

  public FieldValue plus(FieldValue o) {
    if (type != o.type) {
      throw new IllegalArgumentException("invalid type");
    }

    switch (type) {
      case STRING:
        return new FieldValue(((String) value).concat((String) o.value), type);

      case INTEGER:
        return new FieldValue(((Integer) value) + ((Integer) o.value), type);

      case LONG:
        return new FieldValue(((Long) value) + ((Long) o.value), type);

      case FLOAT:
      case PERCENT:
        return new FieldValue(((Float) value) + ((Float) o.value), type);

      case SIZE:
        Size size = (Size) value;
        Size oSize = (Size) o.value;
        Size.Unit unit = size.getUnit();
        return new FieldValue(new Size(size.get(unit) + oSize.get(unit), unit), type);

      default:
        throw new AssertionError();
    }
  }

  public int compareToIgnoreCase(FieldValue o) {
    if (type != o.type) {
      throw new IllegalArgumentException("invalid type");
    }

    switch (type) {
      case STRING:
        return ((String) value).compareToIgnoreCase((String) o.value);

      case INTEGER:
      case LONG:
      case FLOAT:
      case SIZE:
      case PERCENT:
        return compareTo(o);

      default:
        throw new AssertionError();
    }
  }
}

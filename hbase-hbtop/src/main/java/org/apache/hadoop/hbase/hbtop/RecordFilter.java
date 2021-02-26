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
package org.apache.hadoop.hbase.hbtop;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a filter that's filtering the metric {@link Record}s.
 */
@InterfaceAudience.Private
public final class RecordFilter {

  private enum Operator {
    EQUAL("="),
    DOUBLE_EQUALS("=="),
    GREATER(">"),
    GREATER_OR_EQUAL(">="),
    LESS("<"),
    LESS_OR_EQUAL("<=");

    private final String operator;

    Operator(String operator) {
      this.operator = operator;
    }

    @Override
    public String toString() {
      return operator;
    }
  }

  public static RecordFilter parse(String filterString, boolean ignoreCase) {
    return parse(filterString, Arrays.asList(Field.values()), ignoreCase);
  }

  /*
   * Parse a filter string and build a RecordFilter instance.
   */
  public static RecordFilter parse(String filterString, List<Field> fields, boolean ignoreCase) {
    int index = 0;

    boolean not = isNot(filterString);
    if (not) {
      index += 1;
    }

    StringBuilder fieldString = new StringBuilder();
    while (filterString.length() > index && filterString.charAt(index) != '<'
      && filterString.charAt(index) != '>' && filterString.charAt(index) != '=') {
      fieldString.append(filterString.charAt(index++));
    }

    if (fieldString.length() == 0 || filterString.length() == index) {
      return null;
    }

    Field field = getField(fields, fieldString.toString());
    if (field == null) {
      return null;
    }

    StringBuilder operatorString = new StringBuilder();
    while (filterString.length() > index && (filterString.charAt(index) == '<' ||
      filterString.charAt(index) == '>' || filterString.charAt(index) == '=')) {
      operatorString.append(filterString.charAt(index++));
    }

    Operator operator = getOperator(operatorString.toString());
    if (operator == null) {
      return null;
    }

    String value = filterString.substring(index);
    FieldValue fieldValue = getFieldValue(field, value);
    if (fieldValue == null) {
      return null;
    }

    return new RecordFilter(ignoreCase, not, field, operator, fieldValue);
  }

  private static FieldValue getFieldValue(Field field, String value) {
    try {
      return field.newValue(value);
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean isNot(String filterString) {
    return filterString.startsWith("!");
  }

  private static Field getField(List<Field> fields, String fieldString) {
    for (Field f : fields) {
      if (f.getHeader().equals(fieldString)) {
        return f;
      }
    }
    return null;
  }

  private static Operator getOperator(String operatorString) {
    for (Operator o : Operator.values()) {
      if (operatorString.equals(o.toString())) {
        return o;
      }
    }
    return null;
  }

  private final boolean ignoreCase;
  private final boolean not;
  private final Field field;
  private final Operator operator;
  private final FieldValue value;

  private RecordFilter(boolean ignoreCase, boolean not, Field field, Operator operator,
    FieldValue value) {
    this.ignoreCase = ignoreCase;
    this.not = not;
    this.field = Objects.requireNonNull(field);
    this.operator = Objects.requireNonNull(operator);
    this.value = Objects.requireNonNull(value);
  }

  public Field getField() {
    return field;
  }

  public boolean execute(Record record) {
    FieldValue fieldValue = record.get(field);
    if (fieldValue == null) {
      return false;
    }

    if (operator == Operator.EQUAL) {
      boolean ret;
      if (ignoreCase) {
        ret = fieldValue.asString().toLowerCase().contains(value.asString().toLowerCase());
      } else {
        ret = fieldValue.asString().contains(value.asString());
      }
      return not != ret;
    }

    int compare = ignoreCase ?
      fieldValue.compareToIgnoreCase(value) : fieldValue.compareTo(value);

    boolean ret;
    switch (operator) {
      case DOUBLE_EQUALS:
        ret = compare == 0;
        break;

      case GREATER:
        ret = compare > 0;
        break;

      case GREATER_OR_EQUAL:
        ret = compare >= 0;
        break;

      case LESS:
        ret = compare < 0;
        break;

      case LESS_OR_EQUAL:
        ret = compare <= 0;
        break;

      default:
        throw new AssertionError();
    }
    return not != ret;
  }

  @Override
  public String toString() {
    return (not ? "!" : "") + field.getHeader() + operator + value.asString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RecordFilter)) {
      return false;
    }
    RecordFilter filter = (RecordFilter) o;
    return ignoreCase == filter.ignoreCase && not == filter.not && field == filter.field
      && operator == filter.operator && value.equals(filter.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ignoreCase, not, field, operator, value);
  }

  /*
   * For FilterBuilder
   */
  public static FilterBuilder newBuilder(Field field) {
    return new FilterBuilder(field, false);
  }

  public static FilterBuilder newBuilder(Field field, boolean ignoreCase) {
    return new FilterBuilder(field, ignoreCase);
  }

  public static final class FilterBuilder {
    private final Field field;
    private final boolean ignoreCase;

    private FilterBuilder(Field field, boolean ignoreCase) {
      this.field = Objects.requireNonNull(field);
      this.ignoreCase = ignoreCase;
    }

    public RecordFilter equal(FieldValue value) {
      return newFilter(false, Operator.EQUAL, value);
    }

    public RecordFilter equal(Object value) {
      return equal(field.newValue(value));
    }

    public RecordFilter notEqual(FieldValue value) {
      return newFilter(true, Operator.EQUAL, value);
    }

    public RecordFilter notEqual(Object value) {
      return notEqual(field.newValue(value));
    }

    public RecordFilter doubleEquals(FieldValue value) {
      return newFilter(false, Operator.DOUBLE_EQUALS, value);
    }

    public RecordFilter doubleEquals(Object value) {
      return doubleEquals(field.newValue(value));
    }

    public RecordFilter notDoubleEquals(FieldValue value) {
      return newFilter(true, Operator.DOUBLE_EQUALS, value);
    }

    public RecordFilter notDoubleEquals(Object value) {
      return notDoubleEquals(field.newValue(value));
    }

    public RecordFilter greater(FieldValue value) {
      return newFilter(false, Operator.GREATER, value);
    }

    public RecordFilter greater(Object value) {
      return greater(field.newValue(value));
    }

    public RecordFilter notGreater(FieldValue value) {
      return newFilter(true, Operator.GREATER, value);
    }

    public RecordFilter notGreater(Object value) {
      return notGreater(field.newValue(value));
    }

    public RecordFilter greaterOrEqual(FieldValue value) {
      return newFilter(false, Operator.GREATER_OR_EQUAL, value);
    }

    public RecordFilter greaterOrEqual(Object value) {
      return greaterOrEqual(field.newValue(value));
    }

    public RecordFilter notGreaterOrEqual(FieldValue value) {
      return newFilter(true, Operator.GREATER_OR_EQUAL, value);
    }

    public RecordFilter notGreaterOrEqual(Object value) {
      return notGreaterOrEqual(field.newValue(value));
    }

    public RecordFilter less(FieldValue value) {
      return newFilter(false, Operator.LESS, value);
    }

    public RecordFilter less(Object value) {
      return less(field.newValue(value));
    }

    public RecordFilter notLess(FieldValue value) {
      return newFilter(true, Operator.LESS, value);
    }

    public RecordFilter notLess(Object value) {
      return notLess(field.newValue(value));
    }

    public RecordFilter lessOrEqual(FieldValue value) {
      return newFilter(false, Operator.LESS_OR_EQUAL, value);
    }

    public RecordFilter lessOrEqual(Object value) {
      return lessOrEqual(field.newValue(value));
    }

    public RecordFilter notLessOrEqual(FieldValue value) {
      return newFilter(true, Operator.LESS_OR_EQUAL, value);
    }

    public RecordFilter notLessOrEqual(Object value) {
      return notLessOrEqual(field.newValue(value));
    }

    private RecordFilter newFilter(boolean not, Operator operator, FieldValue value) {
      return new RecordFilter(ignoreCase, not, field, operator, value);
    }
  }
}

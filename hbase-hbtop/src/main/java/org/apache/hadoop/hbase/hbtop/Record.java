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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a record of the metrics in the top screen.
 */
@InterfaceAudience.Private
public class Record implements Map<Field, FieldValue> {

  private Map<Field, FieldValue> values;

  public static final class Entry implements Map.Entry<Field, FieldValue> {
    private final Field key;
    private FieldValue value;

    private Entry(Field key, FieldValue value) {
      this.key = Objects.requireNonNull(key);
      this.value = Objects.requireNonNull(value);
    }

    @Override
    public Field getKey() {
      return key;
    }

    @Override
    public FieldValue getValue() {
      return value;
    }

    @Override
    public FieldValue setValue(FieldValue value) {
      FieldValue oldValue = this.value;
      this.value = value;
      return oldValue;
    }
  }

  public static Entry entry(Field field, Object value) {
    return new Entry(field, field.newValue(value));
  }

  public static Entry entry(Field field, FieldValue value) {
    return new Entry(field, value);
  }

  public static Record ofEntries(List<Entry> entries) {
    return ofEntries(entries.stream());
  }

  public static Record ofEntries(Entry... entries) {
    return ofEntries(Stream.of(entries));
  }

  public static Record ofEntries(Stream<Entry> entries) {
    return entries
      .collect(Record::new, (r, e) -> r.put(e.getKey(), e.getValue()), (r1, r2) -> {});
  }

  public Record() {
    this(new EnumMap<>(Field.class));
  }

  private Record(Map<Field, FieldValue> values) {
    this.values = values;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public boolean isEmpty() {
    return values.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return values.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return values.containsValue(value);
  }

  @Override
  public FieldValue get(Object key) {
    return values.get(key);
  }

  @Override
  public FieldValue put(Field key, FieldValue value) {
    return values.put(key, value);
  }

  public FieldValue put(Field key, Object value) {
    return values.put(key, key.newValue(value));
  }

  @Override
  public FieldValue remove(Object key) {
    return values.remove(key);
  }

  @Override
  public void putAll(@NotNull Map<? extends Field, ? extends FieldValue> m) {
    values.putAll(m);
  }

  @Override
  public void clear() {
    values.clear();
  }

  @Override
  @NotNull
  public Set<Field> keySet() {
    return values.keySet();
  }

  @Override
  @NotNull
  public Collection<FieldValue> values() {
    return values.values();
  }

  @Override
  @NotNull
  public Set<Map.Entry<Field, FieldValue>> entrySet() {
    return values.entrySet();
  }

  public Record combine(Record o) {
    return ofEntries(values.keySet().stream()
      .map(k -> {
        switch (k.getFieldValueType()) {
          case STRING:
            return entry(k, values.get(k));
          default:
            return entry(k, values.get(k).plus(o.values.get(k)));
        }
      }));
  }

  public Record toImmutable() {
    return new Record(Collections.unmodifiableMap(values));
  }
}

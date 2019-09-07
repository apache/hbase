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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.hadoop.hbase.hbtop.field.FieldValueType;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Represents a record of the metrics in the top screen.
 */
@InterfaceAudience.Private
public final class Record implements Map<Field, FieldValue> {

  private final ImmutableMap<Field, FieldValue> values;

  public final static class Entry extends AbstractMap.SimpleImmutableEntry<Field, FieldValue> {
    private Entry(Field key, FieldValue value) {
      super(key, value);
    }
  }

  public final static class Builder {

    private final ImmutableMap.Builder<Field, FieldValue> builder;

    private Builder() {
      builder = ImmutableMap.builder();
    }

    public Builder put(Field key, Object value) {
      builder.put(key, key.newValue(value));
      return this;
    }

    public Builder put(Field key, FieldValue value) {
      builder.put(key, value);
      return this;
    }

    public Builder put(Entry entry) {
      builder.put(entry);
      return this;
    }

    public Builder putAll(Map<Field, FieldValue> map) {
      builder.putAll(map);
      return this;
    }

    public Record build() {
      return new Record(builder.build());
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Entry entry(Field field, Object value) {
    return new Entry(field, field.newValue(value));
  }

  public static Entry entry(Field field, FieldValue value) {
    return new Entry(field, value);
  }

  public static Record ofEntries(Entry... entries) {
    return ofEntries(Stream.of(entries));
  }

  public static Record ofEntries(Stream<Entry> entries) {
    return entries.collect(Record::builder, Builder::put, (r1, r2) -> {}).build();
  }

  private Record(ImmutableMap<Field, FieldValue> values) {
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
    throw new UnsupportedOperationException();
  }

  @Override
  public FieldValue remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(@NonNull Map<? extends Field, ? extends FieldValue> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  @NonNull
  public Set<Field> keySet() {
    return values.keySet();
  }

  @Override
  @NonNull
  public Collection<FieldValue> values() {
    return values.values();
  }

  @Override
  @NonNull
  public Set<Map.Entry<Field, FieldValue>> entrySet() {
    return values.entrySet();
  }

  public Record combine(Record o) {
    return ofEntries(values.keySet().stream()
      .map(k -> {
        if (k.getFieldValueType() == FieldValueType.STRING) {
          return entry(k, values.get(k));
        }
        return entry(k, values.get(k).plus(o.values.get(k)));
      }));
  }
}

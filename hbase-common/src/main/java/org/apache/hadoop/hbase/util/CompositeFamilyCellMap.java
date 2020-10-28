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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Composite read-only familyCellMap
 */
@InterfaceAudience.Private
public class CompositeFamilyCellMap implements NavigableMap<byte[], List<Cell>> {

  private static class CompositeFamilyCellMapEntry implements Entry<byte[], List<Cell>> {
    private final byte[] key;
    private final List<Cell> value;

    public CompositeFamilyCellMapEntry(byte[] key, List<Cell> value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public byte[] getKey() {
      return key;
    }

    @Override
    public List<Cell> getValue() {
      return value;
    }

    @Override
    public List<Cell> setValue(List<Cell> value) {
      throw new UnsupportedOperationException("read-only");
    }
  }

  private final List<NavigableMap<byte[], List<Cell>>> familyCellMapList = new ArrayList<>();

  public CompositeFamilyCellMap() {
  }

  public CompositeFamilyCellMap(List<NavigableMap<byte[], List<Cell>>> familyCellMapList) {
    for (NavigableMap<byte[], List<Cell>> familyCellMap : familyCellMapList) {
      addFamilyCellMap(familyCellMap);
    }
  }

  public void addFamilyCellMap(NavigableMap<byte[], List<Cell>> familyCellMap) {
    familyCellMapList.add(familyCellMap);
  }

  @Override
  public Entry<byte[], List<Cell>> lowerEntry(byte[] key) {
    byte[] lowerKey = lowerKey(key);
    return new CompositeFamilyCellMapEntry(lowerKey, get(lowerKey));
  }

  @Override
  public byte[] lowerKey(byte[] key) {
    return navigableKeySet().lower(key);
  }

  @Override
  public Entry<byte[], List<Cell>> floorEntry(byte[] key) {
    byte[] floorKey = floorKey(key);
    return new CompositeFamilyCellMapEntry(floorKey, get(floorKey));
  }

  @Override
  public byte[] floorKey(byte[] key) {
    return navigableKeySet().floor(key);
  }

  @Override
  public Entry<byte[], List<Cell>> ceilingEntry(byte[] key) {
    byte[] ceilingKey = ceilingKey(key);
    return new CompositeFamilyCellMapEntry(ceilingKey, get(ceilingKey));
  }

  @Override
  public byte[] ceilingKey(byte[] key) {
    return navigableKeySet().ceiling(key);
  }

  @Override
  public Entry<byte[], List<Cell>> higherEntry(byte[] key) {
    byte[] higherKey = higherKey(key);
    return new CompositeFamilyCellMapEntry(higherKey, get(higherKey));
  }

  @Override
  public byte[] higherKey(byte[] key) {
    return navigableKeySet().higher(key);
  }

  @Override
  public Entry<byte[], List<Cell>> firstEntry() {
    byte[] firstKey = firstKey();
    return new CompositeFamilyCellMapEntry(firstKey, get(firstKey));
  }

  @Override
  public Entry<byte[], List<Cell>> lastEntry() {
    byte[] lastKey = lastKey();
    return new CompositeFamilyCellMapEntry(lastKey, get(lastKey));
  }

  @Override
  public Entry<byte[], List<Cell>> pollFirstEntry() {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public Entry<byte[], List<Cell>> pollLastEntry() {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public NavigableMap<byte[], List<Cell>> descendingMap() {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : descendingKeySet()) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public NavigableSet<byte[]> navigableKeySet() {
    NavigableSet<byte[]> keySet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (NavigableMap<byte[], List<Cell>> familyMap : familyCellMapList) {
      keySet.addAll(familyMap.keySet());
    }
    return keySet;
  }

  @Override
  public NavigableSet<byte[]> descendingKeySet() {
    return navigableKeySet().descendingSet();
  }

  @Override
  public NavigableMap<byte[], List<Cell>> subMap(byte[] fromKey, boolean fromInclusive,
    byte[] toKey, boolean toInclusive) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().subSet(fromKey, fromInclusive, toKey, toInclusive)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> headMap(byte[] toKey, boolean inclusive) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().headSet(toKey, inclusive)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> tailMap(byte[] fromKey, boolean inclusive) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().tailSet(fromKey, inclusive)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public Comparator<? super byte[]> comparator() {
    return Bytes.BYTES_COMPARATOR;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> subMap(byte[] fromKey, byte[] toKey) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().subSet(fromKey, toKey)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> headMap(byte[] toKey) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().headSet(toKey)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> tailMap(byte[] fromKey) {
    NavigableMap<byte[], List<Cell>> ret = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : navigableKeySet().tailSet(fromKey)) {
      ret.put(key, get(key));
    }
    return ret;
  }

  @Override
  public byte[] firstKey() {
    return navigableKeySet().first();
  }

  @Override
  public byte[] lastKey() {
    return navigableKeySet().last();
  }

  @Override
  public int size() {
    return familyCellMapList.stream().mapToInt(Map::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return familyCellMapList.stream().allMatch(Map::isEmpty);
  }

  @Override
  public boolean containsKey(Object key) {
    return familyCellMapList.stream().anyMatch(familyMap -> familyMap.containsKey(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return familyCellMapList.stream().anyMatch(familyMap -> familyMap.containsValue(value));
  }

  @Override
  public List<Cell> get(Object key) {
    return familyCellMapList.stream()
      .map(familyMap -> familyMap.getOrDefault(key, Collections.emptyList()))
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  @Override
  public List<Cell> put(byte[] key, List<Cell> value) {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public List<Cell> remove(Object key) {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public void putAll(Map<? extends byte[], ? extends List<Cell>> m) {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("read-only");
  }

  @Override
  public Set<byte[]> keySet() {
    return navigableKeySet();
  }

  @Override
  public Collection<List<Cell>> values() {
    Set<byte[]> keySet = keySet();
    List<List<Cell>> ret = new ArrayList<>(keySet.size());
    for (byte[] key : keySet) {
      ret.add(get(key));
    }
    return ret;
  }

  @Override
  public Set<Entry<byte[], List<Cell>>> entrySet() {
    Set<Entry<byte[], List<Cell>>> ret = new TreeSet<>(
      (o1, o2) -> Bytes.BYTES_COMPARATOR.compare(o1.getKey(), o2.getKey()));
    for (byte[] key : keySet()) {
      ret.add(new CompositeFamilyCellMapEntry(key, get(key)));
    }
    return ret;
  }
}

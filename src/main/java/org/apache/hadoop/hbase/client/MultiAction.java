/*
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

/**
 * Container for Actions (i.e. Get, Delete, or Put), which are grouped by
 * regionName. Intended to be used with HConnectionManager.processBatch()
 */
@ThriftStruct
public final class MultiAction implements Writable {

  private static final int VERSION_0 = 0;
  // map of regions to lists of puts/gets/deletes for that region.
  private Map<byte[], List<Get>> gets = null;
  private Map<byte[], List<Put>> puts = null;
  private Map<byte[], List<Delete>> deletes = null;
  private Map<byte[], List<Integer>> originalIndex = null;

  public MultiAction() {
  }

  /**
   * Thrift constructor for MultiAction serialization
   * @param gets
   * @param puts
   * @param deletes
   * @param originalIndex
   */
  @ThriftConstructor
  public MultiAction(@ThriftField(1) Map<byte[], List<Get>> gets,
      @ThriftField(2) Map<byte[], List<Put>> puts,
      @ThriftField(3) Map<byte[], List<Delete>> deletes) {
    if (gets != null) {
      this.gets = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.gets.putAll(gets);
    }
    if (puts != null) {
      this.puts = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.puts.putAll(puts);
    }
    if (deletes != null) {
      this.deletes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.deletes.putAll(deletes);
    }
  }

  @ThriftField(1)
  public Map<byte[], List<Get>> getGets() {
    return gets;
  }

  @ThriftField(2)
  public Map<byte[], List<Put>> getPuts() {
    return puts;
  }

  @ThriftField(3)
  public Map<byte[], List<Delete>> getDeletes() {
    return deletes;
  }

  public Map<byte[], List<Integer>> getOriginalIndex() {
    return originalIndex;
  }

  /**
   * Add an Action to this container based on it's regionName. If the regionName
   * is wrong, the initial execution will fail, but will be automatically
   * retried after looking up the correct region.
   *
   * @param regionName
   * @param row
   */
  public void addGet(byte[] regionName, Get row, int index) {
    if (gets == null)
        gets = new TreeMap<byte[], List<Get>>(Bytes.BYTES_COMPARATOR);
    add(regionName, (Get)row, gets);

    if (originalIndex == null)
        originalIndex = new TreeMap<byte[], List<Integer>>(Bytes.BYTES_COMPARATOR);
    add(regionName, new Integer(index), originalIndex);
  }

  public void mutate(byte[] regionName, Mutation row) {
    if(row instanceof Put) {
      if (puts == null)
        puts = new TreeMap<byte[], List<Put>>(Bytes.BYTES_COMPARATOR);
      add(regionName, (Put)row, puts);
    } else if(row instanceof Delete) {
      if (deletes == null)
        deletes = new TreeMap<byte[], List<Delete>>(Bytes.BYTES_COMPARATOR);
      add(regionName, (Delete)row, deletes);
    }
  }

  private <R> void add(byte[] regionName, R a, Map<byte[], List<R>> map) {
    List<R> rsActions = map.get(regionName);
    if (rsActions == null) {
      rsActions = new ArrayList<R>();
      map.put(regionName, rsActions);
    }
    rsActions.add((R)a);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION_0);
    writeMap(out, gets);
    writeMap(out, puts);
    writeMap(out, deletes);
  }

  private <R extends Row> void writeMap(DataOutput out, Map<byte[], List<R>> map) throws IOException {
    if (map == null) {
      out.writeInt(0);
      return;
    }

    out.writeInt(map.size());
    for (Map.Entry<byte[], List<R>> e : map.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      List<R> lst = e.getValue();
      out.writeInt(lst.size());
      for (R a : lst) {
        HbaseObjectWritable.writeObject(out, a, a.getClass(), null);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version != VERSION_0) throw new RuntimeException("Unsupported Version");

    gets = readMap(in);
    puts = readMap(in);
    deletes = readMap(in);
  }

  private <R extends Row> Map<byte[], List<R>> readMap(DataInput in) throws IOException {
    int mapSize = in.readInt();

    if (mapSize == 0) return null;

    Map<byte[], List<R>> map = new TreeMap<byte[], List<R>>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);
      int listSize = in.readInt();
      List<R> lst = new ArrayList<R>(listSize);
      for (int j = 0; j < listSize; j++) {
        @SuppressWarnings("unchecked")
        R elem = (R) HbaseObjectWritable.readObject(in, null);
        lst.add(elem);
      }
      map.put(key, lst);
    }
    return map;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(deletes, puts, gets);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiAction other = (MultiAction) obj;
    if (deletes == null) {
      if (other.deletes != null)
        return false;
    } else if (!deletes.equals(other.deletes))
      return false;
    if (gets == null) {
      if (other.gets != null)
        return false;
    } else if (!gets.equals(other.gets))
      return false;
    if (puts == null) {
      if (other.puts != null)
        return false;
    } else if (!puts.equals(other.puts))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "MultiAction [gets=" + gets + ", puts=" + puts + ", deletes="
        + deletes + "]";
  }

}

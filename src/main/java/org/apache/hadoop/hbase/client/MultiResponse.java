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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * A container for Result objects, grouped by regionName.
 */
public class MultiResponse implements Writable {

  private static final int VERSION_0 = 0;
  // map of regionName to list of (Results paired to the original index for that
  // Result)
  private Map<byte[], Object> resultsForGet = null;
  private Map<byte[], Object> resultsForPut = null;
  private Map<byte[], Object> resultsForDelete = null;

  public MultiResponse() {
  }


  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param r
   *          First item in the pair is the original index of the Action
   *          (request). Second item is the Result. Result will be empty for
   *          successful Put and Delete actions.
   */
  public void addGetResponse(byte[] regionName, Object rs) {
    if (resultsForGet == null)
      resultsForGet = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
    resultsForGet.put(regionName, rs);
  }

  public void addPutResponse(byte[] regionName, Object result) {
    if (resultsForPut == null)
      resultsForPut = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
    resultsForPut.put(regionName, result);
  }

  public void addDeleteResponse(byte[] regionName, Object result) {
    if (resultsForDelete == null)
      resultsForDelete = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
    resultsForDelete.put(regionName, result);
  }

  public int getPutResult(byte []regionName) throws Exception {
    Object result = resultsForPut.get(regionName);
    if (result instanceof Exception) // false if result == null
      throw (Exception)result;

    return ((Integer)result).intValue();
  }

  public int getDeleteResult(byte []regionName) throws Exception {
    Object result = resultsForDelete.get(regionName);
    if (result instanceof Exception) // false if result == null
      throw (Exception)result;

    return ((Integer)result).intValue();
  }

  public Result[] getGetResult(byte []regionName) throws Exception {
    Object result = resultsForGet.get(regionName);
    if (result instanceof Exception) // false if result == null
      throw (Exception)result;

    return ((Result[])result);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION_0);
    writeMap(out, this.resultsForGet);
    writeMap(out, this.resultsForPut);
    writeMap(out, this.resultsForDelete);
  }

  private void writeMap(DataOutput out, Map<byte[], Object> map) throws IOException {
    if (map == null) {
      out.writeInt(0);
      return;
    }

    out.writeInt(map.size());
    for (Map.Entry<byte[], Object> e : map.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());

      Object result = e.getValue();
      HbaseObjectWritable.writeObject(out, result, result.getClass(), null);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version != VERSION_0) throw new RuntimeException("Unsupported Version");

    this.resultsForGet = readMap(in);
    this.resultsForPut = readMap(in);
    this.resultsForDelete = readMap(in);
  }

  private Map<byte[], Object> readMap(DataInput in) throws IOException {
    int mapSize = in.readInt();

    if (mapSize == 0) return null;

    Map<byte[], Object> map = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < mapSize; i++) {
      byte[] key = Bytes.readByteArray(in);
      Object value = HbaseObjectWritable.readObject(in, null);
      map.put(key, value);
    }
    return map;
  }

}

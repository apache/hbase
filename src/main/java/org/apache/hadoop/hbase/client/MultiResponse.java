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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

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

  public MultiResponse(Map<byte[], Object> resultsForGet,
      Map<byte[], Object> resultsForPut,
      Map<byte[], Object> resultsForDelete) {
    // TODO @gauravm: Change from copy to direct assignment.
    if (resultsForGet != null) {
      this.resultsForGet = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.resultsForGet.putAll(resultsForGet);
    } else {
      this.resultsForGet = null;
    }

    if (resultsForPut != null) {
      this.resultsForPut = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.resultsForPut.putAll(resultsForPut);
    } else {
      this.resultsForPut = null;
    }

    if (resultsForDelete != null) {
      this.resultsForDelete = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.resultsForDelete.putAll(resultsForDelete);
    } else {
      this.resultsForDelete = null;
    }
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName - name of the region
   * @param rs - can be Integer, Result[] or Exception - if the
   * call was not successful.
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

  public Result[] getGetResult(byte[] regionName) throws Exception {
    Object result = resultsForGet.get(regionName);
    if (result instanceof Exception) // false if result == null
      throw (Exception) result;

    return ((Result[]) result);
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((resultsForDelete == null) ? 0 : resultsForDelete.hashCode());
    result = prime * result
        + ((resultsForGet == null) ? 0 : resultsForGet.hashCode());
    result = prime * result
        + ((resultsForPut == null) ? 0 : resultsForPut.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiResponse other = (MultiResponse) obj;
    if (resultsForDelete == null) {
      if (other.resultsForDelete != null)
        return false;
    } else if (!resultsForDelete.equals(other.resultsForDelete))
      return false;
    if (resultsForGet == null) {
      if (other.resultsForGet != null)
        return false;
    } else if (!resultsForGet.equals(other.resultsForGet))
      return false;
    if (resultsForPut == null) {
      if (other.resultsForPut != null)
        return false;
    } else if (!resultsForPut.equals(other.resultsForPut))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "MultiResponse [resultsForGet=" + resultsForGet + ", resultsForPut="
        + resultsForPut + ", resultsForDelete=" + resultsForDelete + "]";
  }

  public Map<byte[], Object> getResultsForGet() {
    return resultsForGet;
  }

  public Map<byte[], Object> getResultsForPut() {
    return resultsForPut;
  }

  public Map<byte[], Object> getResultsForDelete() {
    return resultsForDelete;
  }

  public static class Builder {

    public static MultiResponse createFromTMultiResponse(
        TMultiResponse tMultiResponse) {
      Map<byte[], IntegerOrResultOrException> resultsForGet = tMultiResponse
          .getResultsForGet();
      Map<byte[], IntegerOrResultOrException> resutsForPut = tMultiResponse
          .getResultsForPut();
      Map<byte[], IntegerOrResultOrException> resultsForDelete = tMultiResponse
          .getResultsForDelete();
      return new MultiResponse(transformTmultiResponseMap(resultsForGet),
          transformTmultiResponseMap(resutsForPut),
          transformTmultiResponseMap(resultsForDelete));
    }

    public static Map<byte[], Object> transformTmultiResponseMap(
        Map<byte[], IntegerOrResultOrException> map) {
      if (map == null) {
        return null;
      }
      Map<byte[], Object> resultMap = new TreeMap<byte[], Object>(
          Bytes.BYTES_COMPARATOR);
      for (Entry<byte[], IntegerOrResultOrException> entry : map.entrySet()) {
        resultMap.put(entry.getKey(), IntegerOrResultOrException
            .createObjectFromIntegerOrResultOrException(entry.getValue()));
      }
      return resultMap;
    }
  }
}

/*
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.IntegerOrResultOrException.Type;
import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

/**
 * A container for Result objects, grouped by regionName. Uses lazy
 * Initialization of the three containers that it contains: resultsForGet,
 * resultsForPut and resultsForDelete, since it is very likely that the
 * MultiResponse object will contain only data for one of them
 */

@ThriftStruct
public class TMultiResponse {
  // map of regionName to list of (Results paired to the original index for that
  // Result)
  private Map<byte[], IntegerOrResultOrException> resultsForGet = null;
  private Map<byte[], IntegerOrResultOrException> resultsForPut = null;
  private Map<byte[], IntegerOrResultOrException> resultsForDelete = null;

  public TMultiResponse() {
  }

  /**
   * Thrift Constructor for MultiResponse serialization
   *
   * @param resultsForGet
   * @param resultsForPut
   * @param resultsForDelete
   */
  @ThriftConstructor
  public TMultiResponse(@ThriftField(1) Map<byte[], IntegerOrResultOrException> resultsForGet,
      @ThriftField(2) Map<byte[], IntegerOrResultOrException> resultsForPut,
      @ThriftField(3) Map<byte[], IntegerOrResultOrException> resultsForDelete) {
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
      resultsForPut = null;
    }
    if (resultsForDelete != null) {
      this.resultsForDelete = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      this.resultsForDelete.putAll(resultsForDelete);
    } else {
      resultsForDelete = null;
    }
  }


  @ThriftField(1)
  public Map<byte[], IntegerOrResultOrException> getResultsForGet() {
    return resultsForGet;
  }

  @ThriftField(2)
  public Map<byte[], IntegerOrResultOrException> getResultsForPut() {
    return resultsForPut;
  }

  @ThriftField(3)
  public Map<byte[], IntegerOrResultOrException> getResultsForDelete() {
    return resultsForDelete;
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName - name of the region in bytes
   * @param result - Can be be either Exception (if the operation was not
   * successful or Result)
   *
   */
  public void addGetResponse(byte[] regionName,
      IntegerOrResultOrException result) {
    if (resultsForGet == null)
      resultsForGet = new TreeMap<byte[], IntegerOrResultOrException>(
          Bytes.BYTES_COMPARATOR);
    resultsForGet.put(regionName, result);
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName - name of the region in bytes
   * @param result - Can be be either Exception (if the operation was not
   * successful or Integer otherwise)
   *
   */
  public void addPutResponse(byte[] regionName,
      IntegerOrResultOrException result) {
    if (resultsForPut == null)
      resultsForPut = new TreeMap<byte[], IntegerOrResultOrException>(
          Bytes.BYTES_COMPARATOR);
    resultsForPut.put(regionName, result);
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName - name of the region in bytes
   * @param result - Can be be either Exception (if the operation was not
   * successful or Integer otherwise)
   *
   */
  public void addDeleteResponse(byte[] regionName,
      IntegerOrResultOrException result) {
    if (resultsForDelete == null)
      resultsForDelete = new TreeMap<byte[], IntegerOrResultOrException>(
          Bytes.BYTES_COMPARATOR);
    resultsForDelete.put(regionName, result);
  }

  /**
   *
   * @param regionName
   * @return
   * @throws Exception
   */
  public int getPutResult(byte[] regionName) throws Exception {
    IntegerOrResultOrException result = resultsForPut.get(regionName);
    if (result.getType() == Type.EXCEPTION)
      throw result.getEx();
    return result.getInteger();
  }

  public int getDeleteResult(byte[] regionName) throws Exception {
    IntegerOrResultOrException result = resultsForDelete.get(regionName);
    if (result.getType() == Type.EXCEPTION)
      throw result.getEx();
    return result.getInteger();
  }

  public List<Result> getGetResult(byte[] regionName) throws Exception {
    IntegerOrResultOrException result = resultsForGet.get(regionName);
    if (result.getType() == Type.EXCEPTION)
      throw result.getEx();
    return result.getResults();
  }

  /**
   * Puts entries from map of <byte[], Object> to map of <byte[],
   * IntegerOrResultOrException>
   *
   */
  public static Map<byte[], IntegerOrResultOrException> fromObjectsMapToIOROEMap(
      Map<byte[], Object> originalMap) {
    if (originalMap == null) {
      return null;
    } else {
      Map<byte[], IntegerOrResultOrException> map = new TreeMap<>(
          Bytes.BYTES_COMPARATOR);
      for (Entry<byte[], Object> e : originalMap.entrySet()) {
        map.put(e.getKey(),
            IntegerOrResultOrException.createFromObject(e.getValue()));
      }
      return map;
    }
  }

  @Override
  public String toString() {
    return "MultiResponse [resultsForGet=" + resultsForGet + ", resultsForPut="
        + resultsForPut + ", resultsForDelete=" + resultsForDelete + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(resultsForDelete, resultsForGet, resultsForPut);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TMultiResponse other = (TMultiResponse) obj;
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

  public static class Builder {
    public static TMultiResponse createFromMultiResponse(
        MultiResponse multiResponse) {
      Map<byte[], Object> resForPut = multiResponse.getResultsForPut();
      Map<byte[], Object> resForGet = multiResponse.getResultsForGet();
      Map<byte[], Object> resForDelete = multiResponse.getResultsForDelete();
      return new TMultiResponse(fromObjectsMapToIOROEMap(resForGet),
          fromObjectsMapToIOROEMap(resForPut), fromObjectsMapToIOROEMap(resForDelete));
    }
  }
}

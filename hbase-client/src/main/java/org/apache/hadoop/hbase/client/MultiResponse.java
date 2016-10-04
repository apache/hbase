/*
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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A container for Result objects, grouped by regionName.
 */
@InterfaceAudience.Private
public class MultiResponse extends AbstractResponse {

  // map of regionName to map of Results by the original index for that Result
  private Map<byte[], RegionResult> results = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * The server can send us a failure for the region itself, instead of individual failure.
   * It's a part of the protobuf definition.
   */
  private Map<byte[], Throwable> exceptions =
      new TreeMap<byte[], Throwable>(Bytes.BYTES_COMPARATOR);

  public MultiResponse() {
    super();
  }

  /**
   * @return Number of pairs in this container
   */
  public int size() {
    int size = 0;
    for (RegionResult result: results.values()) {
      size += result.size();
    }
    return size;
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param originalIndex the original index of the Action (request).
   * @param resOrEx the result or error; will be empty for successful Put and Delete actions.
   */
  public void add(byte[] regionName, int originalIndex, Object resOrEx) {
    getResult(regionName).addResult(originalIndex, resOrEx);
  }

  public void addException(byte []regionName, Throwable ie){
    exceptions.put(regionName, ie);
  }

  /**
   * @return the exception for the region, if any. Null otherwise.
   */
  public Throwable getException(byte []regionName){
    return exceptions.get(regionName);
  }

  public Map<byte[], Throwable> getExceptions() {
    return exceptions;
  }

  public void addStatistic(byte[] regionName, ClientProtos.RegionLoadStats stat) {
    getResult(regionName).setStat(stat);
  }

  private RegionResult getResult(byte[] region){
    RegionResult rs = results.get(region);
    if (rs == null) {
      rs = new RegionResult();
      results.put(region, rs);
    }
    return rs;
  }

  public Map<byte[], RegionResult> getResults(){
    return this.results;
  }

  @Override
  public ResponseType type() {
    return ResponseType.MULTI;
  }

  static class RegionResult{
    Map<Integer, Object> result = new HashMap<>();
    ClientProtos.RegionLoadStats stat;

    public void addResult(int index, Object result){
      this.result.put(index, result);
    }

    public void setStat(ClientProtos.RegionLoadStats stat){
      this.stat = stat;
    }

    public int size() {
      return this.result.size();
    }

    public ClientProtos.RegionLoadStats getStat() {
      return this.stat;
    }
  }
}

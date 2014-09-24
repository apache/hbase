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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * A container for Result objects, grouped by regionName.
 */
@InterfaceAudience.Private
public class MultiResponse {

  // map of regionName to map of Results by the original index for that Result
  private Map<byte[], Map<Integer, Object>> results =
      new TreeMap<byte[], Map<Integer, Object>>(Bytes.BYTES_COMPARATOR);

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
    for (Map<?,?> c : results.values()) {
      size += c.size();
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
    Map<Integer, Object> rs = results.get(regionName);
    if (rs == null) {
      rs = new HashMap<Integer, Object>();
      results.put(regionName, rs);
    }
    rs.put(originalIndex, resOrEx);
  }

  public Map<byte[], Map<Integer, Object>> getResults() {
    return results;
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
}

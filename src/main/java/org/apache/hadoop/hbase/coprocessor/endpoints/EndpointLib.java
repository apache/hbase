/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

/**
 * Some algorithm library for implementing endpoints.
 *
 * Current implemented:
 * aggregateScan aggregate key-values defined by a Scan
 */
public class EndpointLib {
  /**
   * An interface for aggregateScan
   */
  public interface IAggregator {
    void aggregate(KeyValue kv);
  }

  /**
   * Aggregates all KeyValue's in a region defined by a Scan.
   */
  public static void aggregateScan(HRegion region, Scan scan, IAggregator aggr)
      throws IOException {
    try (InternalScanner scanner = region.getScanner(scan)) {
      ArrayList<KeyValue> kvs = new ArrayList<>();
      boolean hasMore = true;
      while (hasMore) {
        kvs.clear();
        hasMore = scanner.next(kvs);
        for (KeyValue kv : kvs) {
          aggr.aggregate(kv);
        }
      }
    }
  }

}

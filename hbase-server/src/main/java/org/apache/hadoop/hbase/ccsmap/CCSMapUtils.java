/*
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

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public final class CCSMapUtils {

  private CCSMapUtils() {}

  static final long FOUR_BYTES_MARK = 0xFFFFFFFF;

  static final String CHUNK_CAPACITY_KEY = "hbase.regionserver.memstore.ccsmap.capacity";

  static final String CHUNK_SIZE_KEY = "hbase.regionserver.memstore.ccsmap.chunksize";

  static final String INITIAL_CHUNK_COUNT_KEY = "hbase.regionserver.memstore.ccsmap.chunk.initial";

  static final String USE_OFFHEAP = "hbase.regionserver.memstore.ccsmap.useoffheap";

}

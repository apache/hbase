/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;

/**
 * An index interface.
 */
public interface IdxIndex extends HeapSize {

  /**
   * Looks up an object. returns only exact matches.
   *
   * @param probe the probe to lookup
   * @return the result set
   */
  IntSet lookup(byte[] probe);

  /**
   * Gets all hte objects which are greater (or greater equal) than the probe.
   *
   * @param probe     the probe to lookup
   * @param inclusive if greater equal
   * @return the result set
   */
  IntSet tail(byte[] probe, boolean inclusive);

  /**
   * Gets all hte objects which are lesser (or lesser equal) than the probe.
   *
   * @param probe     the probe to lookup
   * @param inclusive if greater equal
   * @return the result set
   */
  IntSet head(byte[] probe, boolean inclusive);

  /**
   * Finds all the results which match any key in this index.
   *
   * @return all the ids in this index.
   */
  IntSet all();

  /**
   * Returns a string representation of the provided bytes probe.
   *
   * @param bytes the bytes
   * @return the string representation
   */
  String probeToString(byte[] bytes);

  /**
   * The number of entries in the index.
   *
   * @return the number of entries in the index
   */
  int size();
}

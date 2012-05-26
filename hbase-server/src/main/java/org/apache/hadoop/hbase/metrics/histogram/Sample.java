/**
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
package org.apache.hadoop.hbase.metrics.histogram;

/**
 * A statistically representative sample of items from a stream.
 */
public interface Sample {
  /**
   * Clears all recorded values.
   */
  void clear();

  /**
   * Returns the number of values recorded.
   *
   * @return the number of values recorded
   */
  int size();

  /**
   * Adds a new recorded value to the sample.
   *
   * @param value a new recorded value
   */
  void update(long value);

  /**
   * Returns a snapshot of the sample's values.
   *
   * @return a snapshot of the sample's values
   */
  Snapshot getSnapshot();
}

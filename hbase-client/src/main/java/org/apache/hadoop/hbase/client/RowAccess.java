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
package org.apache.hadoop.hbase.client;


import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Provide a way to access the inner buffer.
 * The purpose is to reduce the elapsed time to move a large number
 * of elements between collections.
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@VisibleForTesting
interface RowAccess<T> extends Iterable<T> {
  /**
   * @return true if there are no elements.
   */
  boolean isEmpty();

  /**
   * @return the number of elements in this list.
   */
  int size();
}

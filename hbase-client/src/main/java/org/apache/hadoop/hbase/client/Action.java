/**
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;

/**
 * A Get, Put, Increment, Append, or Delete associated with it's region.  Used internally by  
 * {@link HTable#batch} to associate the action with it's region and maintain
 * the index from the original request. 
 */
@InterfaceAudience.Private
public class Action<R> implements Comparable<R> {
  // TODO: This class should not be visible outside of the client package.
  private Row action;
  private int originalIndex;
  private long nonce = HConstants.NO_NONCE;

  public Action(Row action, int originalIndex) {
    super();
    this.action = action;
    this.originalIndex = originalIndex;    
  }

  public void setNonce(long nonce) {
    this.nonce = nonce;
  }

  public boolean hasNonce() {
    return nonce != HConstants.NO_NONCE;
  }

  public Row getAction() {
    return action;
  }

  public int getOriginalIndex() {
    return originalIndex;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compareTo(Object o) {
    return action.compareTo(((Action) o).getAction());
  }

  @Override
  public int hashCode() {
    return this.action.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    Action<?> other = (Action<?>) obj;
    return compareTo(other) == 0;
  }

  public long getNonce() {
    return nonce;
  }
}

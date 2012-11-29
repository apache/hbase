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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A Get, Put or Delete associated with it's region.  Used internally by  
 * {@link HTable::batch} to associate the action with it's region and maintain 
 * the index from the original request. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Action<R> implements Comparable {

  private Row action;
  private int originalIndex;
  private R result;

  /*
   * This constructor is replaced by {@link #Action(Row, int)}
   */
  @Deprecated
  public Action(byte[] regionName, Row action, int originalIndex) {
    this(action, originalIndex);
  }

  public Action(Row action, int originalIndex) {
    super();
    this.action = action;
    this.originalIndex = originalIndex;    
  }
  
  @Deprecated
  public byte[] getRegionName() {
    return null;
  }

  @Deprecated
  public void setRegionName(byte[] regionName) {
  }

  public R getResult() {
    return result;
  }

  public void setResult(R result) {
    this.result = result;
  }

  public Row getAction() {
    return action;
  }

  public int getOriginalIndex() {
    return originalIndex;
  }

  @Override
  public int compareTo(Object o) {
    return action.compareTo(((Action) o).getAction());
  }
}

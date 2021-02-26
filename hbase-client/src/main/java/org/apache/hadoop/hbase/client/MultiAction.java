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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Container for Actions (i.e. Get, Delete, or Put), which are grouped by
 * regionName. Intended to be used with {@link AsyncProcess}.
 */
@InterfaceAudience.Private
public final class MultiAction {
  // TODO: This class should not be visible outside of the client package.

  // map of regions to lists of puts/gets/deletes for that region.
  protected Map<byte[], List<Action>> actions = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private long nonceGroup = HConstants.NO_NONCE;

  public MultiAction() {
    super();
  }

  /**
   * Get the total number of Actions
   *
   * @return total number of Actions for all groups in this container.
   */
  public int size() {
    int size = 0;
    for (List<?> l : actions.values()) {
      size += l.size();
    }
    return size;
  }

  /**
   * Add an Action to this container based on it's regionName. If the regionName
   * is wrong, the initial execution will fail, but will be automatically
   * retried after looking up the correct region.
   *
   * @param regionName
   * @param a
   */
  public void add(byte[] regionName, Action a) {
    add(regionName, Collections.singletonList(a));
  }

  /**
   * Add an Action to this container based on it's regionName. If the regionName
   * is wrong, the initial execution will fail, but will be automatically
   * retried after looking up the correct region.
   *
   * @param regionName
   * @param actionList list of actions to add for the region
   */
  public void add(byte[] regionName, List<Action> actionList){
    List<Action> rsActions = actions.get(regionName);
    if (rsActions == null) {
      rsActions = new ArrayList<>(actionList.size());
      actions.put(regionName, rsActions);
    }
    rsActions.addAll(actionList);
  }

  public void setNonceGroup(long nonceGroup) {
    this.nonceGroup = nonceGroup;
  }

  public Set<byte[]> getRegions() {
    return actions.keySet();
  }

  public boolean hasNonceGroup() {
    return nonceGroup != HConstants.NO_NONCE;
  }

  public long getNonceGroup() {
    return this.nonceGroup;
  }

  // returns the max priority of all the actions
  public int getPriority() {
    Optional<Action> result = actions.values().stream().flatMap(List::stream)
        .max((action1, action2) -> Math.max(action1.getPriority(), action2.getPriority()));
    return result.isPresent() ? result.get().getPriority() : HConstants.PRIORITY_UNSET;
  }
}

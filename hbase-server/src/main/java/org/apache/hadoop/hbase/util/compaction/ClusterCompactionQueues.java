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
package org.apache.hadoop.hbase.util.compaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
class ClusterCompactionQueues {

  private final Map<ServerName, List<MajorCompactionRequest>> compactionQueues;
  private final Set<ServerName> compactingServers;
  private final ReadWriteLock lock;
  private final int concurrentServers;

  ClusterCompactionQueues(int concurrentServers) {
    this.concurrentServers = concurrentServers;

    this.compactionQueues = Maps.newHashMap();
    this.lock = new ReentrantReadWriteLock();
    this.compactingServers = Sets.newHashSet();
  }

  void addToCompactionQueue(ServerName serverName, MajorCompactionRequest info) {
    this.lock.writeLock().lock();
    try {
      List<MajorCompactionRequest> result = this.compactionQueues.get(serverName);
      if (result == null) {
        result = Lists.newArrayList();
        compactionQueues.put(serverName, result);
      }
      result.add(info);
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  boolean hasWorkItems() {
    lock.readLock().lock();
    try {
      return !this.compactionQueues.values().stream().allMatch(List::isEmpty);
    } finally {
      lock.readLock().unlock();
    }
  }

  int getCompactionRequestsLeftToFinish() {
    lock.readLock().lock();
    try {
      int size = 0;
      for (List<MajorCompactionRequest> queue : compactionQueues.values()) {
        size += queue.size();
      }
      return size;
    } finally {
      lock.readLock().unlock();
    }
  }

  List<MajorCompactionRequest> getQueue(ServerName serverName) {
    lock.readLock().lock();
    try {
      return compactionQueues.get(serverName);
    } finally {
      lock.readLock().unlock();
    }
  }

  MajorCompactionRequest reserveForCompaction(ServerName serverName) {
    lock.writeLock().lock();
    try {
      if (!compactionQueues.get(serverName).isEmpty()) {
        compactingServers.add(serverName);
        return compactionQueues.get(serverName).remove(0);
      }
      return null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  void releaseCompaction(ServerName serverName) {
    lock.writeLock().lock();
    try {
      compactingServers.remove(serverName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  boolean atCapacity() {
    lock.readLock().lock();
    try {
      return compactingServers.size() >= concurrentServers;
    } finally {
      lock.readLock().unlock();
    }
  }

  Optional<ServerName> getLargestQueueFromServersNotCompacting() {
    lock.readLock().lock();
    try {
      return compactionQueues.entrySet().stream()
          .filter(entry -> !compactingServers.contains(entry.getKey()))
          .max(Map.Entry.comparingByValue(
            (o1, o2) -> Integer.compare(o1.size(), o2.size()))).map(Map.Entry::getKey);
    } finally {
      lock.readLock().unlock();
    }
  }

}

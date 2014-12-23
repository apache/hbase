package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.consensus.client.FetchTask;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Iterate through a list of peers' committed logs and create fetch plan
 * in round robin fashion. It guarantees coverage of maximum index range on all
 * peers, but log file selection is not optimal. Seeking less overlaps or local
 * peer preference etc. are not taken into account.
 */
public class LogFileInfoIterator implements LogFetchPlanCreator {

  private Map<String, FetchTask> tasks;
  private long minIndex;

  /**
   * Hold lists of log files for each peer. In the pair, string is the peer's address.
   * Iterator is used to keep position on each list.
   */
  private LinkedList<Pair<String, Iterator<LogFileInfo>>> iterators = new LinkedList<>();

  public LogFileInfoIterator(List<Pair<String, List<LogFileInfo>>> statuses, long minIndex) {
    this.minIndex = minIndex;

    tasks = new HashMap<>(statuses.size());
    for (Pair<String, List<LogFileInfo>> status : statuses) {
      Collections.sort(status.getSecond());
      String peerAddress = status.getFirst();

      // Create a FetchTask for each peer. Don't create too many download requests.
      FetchTask task = new FetchTask(peerAddress);
      tasks.put(peerAddress, task);

      iterators.add(new Pair<>(peerAddress, status.getSecond().iterator()));
    }
  }

  @Override
  public Collection<FetchTask> createFetchTasks() {
    Pair<String, LogFileInfo> pair;

    // In each loop, take out a log file that contains the target index
    // and add it to the corresponding fetch task. Then we increase current
    // target index to < largest log index + 1 > as the next
    // target index.
    while ((pair = next(minIndex)) != null) {
      LogFileInfo info = pair.getSecond();
      minIndex = info.getLastIndex() + 1;
      tasks.get(pair.getFirst()).addTask(info);
    }

    return tasks.values();
  }

  /**
   * Iterate through sorted log file lists. Try to take a file from a
   * peer at the same time and do round robin within peers. The file's
   * index range has to cover targetIndex.
   *
   * Basic logic:
   * 1. Take an iterator of log file list of one peer
   * 2. Try to find a log file that covers the target index
   * 3. If the peer does not have any file that covers target index, discard its iterator
   *    If a log file qualifies, return it with the peer's address
   * 4. If there is possibility to find another targetIndex, put the iterator
   *    to the end of the list. So peers are queried in a rotation.
   *
   * @param targetIndex the index that has to be in the result file
   * @return log file that contained target index and the peer it belongs to.
   *         null if no matching file is found.
   */
  private Pair<String, LogFileInfo> next(long targetIndex) {
    while (true) {
      // Select the peer at list head
      Pair<String, Iterator<LogFileInfo>> pair = iterators.pollFirst();
      if (pair == null) {
        return null;
      }
      Iterator<LogFileInfo> it = pair.getSecond();
      while (it.hasNext()) {
        LogFileInfo info = it.next();
        if (targetIndex < info.getInitialIndex()) {
          // Current peer has missing logs? Discard it!
          break;
        }
        if (targetIndex > info.getLastIndex()) {
          // Target index is larger than the file's last index, so will try
          // next file on the same iterator.
          continue;
        }
        // Current log file contains the target index. Insert the iterator back
        // to list end and return current file.
        if (it.hasNext()) {
          iterators.addLast(pair);
        }
        return new Pair<>(pair.getFirst(), info);
      }
    }
  }
}

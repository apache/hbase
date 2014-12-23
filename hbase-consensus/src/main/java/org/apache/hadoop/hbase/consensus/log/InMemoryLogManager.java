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


import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.ImmutableRaftContext;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.BucketAllocatorException;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryLogManager implements CommitLogManagerInterface {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryLogManager.class);

  ArrayList<EditId> transactionList;
  Map<EditId, ByteBuffer> transactionMap;

  @Override
  public synchronized boolean append(EditId editId, long commitIndex,
                                     final ByteBuffer txns) {
    EditId cloneId = editId.clone();
    if (transactionList.isEmpty() ||
      (transactionList.get(transactionList.size() - 1).compareTo(editId) < 0)) {
      transactionList.add(cloneId);
      transactionMap.put(cloneId, txns);
      return true;
    }

    return false;
  }

  @Override
  public synchronized boolean isExist(EditId editId) {
    if (editId.getTerm() == 0) {
      return true;
    }
    return transactionMap.containsKey(editId);
  }

  @Override
  public synchronized boolean truncate(EditId editId) {
    if (editId.equals(TransactionLogManager.UNDEFINED_EDIT_ID)) {
      transactionMap.clear();
      transactionList.clear();
      return true;
    }
    if (!isExist(editId)) {
      return false;
    }

    if (editId.getTerm() == 0) {
      transactionList.clear();
      transactionMap.clear();
      return true;
    }

    // delete all the elements up to this editId
    int index = transactionList.indexOf(editId);
    while (transactionList.size() > (index + 1)) {
      EditId last = transactionList.remove(transactionList.size() - 1);
      transactionMap.remove(last);
    }

    return true;
  }

  private synchronized MemoryBuffer getTransaction(String sessionKey,
                                          EditId editId,
                                          final Arena arena) {
    final ByteBuffer entry = transactionMap.get(editId);
    MemoryBuffer buffer = null;

    if (entry != null) {
      try {
        buffer = arena.allocateByteBuffer(entry.remaining());
      } catch (CacheFullException | BucketAllocatorException e) {
        if (LOG.isTraceEnabled()) {
          LOG.error("Unable to allocate buffer from arena", e);
        }
      }
      buffer.getBuffer().put(entry.array(),
        entry.arrayOffset() + entry.position(), entry.remaining());
      buffer.flip();
    }
    return buffer;
  }

  @Override
  public synchronized EditId getPreviousEditID(EditId editId) {
    int index = transactionList.indexOf(editId);
    if (index > 0) {
      return transactionList.get(index - 1).clone();
    } else {
      return TransactionLogManager.UNDEFINED_EDIT_ID;
    }
  }

  /**
   * Used for hinting the leader, from where to catch up. This method tries to
   * locate a latest term for which there are at least two edits and then returns
   * the second last editid for that term.
   * @return EditId
   */
  public synchronized EditId getLastValidTransactionId() {
    int index = transactionList.size() - 1;

    for (int ii = index ; ii > 0; ii--) {
      if (transactionList.get(ii).getTerm() == transactionList.get(ii - 1).getTerm()) {
        return transactionList.get(ii - 1);
      }
    }
    return TransactionLogManager.UNDEFINED_EDIT_ID;
  }

  @Override
  public synchronized EditId getLastEditID() {
    if (transactionList.isEmpty()) {
      return TransactionLogManager.UNDEFINED_EDIT_ID;
    }
    return transactionList.get(transactionList.size()-1).clone();
  }

  @Override
  public long getFirstIndex() {
    if (transactionList != null) {
      return transactionList.get(0).getIndex();
    }
    return UNDEFINED_EDIT_ID.getIndex();
  }

  @Override
  public void initialize(ImmutableRaftContext context) {
    transactionList = new ArrayList<>();
    transactionMap = new HashMap<>();
  }

  @Override
  public boolean isAccessible() {
    return true;
  }

  private synchronized EditId getNextEditId(EditId currentId) {
    if (currentId.getTerm() == 0 && !transactionList.isEmpty()) {
      return transactionList.get(0).clone();
    }

    int index = transactionList.indexOf(currentId);

    if (++index < transactionList.size()) {
      return transactionList.get(index).clone();
    }
    return TransactionLogManager.UNDEFINED_EDIT_ID;
  }

  @Override
  public synchronized Pair<EditId, MemoryBuffer> getNextEditIdTransaction(
      final String  sessionKey,
      final long  currentIndex,
      final Arena arena
  ) throws IOException {
    EditId nextId = getNextEditId(getEditId(currentIndex));
    if (nextId == null || nextId.equals(TransactionLogManager.UNDEFINED_EDIT_ID)) {
      return null;
    } else {
      MemoryBuffer buffer = getTransaction(sessionKey, nextId, arena);
      return new Pair<>(nextId, buffer);
    }
  }

  @Override
  public String toString() {
    return dumpLogs(Integer.MAX_VALUE);
  }

  @Override
  public synchronized String dumpLogs(int n) {
    if (n <= 0) {
      n = transactionList.size();
    }

    return RaftUtil.listToString(transactionList
      .subList(Math.max(transactionList.size() - n, 0),
        transactionList.size() - 1));
  }

  @Override
  public synchronized LogState getLogState() {
    LogState logState = new LogState(null);
    logState.addUncommittedLogFile(new LogFileInfo("Dummy", 0,
        transactionList.get(0), transactionList.get(transactionList.size() - 1), 0, 0));

    return logState;
  }

  @Override
  public List<LogFileInfo> getCommittedLogStatus(long minIndex) {
    throw new NotImplementedException("InMemoryLogManager does not track committed log");
  }

  @Override
  public String getPath() {
    return "@RAM";
  }

  @Override public EditId getEditId(long index) {
    for (EditId edit : transactionList) {
      if (edit.getIndex() == index) {
        return edit;
      }
    }
    return UNDEFINED_EDIT_ID;
  }

  @Override
  public Pair<EditId, EditId> greedyIncorporateCandidateLogs(
      String    sessionKey,
      long      lastLogIndex
  ) {
    return null;
  }

  @Override
  public void fillLogGap(long seedIndex) throws IOException {

  }

  @Override
  public void stop() {
    // do nothing
  }
}

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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Manages the read/write consistency within memstore. This provides
 * an interface for readers to determine what entries to ignore, and
 * a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
@InterfaceAudience.Private
public class MultiVersionConsistencyControl {
  static final long NO_WRITE_NUMBER = 0;
  private volatile long memstoreRead = 0;
  private final Object readWaiters = new Object();

  // This is the pending queue of writes.
  private final LinkedHashSet<WriteEntry> writeQueue =
      new LinkedHashSet<WriteEntry>();

  /**
   * Default constructor. Initializes the memstoreRead/Write points to 0.
   */
  public MultiVersionConsistencyControl() {
  }

  /**
   * Initializes the memstoreRead/Write points appropriately.
   * @param startPoint
   */
  public void initialize(long startPoint) {
    synchronized (writeQueue) {
      writeQueue.clear();
      memstoreRead = startPoint;
    }
  }

  /**
   *
   * @param initVal The value we used initially and expected it'll be reset later
   * @return WriteEntry instance.
   */
  WriteEntry beginMemstoreInsert() {
    return beginMemstoreInsertWithSeqNum(NO_WRITE_NUMBER);
  }

  /**
   * Get a mvcc write number before an actual one(its log sequence Id) being assigned
   * @param sequenceId
   * @return long a faked write number which is bigger enough not to be seen by others before a real
   *         one is assigned
   */
  public static long getPreAssignedWriteNumber(AtomicLong sequenceId) {
    // the 1 billion is just an arbitrary big number to guard no scanner will reach it before
    // current MVCC completes. Theoretically the bump only needs to be 2 * the number of handlers
    // because each handler could increment sequence num twice and max concurrent in-flight
    // transactions is the number of RPC handlers.
    // We can't use Long.MAX_VALUE because we still want to maintain the ordering when multiple
    // changes touch same row key.
    // If for any reason, the bumped value isn't reset due to failure situations, we'll reset
    // curSeqNum to NO_WRITE_NUMBER in order NOT to advance memstore read point at all.
    // St.Ack 20150901 Where is the reset to NO_WRITE_NUMBER done?
    return sequenceId.incrementAndGet() + 1000000000;
  }

  /**
   * This function starts a MVCC transaction with current region's log change sequence number. Since
   * we set change sequence number when flushing current change to WAL(late binding), the flush
   * order may differ from the order to start a MVCC transaction. For example, a change begins a
   * MVCC firstly may complete later than a change which starts MVCC at a later time. Therefore, we
   * add a safe bumper to the passed in sequence number to start a MVCC so that no other concurrent
   * transactions will reuse the number till current MVCC completes(success or fail). The "faked"
   * big number is safe because we only need it to prevent current change being seen and the number
   * will be reset to real sequence number(set in log sync) right before we complete a MVCC in order
   * for MVCC to align with flush sequence.
   * @param curSeqNum
   * @return WriteEntry a WriteEntry instance with the passed in curSeqNum
   */
  public WriteEntry beginMemstoreInsertWithSeqNum(long curSeqNum) {
    return beginMemstoreInsertWithSeqNum(curSeqNum, false);
  }

  private WriteEntry beginMemstoreInsertWithSeqNum(long curSeqNum, boolean complete) {
    WriteEntry e = new WriteEntry(curSeqNum);
    if (complete) {
      e.markCompleted();
    }
    synchronized (writeQueue) {
      writeQueue.add(e);
      return e;
    }
  }

  /**
   * Complete a {@link WriteEntry} that was created by
   * {@link #beginMemstoreInsertWithSeqNum(long)}. At the end of this call, the global read
   * point is at least as large as the write point of the passed in WriteEntry. Thus, the write is
   * visible to MVCC readers.
   * @throws IOException
   */
  public void completeMemstoreInsertWithSeqNum(WriteEntry e, SequenceId seqId)
      throws IOException {
    if(e == null) return;
    if (seqId != null) {
      e.setWriteNumber(seqId.getSequenceId());
    } else {
      // set the value to NO_WRITE_NUMBER in order NOT to advance memstore readpoint inside
      // function beginMemstoreInsertWithSeqNum in case of failures
      e.setWriteNumber(NO_WRITE_NUMBER);
    }
    waitForPreviousTransactionsComplete(e);
  }

  /**
   * Cancel a write insert that failed.
   * Removes the write entry without advancing read point or without interfering with write
   * entries queued behind us. It is like #advanceMemstore(WriteEntry) only this method
   * will move the read point to the sequence id that is in WriteEntry even if it ridiculous (see
   * the trick in HRegion where we call {@link #getPreAssignedWriteNumber(AtomicLong)} just to mark
   * it as for special handling).
   * @param writeEntry Failed attempt at write. Does cleanup.
   */
  public void cancelMemstoreInsert(WriteEntry writeEntry) {
    // I'm not clear on how this voodoo all works but setting write number to -1 does NOT advance
    // readpoint and gets my little writeEntry completed and removed from queue of outstanding
    // events which seems right.  St.Ack 20150901.
    writeEntry.setWriteNumber(NO_WRITE_NUMBER);
    advanceMemstore(writeEntry);
  }

  /**
   * Complete a {@link WriteEntry} that was created by {@link #beginMemstoreInsert()}. At the
   * end of this call, the global read point is at least as large as the write point of the passed
   * in WriteEntry. Thus, the write is visible to MVCC readers.
   */
  public void completeMemstoreInsert(WriteEntry e) {
    waitForPreviousTransactionsComplete(e);
  }

  /**
   * Mark the {@link WriteEntry} as complete and advance the read point as
   * much as possible.
   *
   * How much is the read point advanced?
   * Let S be the set of all write numbers that are completed and where all previous write numbers
   * are also completed.  Then, the read point is advanced to the supremum of S.
   *
   * @param e
   * @return true if e is visible to MVCC readers (that is, readpoint >= e.writeNumber)
   */
  boolean advanceMemstore(WriteEntry e) {
    long nextReadValue = -1;
    synchronized (writeQueue) {
      e.markCompleted();

      while (!writeQueue.isEmpty()) {
        WriteEntry queueFirst = writeQueue.iterator().next();
        if (queueFirst.isCompleted()) {
          // Using Max because Edit complete in WAL sync order not arriving order
          nextReadValue = Math.max(nextReadValue, queueFirst.getWriteNumber());
          writeQueue.remove(queueFirst);
        } else {
          break;
        }
      }

      if (nextReadValue > memstoreRead) {
        memstoreRead = nextReadValue;
      }

      // notify waiters on writeQueue before return
      writeQueue.notifyAll();
    }

    if (nextReadValue > 0) {
      synchronized (readWaiters) {
        readWaiters.notifyAll();
      }
    }

    if (memstoreRead >= e.getWriteNumber()) {
      return true;
    }
    return false;
  }

  /**
   * Advances the current read point to be given seqNum if it is smaller than
   * that.
   */
  void advanceMemstoreReadPointIfNeeded(long seqNum) {
    synchronized (writeQueue) {
      if (this.memstoreRead < seqNum) {
        memstoreRead = seqNum;
      }
    }
  }

  /**
   * Wait for all previous MVCC transactions complete
   */
  public void waitForPreviousTransactionsComplete() {
    WriteEntry w = beginMemstoreInsertWithSeqNum(NO_WRITE_NUMBER, true);
    waitForPreviousTransactionsComplete(w);
  }

  public void waitForPreviousTransactionsComplete(WriteEntry waitedEntry) {
    boolean interrupted = false;
    WriteEntry w = waitedEntry;
    w.markCompleted();

    try {
      WriteEntry firstEntry = null;
      do {
        synchronized (writeQueue) {
          if (writeQueue.isEmpty()) {
            break;
          }
          firstEntry = writeQueue.iterator().next();
          if (firstEntry == w) {
            // all previous in-flight transactions are done
            break;
          }
          // WriteEntry already was removed from the queue by another handler
          if (!writeQueue.contains(w)) {
            break;
          }
          try {
            writeQueue.wait(0);
          } catch (InterruptedException ie) {
            // We were interrupted... finish the loop -- i.e. cleanup --and then
            // on our way out, reset the interrupt flag.
            interrupted = true;
            break;
          }
        }
      } while (firstEntry != null);
    } finally {
      advanceMemstore(w);
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public long memstoreReadPoint() {
    return memstoreRead;
  }

  public static class WriteEntry {
    private long writeNumber;
    private volatile boolean completed = false;

    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }
    void markCompleted() {
      this.completed = true;
    }
    boolean isCompleted() {
      return this.completed;
    }
    long getWriteNumber() {
      return this.writeNumber;
    }
    void setWriteNumber(long val){
      this.writeNumber = val;
    }
  }

  public static final long FIXED_SIZE = ClassSize.align(
      ClassSize.OBJECT +
      2 * Bytes.SIZEOF_LONG +
      2 * ClassSize.REFERENCE);

}

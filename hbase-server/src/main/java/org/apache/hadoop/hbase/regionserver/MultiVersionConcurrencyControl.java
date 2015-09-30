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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.mortbay.log.Log;

/**
 * Manages the read/write consistency. This provides an interface for readers to determine what
 * entries to ignore, and a mechanism for writers to obtain new write numbers, then "commit"
 * the new writes for readers to read (thus forming atomic transactions).
 */
@InterfaceAudience.Private
public class MultiVersionConcurrencyControl {
  final AtomicLong readPoint = new AtomicLong(0);
  final AtomicLong writePoint = new AtomicLong(0);
  private final Object readWaiters = new Object();
  /**
   * Represents no value, or not set.
   */
  public static final long NONE = -1;

  // This is the pending queue of writes.
  //
  // TODO(eclark): Should this be an array of fixed size to
  // reduce the number of allocations on the write path?
  // This could be equal to the number of handlers + a small number.
  // TODO: St.Ack 20150903 Sounds good to me.
  private final LinkedList<WriteEntry> writeQueue = new LinkedList<WriteEntry>();

  public MultiVersionConcurrencyControl() {
    super();
  }

  /**
   * Construct and set read point. Write point is uninitialized.
   */
  public MultiVersionConcurrencyControl(long startPoint) {
    tryAdvanceTo(startPoint, NONE);
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   * @param newStartPoint
   */
  public void advanceTo(long newStartPoint) {
    while (true) {
      long seqId = this.getWritePoint();
      if (seqId >= newStartPoint) break;
      if (this.tryAdvanceTo(/* newSeqId = */ newStartPoint, /* expected = */ seqId)) break;
    }
  }

  /**
   * Step the MVCC forward on to a new read/write basis.
   * @param newStartPoint Point to move read and write points to.
   * @param expected If not -1 (#NONE)
   * @return Returns false if <code>expected</code> is not equal to the
   * current <code>readPoint</code> or if <code>startPoint</code> is less than current
   * <code>readPoint</code>
   */
  boolean tryAdvanceTo(long newStartPoint, long expected) {
    synchronized (writeQueue) {
      long currentRead = this.readPoint.get();
      long currentWrite = this.writePoint.get();
      if (currentRead != currentWrite) {
        throw new RuntimeException("Already used this mvcc; currentRead=" + currentRead +
          ", currentWrite=" + currentWrite + "; too late to tryAdvanceTo");
      }
      if (expected != NONE && expected != currentRead) {
        return false;
      }

      if (newStartPoint < currentRead) {
        return false;
      }

      readPoint.set(newStartPoint);
      writePoint.set(newStartPoint);
    }
    return true;
  }

  /**
   * Start a write transaction. Create a new {@link WriteEntry} with a new write number and add it
   * to our queue of ongoing writes. Return this WriteEntry instance.
   * To complete the write transaction and wait for it to be visible, call
   * {@link #completeAndWait(WriteEntry)}. If the write failed, call
   * {@link #complete(WriteEntry)} so we can clean up AFTER removing ALL trace of the failed write
   * transaction.
   * @see #complete(WriteEntry)
   * @see #completeAndWait(WriteEntry)
   */
  public WriteEntry begin() {
    synchronized (writeQueue) {
      long nextWriteNumber = writePoint.incrementAndGet();
      WriteEntry e = new WriteEntry(nextWriteNumber);
      writeQueue.add(e);
      return e;
    }
  }

  /**
   * Wait until the read point catches up to the write point; i.e. wait on all outstanding mvccs
   * to complete.
   */
  public void await() {
    // Add a write and then wait on reads to catch up to it.
    completeAndWait(begin());
  }

  /**
   * Complete a {@link WriteEntry} that was created by {@link #begin()} then wait until the
   * read point catches up to our write.
   *
   * At the end of this call, the global read point is at least as large as the write point
   * of the passed in WriteEntry.  Thus, the write is visible to MVCC readers.
   */
  public void completeAndWait(WriteEntry e) {
    complete(e);
    waitForRead(e);
  }

  /**
   * Mark the {@link WriteEntry} as complete and advance the read point as much as possible.
   * Call this even if the write has FAILED (AFTER backing out the write transaction
   * changes completely) so we can clean up the outstanding transaction.
   *
   * How much is the read point advanced?
   * 
   * Let S be the set of all write numbers that are completed. Set the read point to the highest
   * numbered write of S.
   *
   * @param writeEntry
   *
   * @return true if e is visible to MVCC readers (that is, readpoint >= e.writeNumber)
   */
  public boolean complete(WriteEntry writeEntry) {
    synchronized (writeQueue) {
      writeEntry.markCompleted();

      long nextReadValue = NONE;
      boolean ranOnce = false;
      while (!writeQueue.isEmpty()) {
        ranOnce = true;
        WriteEntry queueFirst = writeQueue.getFirst();

        if (nextReadValue > 0) {
          if (nextReadValue + 1 != queueFirst.getWriteNumber()) {
            throw new RuntimeException("Invariant in complete violated, nextReadValue="
                + nextReadValue + ", writeNumber=" + queueFirst.getWriteNumber());
          }
        }

        if (queueFirst.isCompleted()) {
          nextReadValue = queueFirst.getWriteNumber();
          writeQueue.removeFirst();
        } else {
          break;
        }
      }

      if (!ranOnce) {
        throw new RuntimeException("There is no first!");
      }

      if (nextReadValue > 0) {
        synchronized (readWaiters) {
          readPoint.set(nextReadValue);
          readWaiters.notifyAll();
        }
      }
      return readPoint.get() >= writeEntry.getWriteNumber();
    }
  }

  /**
   * Wait for the global readPoint to advance up to the passed in write entry number.
   */
  void waitForRead(WriteEntry e) {
    boolean interrupted = false;
    int count = 0;
    synchronized (readWaiters) {
      while (readPoint.get() < e.getWriteNumber()) {
        if (count % 100 == 0 && count > 0) {
          Log.warn("STUCK: " + this);
        }
        count++;
        try {
          readWaiters.wait(10);
        } catch (InterruptedException ie) {
          // We were interrupted... finish the loop -- i.e. cleanup --and then
          // on our way out, reset the interrupt flag.
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  public String toString() {
    StringBuffer sb = new StringBuffer(256);
    sb.append("readPoint=");
    sb.append(this.readPoint.get());
    sb.append(", writePoint=");
    sb.append(this.writePoint);
    synchronized (this.writeQueue) {
      for (WriteEntry we: this.writeQueue) {
        sb.append(", [");
        sb.append(we);
        sb.append("]");
      }
    }
    return sb.toString();
  }

  public long getReadPoint() {
    return readPoint.get();
  }

  @VisibleForTesting
  public long getWritePoint() {
    return writePoint.get();
  }

  /**
   * Write number and whether write has completed given out at start of a write transaction.
   * Every created WriteEntry must be completed by calling mvcc#complete or #completeAndWait.
   */
  @InterfaceAudience.Private
  public static class WriteEntry {
    private final long writeNumber;
    private boolean completed = false;

    WriteEntry(long writeNumber) {
      this.writeNumber = writeNumber;
    }

    void markCompleted() {
      this.completed = true;
    }

    boolean isCompleted() {
      return this.completed;
    }

    public long getWriteNumber() {
      return this.writeNumber;
    }

    @Override
    public String toString() {
      return this.writeNumber + ", " + this.completed;
    }
  }

  public static final long FIXED_SIZE = ClassSize.align(
      ClassSize.OBJECT +
      2 * Bytes.SIZEOF_LONG +
      2 * ClassSize.REFERENCE);
}
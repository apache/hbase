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
package org.apache.hadoop.hbase.regionserver.wal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * A WAL Entry for {@link FSHLog} implementation.  Immutable.
 * A subclass of {@link HLog.Entry} that carries extra info across the ring buffer such as
 * region sequence id (we want to use this later, just before we write the WAL to ensure region
 * edits maintain order).  The extra info added here is not 'serialized' as part of the WALEdit
 * hence marked 'transient' to underline this fact.  It also adds mechanism so we can wait on
 * the assign of the region sequence id.  See {@link #stampRegionSequenceId()}.
 */
@InterfaceAudience.Private
class FSWALEntry extends HLog.Entry {
  // The below data members are denoted 'transient' just to highlight these are not persisted;
  // they are only in memory and held here while passing over the ring buffer.
  private final transient long sequence;
  private final transient AtomicLong regionSequenceIdReference;
  private final transient boolean inMemstore;
  private final transient HTableDescriptor htd;
  private final transient HRegionInfo hri;
  // Latch that is set on creation and then is undone on the other side of the ring buffer by the
  // consumer thread just after it sets the region edit/sequence id in here.
  private final transient CountDownLatch latch = new CountDownLatch(1);

  FSWALEntry(final long sequence, final HLogKey key, final WALEdit edit,
      final AtomicLong referenceToRegionSequenceId, final boolean inMemstore,
      final HTableDescriptor htd, final HRegionInfo hri) {
    super(key, edit);
    this.regionSequenceIdReference = referenceToRegionSequenceId;
    this.inMemstore = inMemstore;
    this.htd = htd;
    this.hri = hri;
    this.sequence = sequence;
  }

  public String toString() {
    return "sequence=" + this.sequence + ", " + super.toString();
  };

  boolean isInMemstore() {
    return this.inMemstore;
  }

  HTableDescriptor getHTableDescriptor() {
    return this.htd;
  }

  HRegionInfo getHRegionInfo() {
    return this.hri;
  }

  /**
   * @return The sequence on the ring buffer when this edit was added.
   */
  long getSequence() {
    return this.sequence;
  }

  /**
   * Stamp this edit with a region edit/sequence id.
   * Call when safe to do so: i.e. the context is such that the increment on the passed in
   * {@link #regionSequenceIdReference} is guaranteed aligned w/ how appends are going into the
   * WAL.  This method works with {@link #getRegionSequenceId()}.  It will block waiting on this
   * method to be called.
   * @return The region edit/sequence id we set for this edit.
   * @see #getRegionSequenceId()
   */
  long stampRegionSequenceId() {
    long regionSequenceId = this.regionSequenceIdReference.incrementAndGet();
    getKey().setLogSeqNum(regionSequenceId);
    // On creation, a latch was set.  Count it down when sequence id is set.  This will free
    // up anyone blocked on {@link #getRegionSequenceId()}
    this.latch.countDown();
    return regionSequenceId;
  }

  long getRegionSequenceId() throws InterruptedException {
    this.latch.await();
    return getKey().getLogSeqNum();
  }
}

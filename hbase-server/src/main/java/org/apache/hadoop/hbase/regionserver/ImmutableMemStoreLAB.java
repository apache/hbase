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
package org.apache.hadoop.hbase.regionserver;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A MemStoreLAB implementation which wraps N MemStoreLABs. Its main duty is in proper managing the
 * close of the individual MemStoreLAB. This is treated as an immutable one and so do not allow to
 * add any more Cells into it. {@link #copyCellInto(Cell)} throws Exception
 */
@InterfaceAudience.Private
public class ImmutableMemStoreLAB implements MemStoreLAB {

  private final AtomicInteger openScannerCount = new AtomicInteger();
  private volatile boolean closed = false;

  private final List<MemStoreLAB> mslabs;

  public ImmutableMemStoreLAB(List<MemStoreLAB> mslabs) {
    this.mslabs = mslabs;
  }

  @Override
  public Cell copyCellInto(Cell cell) {
    throw new IllegalStateException("This is an Immutable MemStoreLAB.");
  }

  @Override
  // returning a new chunk, without replacing current chunk,
  // the space on this chunk will be allocated externally
  // use the first MemStoreLABImpl in the list
  public Chunk getNewExternalChunk() {
    MemStoreLAB mslab = this.mslabs.get(0);
    return mslab.getNewExternalChunk();
  }

  @Override
  public void close() {
    // 'openScannerCount' here tracks the scanners opened on segments which directly refer to this
    // MSLAB. The individual MSLABs this refers also having its own 'openScannerCount'. The usage of
    // the variable in close() and decScannerCount() is as as that in HeapMemstoreLAB. Here the
    // close just delegates the call to the individual MSLABs. The actual return of the chunks to
    // MSLABPool will happen within individual MSLABs only (which is at the leaf level).
    // Say an ImmutableMemStoreLAB is created over 2 HeapMemStoreLABs at some point and at that time
    // both of them were referred by ongoing scanners. So they have > 0 'openScannerCount'. Now over
    // the new Segment some scanners come in and this MSLABs 'openScannerCount' also goes up and
    // then come down on finish of scanners. Now a close() call comes to this Immutable MSLAB. As
    // it's 'openScannerCount' is zero it will call close() on both of the Heap MSLABs. Say by that
    // time the old scanners on one of the MSLAB got over where as on the other, still an old
    // scanner is going on. The call close() on that MSLAB will not close it immediately but will
    // just mark it for closure as it's 'openScannerCount' still > 0. Later once the old scan is
    // over, the decScannerCount() call will do the actual close and return of the chunks.
    this.closed = true;
    // When there are still on going scanners over this MSLAB, we will defer the close until all
    // scanners finish. We will just mark it for closure. See #decScannerCount(). This will be
    // called at end of every scan. When it is marked for closure and scanner count reached 0, we
    // will do the actual close then.
    checkAndCloseMSLABs(openScannerCount.get());
  }

  private void checkAndCloseMSLABs(int openScanners) {
    if (openScanners == 0) {
      for (MemStoreLAB mslab : this.mslabs) {
        mslab.close();
      }
    }
  }

  @Override
  public void incScannerCount() {
    this.openScannerCount.incrementAndGet();
  }

  @Override
  public void decScannerCount() {
    int count = this.openScannerCount.decrementAndGet();
    if (this.closed) {
      checkAndCloseMSLABs(count);
    }
  }
}

/*
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

import com.google.errorprone.annotations.RestrictedApi;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.nio.RefCnt;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A MemStoreLAB implementation which wraps N MemStoreLABs. Its main duty is in proper managing the
 * close of the individual MemStoreLAB. This is treated as an immutable one and so do not allow to
 * add any more Cells into it. {@link #copyCellInto(ExtendedCell)} throws Exception
 */
@InterfaceAudience.Private
public class ImmutableMemStoreLAB implements MemStoreLAB {

  private final RefCnt refCnt;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final List<MemStoreLAB> mslabs;

  public ImmutableMemStoreLAB(List<MemStoreLAB> mslabs) {
    this.mslabs = mslabs;
    this.refCnt = RefCnt.create(() -> {
      closeMSLABs();
    });
  }

  @Override
  public ExtendedCell copyCellInto(ExtendedCell cell) {
    throw new IllegalStateException("This is an Immutable MemStoreLAB.");
  }

  /**
   * The process of merging assumes all cells are allocated on mslab. There is a rare case in which
   * the first immutable segment, participating in a merge, is a CSLM. Since the CSLM hasn't been
   * flattened yet, and there is no point in flattening it (since it is going to be merged), its big
   * cells (for whom size > maxAlloc) must be copied into mslab. This method copies the passed cell
   * into the first mslab in the mslabs list, returning either a new cell instance over the copied
   * data, or null when this cell cannt be copied.
   */
  @Override
  public ExtendedCell forceCopyOfBigCellInto(ExtendedCell cell) {
    MemStoreLAB mslab = this.mslabs.get(0);
    return mslab.forceCopyOfBigCellInto(cell);
  }

  /*
   * Returning a new pool chunk, without replacing current chunk, meaning MSLABImpl does not make
   * the returned chunk as CurChunk. The space on this chunk will be allocated externally. The
   * interface is only for external callers.
   */
  @Override
  public Chunk getNewExternalChunk(ChunkCreator.ChunkType chunkType) {
    MemStoreLAB mslab = this.mslabs.get(0);
    return mslab.getNewExternalChunk(chunkType);
  }

  /*
   * Returning a new chunk, without replacing current chunk, meaning MSLABImpl does not make the
   * returned chunk as CurChunk. The space on this chunk will be allocated externally. The interface
   * is only for external callers.
   */
  @Override
  public Chunk getNewExternalChunk(int size) {
    MemStoreLAB mslab = this.mslabs.get(0);
    return mslab.getNewExternalChunk(size);
  }

  @Override
  public void close() {
    // 'refCnt' here tracks the scanners opened on segments which directly refer to this
    // MSLAB. The individual MSLABs this refers also having its own 'refCnt'. The usage of
    // the variable in close() and decScannerCount() is as as that in HeapMemstoreLAB. Here the
    // close just delegates the call to the individual MSLABs. The actual return of the chunks to
    // MSLABPool will happen within individual MSLABs only (which is at the leaf level).
    // Say an ImmutableMemStoreLAB is created over 2 HeapMemStoreLABs at some point and at that time
    // both of them were referred by ongoing scanners. So they have > 0 'refCnt'. Now over
    // the new Segment some scanners come in and this MSLABs 'refCnt' also goes up and
    // then come down on finish of scanners. Now a close() call comes to this Immutable MSLAB. As
    // it's 'refCnt' is zero it will call close() on both of the Heap MSLABs. Say by that
    // time the old scanners on one of the MSLAB got over where as on the other, still an old
    // scanner is going on. The call close() on that MSLAB will not close it immediately but will
    // just decrease it's 'refCnt' and it's 'refCnt' still > 0. Later once the old scan is
    // over, the decScannerCount() call will do the actual close and return of the chunks.
    if (!this.closed.compareAndSet(false, true)) {
      return;
    }
    // When there are still on going scanners over this MSLAB, we will defer the close until all
    // scanners finish. We will just decrease it's 'refCnt'. See #decScannerCount(). This will be
    // called at end of every scan. When it's 'refCnt' reached 0, we will do the actual close then.
    this.refCnt.release();
  }

  private void closeMSLABs() {
    for (MemStoreLAB mslab : this.mslabs) {
      mslab.close();
    }
  }

  @Override
  public void incScannerCount() {
    this.refCnt.retain();
  }

  @Override
  public void decScannerCount() {
    this.refCnt.release();
  }

  @Override
  public boolean isOnHeap() {
    return !isOffHeap();
  }

  @Override
  public boolean isOffHeap() {
    return ChunkCreator.getInstance().isOffheap();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  int getRefCntValue() {
    return this.refCnt.refCnt();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  boolean isClosed() {
    return this.closed.get();
  }

}

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

package org.apache.hadoop.hbase.codec.prefixtree;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.codec.prefixtree.decode.DecoderFactory;
import org.apache.hadoop.hbase.codec.prefixtree.decode.PrefixTreeArraySearcher;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;

/**
 * These methods have the same definition as any implementation of the EncodedSeeker.
 *
 * In the future, the EncodedSeeker could be modified to work with the Cell interface directly.  It
 * currently returns a new KeyValue object each time getKeyValue is called.  This is not horrible,
 * but in order to create a new KeyValue object, we must first allocate a new byte[] and copy in
 * the data from the PrefixTreeCell.  It is somewhat heavyweight right now.
 */
@InterfaceAudience.Private
public class PrefixTreeSeeker implements EncodedSeeker {

  protected ByteBuffer block;
  protected boolean includeMvccVersion;
  protected PrefixTreeArraySearcher ptSearcher;
  protected boolean movedToPrevious = false;

  public PrefixTreeSeeker(boolean includeMvccVersion) {
    this.includeMvccVersion = includeMvccVersion;
  }

  @Override
  public void setCurrentBuffer(ByteBuffer fullBlockBuffer) {
    block = fullBlockBuffer;
    ptSearcher = DecoderFactory.checkOut(block, includeMvccVersion);
    rewind();
  }

  /**
   * Currently unused.
   * <p/>
   * TODO performance leak. should reuse the searchers. hbase does not currently have a hook where
   * this can be called
   */
  public void releaseCurrentSearcher(){
    DecoderFactory.checkIn(ptSearcher);
  }


  @Override
  public ByteBuffer getKeyDeepCopy() {
    return KeyValueUtil.copyKeyToNewByteBuffer(ptSearcher.current());
  }


  @Override
  public ByteBuffer getValueShallowCopy() {
    return CellUtil.getValueBufferShallowCopy(ptSearcher.current());
  }

  /**
   * currently must do deep copy into new array
   */
  @Override
  public ByteBuffer getKeyValueBuffer() {
    return KeyValueUtil.copyToNewByteBuffer(ptSearcher.current());
  }

  /**
   * currently must do deep copy into new array
   */
  @Override
  public KeyValue getKeyValue() {
    if (ptSearcher.current() == null) {
      return null;
    }
    return KeyValueUtil.copyToNewKeyValue(ptSearcher.current());
  }

  /**
   * Currently unused.
   * <p/>
   * A nice, lightweight reference, though the underlying cell is transient. This method may return
   * the same reference to the backing PrefixTreeCell repeatedly, while other implementations may
   * return a different reference for each Cell.
   * <p/>
   * The goal will be to transition the upper layers of HBase, like Filters and KeyValueHeap, to
   * use this method instead of the getKeyValue() methods above.
   */
  public Cell get() {
    return ptSearcher.current();
  }

  @Override
  public void rewind() {
    ptSearcher.positionAtFirstCell();
  }

  @Override
  public boolean next() {
    return ptSearcher.advance();
  }

  public boolean advance() {
    return ptSearcher.advance();
  }


  private static final boolean USE_POSITION_BEFORE = false;

  /**
   * Seek forward only (should be called reseekToKeyInBlock?).
   * <p/>
   * If the exact key is found look at the seekBefore variable and:<br/>
   * - if true: go to the previous key if it's true<br/>
   * - if false: stay on the exact key
   * <p/>
   * If the exact key is not found, then go to the previous key *if possible*, but remember to
   * leave the scanner in a valid state if possible.
   * <p/>
   * @param keyOnlyBytes KeyValue format of a Cell's key at which to position the seeker
   * @param offset offset into the keyOnlyBytes array
   * @param length number of bytes of the keyOnlyBytes array to use
   * @param forceBeforeOnExactMatch if an exact match is found and seekBefore=true, back up 1 Cell
   * @return 0 if the seeker is on the exact key<br/>
   *         1 if the seeker is not on the key for any reason, including seekBefore being true
   */
  @Override
  public int seekToKeyInBlock(byte[] keyOnlyBytes, int offset, int length,
      boolean forceBeforeOnExactMatch) {
    if (USE_POSITION_BEFORE) {
      return seekToOrBeforeUsingPositionAtOrBefore(keyOnlyBytes, offset, length,
        forceBeforeOnExactMatch);
    }else{
      return seekToOrBeforeUsingPositionAtOrAfter(keyOnlyBytes, offset, length,
        forceBeforeOnExactMatch);
    }
  }



  /*
   * Support both of these options since the underlying PrefixTree supports both.  Possibly
   * expand the EncodedSeeker to utilize them both.
   */

  protected int seekToOrBeforeUsingPositionAtOrBefore(byte[] keyOnlyBytes, int offset, int length,
      boolean seekBefore){
    // this does a deep copy of the key byte[] because the CellSearcher interface wants a Cell
    KeyValue kv = KeyValue.createKeyValueFromKey(keyOnlyBytes, offset, length);

    CellScannerPosition position = ptSearcher.seekForwardToOrBefore(kv);

    if(CellScannerPosition.AT == position){
      if (seekBefore) {
        ptSearcher.previous();
        return 1;
      }
      return 0;
    }

    return 1;
  }


  protected int seekToOrBeforeUsingPositionAtOrAfter(byte[] keyOnlyBytes, int offset, int length,
      boolean seekBefore){
    // this does a deep copy of the key byte[] because the CellSearcher interface wants a Cell
    KeyValue kv = KeyValue.createKeyValueFromKey(keyOnlyBytes, offset, length);

    //should probably switch this to use the seekForwardToOrBefore method
    CellScannerPosition position = ptSearcher.seekForwardToOrAfter(kv);

    if(CellScannerPosition.AT == position){
      if (seekBefore) {
        ptSearcher.previous();
        return 1;
      }
      return 0;

    }

    if(CellScannerPosition.AFTER == position){
      if(!ptSearcher.isBeforeFirst()){
        ptSearcher.previous();
      }
      return 1;
    }

    if(position == CellScannerPosition.AFTER_LAST){
      if (seekBefore) {
        // We need not set movedToPrevious because the intention is to seekBefore
        ptSearcher.previous();
      }
      return 1;
    }

    throw new RuntimeException("unexpected CellScannerPosition:"+position);
  }

  @Override
  public int compareKey(KVComparator comparator, byte[] key, int offset, int length) {
    // can't optimize this, make a copy of the key
    ByteBuffer bb = getKeyDeepCopy();
    return comparator.compareFlatKey(key, offset, length, bb.array(), bb.arrayOffset(), bb.limit());
  }
}

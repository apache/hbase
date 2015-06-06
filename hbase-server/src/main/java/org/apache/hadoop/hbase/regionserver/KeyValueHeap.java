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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;

/**
 * Implements a heap merge across any number of KeyValueScanners.
 * <p>
 * Implements KeyValueScanner itself.
 * <p>
 * This class is used at the Region level to merge across Stores
 * and at the Store level to merge across the memstore and StoreFiles.
 * <p>
 * In the Region case, we also need InternalScanner.next(List), so this class
 * also implements InternalScanner.  WARNING: As is, if you try to use this
 * as an InternalScanner at the Store level, you will get runtime exceptions.
 */
@InterfaceAudience.Private
public class KeyValueHeap extends NonReversedNonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner {
  protected PriorityQueue<KeyValueScanner> heap = null;

  /**
   * The current sub-scanner, i.e. the one that contains the next key/value
   * to return to the client. This scanner is NOT included in {@link #heap}
   * (but we frequently add it back to the heap and pull the new winner out).
   * We maintain an invariant that the current sub-scanner has already done
   * a real seek, and that current.peek() is always a real key/value (or null)
   * except for the fake last-key-on-row-column supplied by the multi-column
   * Bloom filter optimization, which is OK to propagate to StoreScanner. In
   * order to ensure that, always use {@link #pollRealKV()} to update current.
   */
  protected KeyValueScanner current = null;

  protected KVScannerComparator comparator;

  protected Set<KeyValueScanner> scannersForDelayedClose = new HashSet<KeyValueScanner>();

  /**
   * Constructor.  This KeyValueHeap will handle closing of passed in
   * KeyValueScanners.
   * @param scanners
   * @param comparator
   */
  public KeyValueHeap(List<? extends KeyValueScanner> scanners,
      CellComparator comparator) throws IOException {
    this(scanners, new KVScannerComparator(comparator));
  }

  /**
   * Constructor.
   * @param scanners
   * @param comparator
   * @throws IOException
   */
  KeyValueHeap(List<? extends KeyValueScanner> scanners,
      KVScannerComparator comparator) throws IOException {
    this.comparator = comparator;
    if (!scanners.isEmpty()) {
      this.heap = new PriorityQueue<KeyValueScanner>(scanners.size(),
          this.comparator);
      for (KeyValueScanner scanner : scanners) {
        if (scanner.peek() != null) {
          this.heap.add(scanner);
        } else {
          this.scannersForDelayedClose.add(scanner);
        }
      }
      this.current = pollRealKV();
    }
  }

  public Cell peek() {
    if (this.current == null) {
      return null;
    }
    return this.current.peek();
  }

  public Cell next()  throws IOException {
    if(this.current == null) {
      return null;
    }
    Cell kvReturn = this.current.next();
    Cell kvNext = this.current.peek();
    if (kvNext == null) {
      this.scannersForDelayedClose.add(this.current);
      this.current = pollRealKV();
    } else {
      KeyValueScanner topScanner = this.heap.peek();
      // no need to add current back to the heap if it is the only scanner left
      if (topScanner != null && this.comparator.compare(kvNext, topScanner.peek()) >= 0) {
        this.heap.add(this.current);
        this.current = pollRealKV();
      }
    }
    return kvReturn;
  }

  /**
   * Gets the next row of keys from the top-most scanner.
   * <p>
   * This method takes care of updating the heap.
   * <p>
   * This can ONLY be called when you are using Scanners that implement InternalScanner as well as
   * KeyValueScanner (a {@link StoreScanner}).
   * @param result
   * @return true if more rows exist after this one, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> result) throws IOException {
    return next(result, NoLimitScannerContext.getInstance());
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    if (this.current == null) {
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    InternalScanner currentAsInternal = (InternalScanner)this.current;
    boolean moreCells = currentAsInternal.next(result, scannerContext);
    Cell pee = this.current.peek();

    /*
     * By definition, any InternalScanner must return false only when it has no
     * further rows to be fetched. So, we can close a scanner if it returns
     * false. All existing implementations seem to be fine with this. It is much
     * more efficient to close scanners which are not needed than keep them in
     * the heap. This is also required for certain optimizations.
     */

    if (pee == null || !moreCells) {
      this.scannersForDelayedClose.add(this.current);
    } else {
      this.heap.add(this.current);
    }
    this.current = pollRealKV();
    if (this.current == null) {
      moreCells = scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }
    return moreCells;
  }

  protected static class KVScannerComparator implements Comparator<KeyValueScanner> {
    protected CellComparator kvComparator;
    /**
     * Constructor
     * @param kvComparator
     */
    public KVScannerComparator(CellComparator kvComparator) {
      this.kvComparator = kvComparator;
    }
    public int compare(KeyValueScanner left, KeyValueScanner right) {
      int comparison = compare(left.peek(), right.peek());
      if (comparison != 0) {
        return comparison;
      } else {
        // Since both the keys are exactly the same, we break the tie in favor
        // of the key which came latest.
        long leftSequenceID = left.getSequenceID();
        long rightSequenceID = right.getSequenceID();
        if (leftSequenceID > rightSequenceID) {
          return -1;
        } else if (leftSequenceID < rightSequenceID) {
          return 1;
        } else {
          return 0;
        }
      }
    }
    /**
     * Compares two KeyValue
     * @param left
     * @param right
     * @return less than 0 if left is smaller, 0 if equal etc..
     */
    public int compare(Cell left, Cell right) {
      return this.kvComparator.compare(left, right);
    }
    /**
     * @return KVComparator
     */
    public CellComparator getComparator() {
      return this.kvComparator;
    }
  }

  public void close() {
    for (KeyValueScanner scanner : this.scannersForDelayedClose) {
      scanner.close();
    }
    this.scannersForDelayedClose.clear();
    if (this.current != null) {
      this.current.close();
    }
    if (this.heap != null) {
      KeyValueScanner scanner;
      while ((scanner = this.heap.poll()) != null) {
        scanner.close();
      }
    }
  }

  /**
   * Seeks all scanners at or below the specified seek key.  If we earlied-out
   * of a row, we may end up skipping values that were never reached yet.
   * Rather than iterating down, we want to give the opportunity to re-seek.
   * <p>
   * As individual scanners may run past their ends, those scanners are
   * automatically closed and removed from the heap.
   * <p>
   * This function (and {@link #reseek(Cell)}) does not do multi-column
   * Bloom filter and lazy-seek optimizations. To enable those, call
   * {@link #requestSeek(Cell, boolean, boolean)}.
   * @param seekKey KeyValue to seek at or after
   * @return true if KeyValues exist at or after specified key, false if not
   * @throws IOException
   */
  @Override
  public boolean seek(Cell seekKey) throws IOException {
    return generalizedSeek(false,    // This is not a lazy seek
                           seekKey,
                           false,    // forward (false: this is not a reseek)
                           false);   // Not using Bloom filters
  }

  /**
   * This function is identical to the {@link #seek(Cell)} function except
   * that scanner.seek(seekKey) is changed to scanner.reseek(seekKey).
   */
  @Override
  public boolean reseek(Cell seekKey) throws IOException {
    return generalizedSeek(false,    // This is not a lazy seek
                           seekKey,
                           true,     // forward (true because this is reseek)
                           false);   // Not using Bloom filters
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean requestSeek(Cell key, boolean forward,
      boolean useBloom) throws IOException {
    return generalizedSeek(true, key, forward, useBloom);
  }

  /**
   * @param isLazy whether we are trying to seek to exactly the given row/col.
   *          Enables Bloom filter and most-recent-file-first optimizations for
   *          multi-column get/scan queries.
   * @param seekKey key to seek to
   * @param forward whether to seek forward (also known as reseek)
   * @param useBloom whether to optimize seeks using Bloom filters
   */
  private boolean generalizedSeek(boolean isLazy, Cell seekKey,
      boolean forward, boolean useBloom) throws IOException {
    if (!isLazy && useBloom) {
      throw new IllegalArgumentException("Multi-column Bloom filter " +
          "optimization requires a lazy seek");
    }

    if (current == null) {
      return false;
    }
    heap.add(current);
    current = null;

    KeyValueScanner scanner;
    while ((scanner = heap.poll()) != null) {
      Cell topKey = scanner.peek();
      if (comparator.getComparator().compare(seekKey, topKey) <= 0) {
        // Top KeyValue is at-or-after Seek KeyValue. We only know that all
        // scanners are at or after seekKey (because fake keys of
        // scanners where a lazy-seek operation has been done are not greater
        // than their real next keys) but we still need to enforce our
        // invariant that the top scanner has done a real seek. This way
        // StoreScanner and RegionScanner do not have to worry about fake keys.
        heap.add(scanner);
        current = pollRealKV();
        return current != null;
      }

      boolean seekResult;
      if (isLazy && heap.size() > 0) {
        // If there is only one scanner left, we don't do lazy seek.
        seekResult = scanner.requestSeek(seekKey, forward, useBloom);
      } else {
        seekResult = NonLazyKeyValueScanner.doRealSeek(
            scanner, seekKey, forward);
      }

      if (!seekResult) {
        this.scannersForDelayedClose.add(scanner);
      } else {
        heap.add(scanner);
      }
    }

    // Heap is returning empty, scanner is done
    return false;
  }

  /**
   * Fetches the top sub-scanner from the priority queue, ensuring that a real
   * seek has been done on it. Works by fetching the top sub-scanner, and if it
   * has not done a real seek, making it do so (which will modify its top KV),
   * putting it back, and repeating this until success. Relies on the fact that
   * on a lazy seek we set the current key of a StoreFileScanner to a KV that
   * is not greater than the real next KV to be read from that file, so the
   * scanner that bubbles up to the top of the heap will have global next KV in
   * this scanner heap if (1) it has done a real seek and (2) its KV is the top
   * among all top KVs (some of which are fake) in the scanner heap.
   */
  protected KeyValueScanner pollRealKV() throws IOException {
    KeyValueScanner kvScanner = heap.poll();
    if (kvScanner == null) {
      return null;
    }

    while (kvScanner != null && !kvScanner.realSeekDone()) {
      if (kvScanner.peek() != null) {
        kvScanner.enforceSeek();
        Cell curKV = kvScanner.peek();
        if (curKV != null) {
          KeyValueScanner nextEarliestScanner = heap.peek();
          if (nextEarliestScanner == null) {
            // The heap is empty. Return the only possible scanner.
            return kvScanner;
          }

          // Compare the current scanner to the next scanner. We try to avoid
          // putting the current one back into the heap if possible.
          Cell nextKV = nextEarliestScanner.peek();
          if (nextKV == null || comparator.compare(curKV, nextKV) < 0) {
            // We already have the scanner with the earliest KV, so return it.
            return kvScanner;
          }

          // Otherwise, put the scanner back into the heap and let it compete
          // against all other scanners (both those that have done a "real
          // seek" and a "lazy seek").
          heap.add(kvScanner);
        } else {
          // Close the scanner because we did a real seek and found out there
          // are no more KVs.
          this.scannersForDelayedClose.add(kvScanner);
        }
      } else {
        // Close the scanner because it has already run out of KVs even before
        // we had to do a real seek on it.
        this.scannersForDelayedClose.add(kvScanner);
      }
      kvScanner = heap.poll();
    }

    return kvScanner;
  }

  /**
   * @return the current Heap
   */
  public PriorityQueue<KeyValueScanner> getHeap() {
    return this.heap;
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  KeyValueScanner getCurrentForTesting() {
    return current;
  }

  @Override
  public Cell getNextIndexedKey() {
    // here we return the next index key from the top scanner
    return current == null ? null : current.getNextIndexedKey();
  }
}

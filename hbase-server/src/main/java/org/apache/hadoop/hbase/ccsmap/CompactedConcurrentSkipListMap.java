/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.UnsafeAccess;


@SuppressWarnings("restriction")
@InterfaceAudience.Private
public class CompactedConcurrentSkipListMap<K, V> extends AbstractMap<K, V>
    implements ConcurrentNavigableMap<K, V>, Cloneable, Serializable {

  private static final long serialVersionUID = 1L;

  private static final Random seedGenerator = new Random();

  private final static int EQ = 1;
  private final static int LT = 2;
  private final static int GT = 0;

  private final static boolean ON_PAGE = true;
  private final static boolean ON_HEAP = false;
  private final static boolean NODE_DELETED = true;
  private final static boolean NODE_PRESENT = false;


  private final static long HEADER_M_DELETED = 0x8000000000000000L;
  private final static long HEADER_M_PAGED = 0x4000000000000000L;
  private final static long HEADER_M_LEVEL = 0x3E00000000000000L;
  private final static long HEADER_M_LENGTH = 0x1FFFFFE00000000L;
  private final static long HEADER_M_REFERENCE = 0x1FFFFFFFFL;

  private final static long HEADER_SHIFT_LEVEL = 57;
  private final static long HEADER_SHIFT_LENGTH = 33;

  private final static long MASK_REAL_NODE = 0x7FFFFFFFFFFFFFFFL;
  private final static long MASK_NODE_DELETE_MARKER = 0x8000000000000000L;

  private final static int MAX_ALLOCATING_PAGES = 2;

  /**
   * The max level of index
   */
  private static final int LEVEL_LIMIT = 31;
  private static final long HEAD_NODE = 1;
  private static final long NULL_NODE = 0;

  transient private final PageManager<byte[]> dataPageManager;
  transient private final PageManager<Object[]> heapKVManager;

  private AtomicLong trashData = new AtomicLong(0);
  private AtomicLong trashHeapKVCount = new AtomicLong(0);
  private AtomicLong trashHeapKVSize = new AtomicLong(0);
  private AtomicLong aliveHeapKVCount = new AtomicLong(0);
  private AtomicLong aliveHeapKVSize = new AtomicLong(0);

  private final CompactedTypeHelper<K, V> typeHelper;
  private final PageSetting settings;


  private transient int randomSeed = seedGenerator.nextInt() | 0x0100; // ensure
                                                                       // nonzero
  AtomicInteger maxLevel = new AtomicInteger(1);

  private transient KeySet<K> keySet;
  private transient Values<V> values;
  private transient EntrySet<K, V> entrySet;
  private transient SubMap<K, V> descendingMap;

  /**
   * Print skip list base&index structure for debug
   */
  public void dump() {
    StringBuilder sb = new StringBuilder();
    for (int level = getHeadHighestLevel(); level > 0; --level) {
      sb.append("level ").append(level).append(":");
      long q = HEAD_NODE;
      long r = NULL_NODE;
      int cnt = 0;

      do {
        r = indexGetNext(q, level);
        sb.append('\t').append(cnt).append(":").append(q);
        q = r;
        cnt += 1;
      } while (r != NULL_NODE);
      sb.append('\n');
    }

    long n = HEAD_NODE;
    long f = NULL_NODE;
    int cnt = 0;
    sb.append("base :");
    do {
      f = nodeGetNext(n);
      sb.append('\t').append(cnt).append(":").append(n).append("");
      n = f;
      cnt += 1;
    } while (f != NULL_NODE);
    System.out.println(sb.toString());
  }

  public MemoryUsage getMemoryUsage() {
    MemoryUsage mu = new MemoryUsage();
    mu.dataSpace = dataPageManager.capacity();
    mu.maxDataSpae = dataPageManager.maxSize();
    mu.trashDataSpace = trashData.get();
    mu.aliveHeapKVCount = aliveHeapKVCount.get();
    mu.aliveHeapKVSize = aliveHeapKVSize.get();
    mu.trashHeapKVCount = trashHeapKVCount.get();
    mu.trashHeapKVSize = trashHeapKVSize.get();
    mu.heapKVCapacity = heapKVManager.capacity();
    mu.maxHeapKVCapacity = heapKVManager.maxSize();
    mu.totalSize = mu.dataSpace + mu.aliveHeapKVSize
        + mu.trashHeapKVSize;
    return mu;
  }

  public CompactedConcurrentSkipListMap(CompactedTypeHelper<K, V> typeHelper) {
    this(typeHelper, new PageSetting());
  }

  public CompactedConcurrentSkipListMap(CompactedTypeHelper<K, V> typeHelper,
      PageSetting ps) {
    this.typeHelper = typeHelper;
    this.settings = ps;

    if (settings.heapKVThreshold > getMaxOnHeapKVThreshold(ps.pageSize)) {
      settings.heapKVThreshold = getMaxOnHeapKVThreshold(ps.pageSize);
    }

    final CompactedConcurrentSkipListMap<K, V> m = this;
    final int dataPageAlignShift = CCSMapUtils.getShiftFromX(Long.SIZE / Byte.SIZE);
    final int firstPageSizeWithoutAligned = estimatedNodeSizeHeadNodeSize() + 1;
    final int firstPageSizeForDataPageManager = (((firstPageSizeWithoutAligned - 1) >> dataPageAlignShift) + 1) << dataPageAlignShift;
    this.dataPageManager = new PageManager<byte[]>("DataPage", ps.pages,
        ps.pageSize, MAX_ALLOCATING_PAGES, dataPageAlignShift) {
      @Override
      protected byte[] allocatePage(int size) {
        if (m.settings.pageAllocator != null) {
          byte[] ret = m.settings.pageAllocator.allocatePages(size);
          if (ret.length != size) {
            throw new InvalidParameterException(
                "Page size not consistent, expect: " + size + " , actual:"
                    + ret.length);
          }
          return ret;
        } else {
          return new byte[size];
        }
      }

      @Override
      protected byte[] allocateFirstPage() {
        return new byte[firstPageSizeForDataPageManager];
      }
    };

    final int firstPageSizeForHeapKVManager = 1;
    this.heapKVManager = new PageManager<Object[]>(
        "HeapKV", ps.heapKVPages, ps.heapKVPageSize, MAX_ALLOCATING_PAGES, 0) {
      @Override
      protected Object[] allocatePage(int size) {
        if (m.settings.pageAllocator != null) {
          Object[] ret = m.settings.pageAllocator.allocateHeapKVPage(size);
          if (ret.length != size) {
            throw new InvalidParameterException(
                "Page size not consistent, expect: " + size + " , actual:"
                    + ret.length);
          }
          return ret;
        } else {
          return new Object[size];
        }
      }

      @Override
      protected Object[] allocateFirstPage() {
        return new Object[firstPageSizeForHeapKVManager];
      }

    };

    // allocate 1 byte to skip address 0, since 0 is a special meaning NULL for node logic.
    this.dataPageManager.allocate(1);
    createHeadNode();
    long dpmFirstPageUsed = this.dataPageManager.skipFirstPage();
    assert dpmFirstPageUsed == firstPageSizeForDataPageManager;
    assert this.dataPageManager.getAllocatedPages() == 0;

    this.heapKVManager.allocate(1);
    long hkvmFirstPageUsed = this.heapKVManager.skipFirstPage();
    assert hkvmFirstPageUsed == firstPageSizeForHeapKVManager;
    assert this.heapKVManager.getAllocatedPages() == 0;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    CompactedConcurrentSkipListMap<K, V> ret = new CompactedConcurrentSkipListMap<K, V>(
        typeHelper, settings);
    ret.putAll(this);
    return ret;
  }

  final private int getMaxOnHeapKVThreshold(int pageSize) {
    int forHeader = 1;
    int forNext = 1;
    int forLevel = LEVEL_LIMIT;
    int forData = pageSize - forHeader - forNext - forLevel;
    return forData;
  }

  final private int compareKeyAt(K key, long node) {
    return compareKeyAt(key, node, nodeHeader(node));
  }

  final private int compareKeyAt(K key, long node, long header) {
    assert !isNullNode(node);
    assert node != HEAD_NODE;
    long refNode = headerReference(header);
    if (!isNullNode(refNode)) {
      return compareKeyAt(key, refNode);
    }
    if (headerOnPage(header) == ON_PAGE) {
      int length = headerDataLength(header);
      long dataPos = dataPos(node, header);
      byte[] data = getDataOnPage(dataPos);
      int lo = getLocalOffsetOnPage(dataPos);
      return typeHelper.compare(key, data, lo, length);
    } else {
      int heapKVOffset = headerDataLength(header);
      KVPair<K, V> kv = getOnHeapKV(heapKVOffset);
      return typeHelper.compare(key, kv.key());
    }
  }

  final private int compare(K key1, K key2) {
    return typeHelper.compare(key1, key2);
  }

  @SuppressWarnings("unchecked")
  public V putIfAbsent(K key, V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    return (V) doPut(key, value, true, false);
  }

  public boolean remove(Object key, Object value) {
    if (key == null) {
      throw new NullPointerException();
    }
    if (value == null) {
      return false;
    }
    return doRemove(key, value) != null;
  }

  @SuppressWarnings("unchecked")
  private V doRemove(Object key, Object value) {
    K k = (K) key;
    if (k == null) {
      throw new InvalidParameterException("Invalid key type.");
    }
    for (;;) {
      long b = findPredecessor(k);
      long n = nodeGetNext(b);
      for (;;) {
        if (isNullNode(n))
          return null;
        long f = nodeGetNext(n);
        if (n != nodeGetNext(b)) { // inconsistent read
          break;
        }

        long n_header = nodeHeader(n);
        if (headerDeleted(n_header)) { // n is deleted
          nodeHelpDelete(n, b, f);
          break;
        }
        if (isMarker(n) || nodeDeleted(b)) { // b is deleted
          break;
        }
        int c = compareKeyAt(k, n, n_header);
        if (c < 0) {
          return null;
        }
        if (c > 0) {
          b = n;
          n = f;
          continue;
        }
        KVPair<K, V> kv = nodeGetKV(n, n_header);
        if (value != null && !value.equals(kv.value())) {
          return null;
        }

        if (!nodeCasToDeleted(n, n_header)) {
          break;
        }
        if (!nodeAppendMarker(n, f) || !nodeCasSetNext(b, n, f)) {
          findNode(k); // Retry via findNode
        } else {
          findPredecessor(k); // Clean index
          if (indexGetNext(HEAD_NODE, getHeadHighestLevel()) == NULL_NODE) {
            tryReduceLevel();
          }
        }
        return kv.value();
      }
    }
  }

  /**
   * Try to reduce head node's max level
   *
   * @return true if succeed
   */
  private boolean tryReduceLevel() {
    int level = getHeadHighestLevel();
    if (level >= 3 && NULL_NODE == indexGetNext(HEAD_NODE, level)) {
      return casHeadHighestLevel(level, level - 1);
    }
    return false;
  }

  public boolean replace(K key, V oldValue, V newValue) {
    if (oldValue == null || newValue == null) {
      throw new NullPointerException();
    }
    long z = 0;
    for (;;) {
      long n = findNode(key);
      if (isNullNode(n)) {
        return false;
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)) {
        KVPair<K, V> oldKV = nodeGetKV(n, n_header);
        if (!oldValue.equals(oldKV.value())) {
          return false;
        }
        if (z == NULL_NODE) {
          z = createNode(key, newValue, nodeGetNext(n));
        } else {
          nodeSetNext(z, nodeGetNext(n));
        }
        if (nodeCasToReference(n, n_header, z)) {
          return true;
        }
      }
    }
  }

  public V replace(K key, V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    long z = NULL_NODE;
    for (;;) {
      long n = findNode(key);
      if (isNullNode(n)) {
        return null;
      }
      if (z == NULL_NODE) {
        z = createNode(key, value, nodeGetNext(n));
      } else {
        nodeSetNext(z, nodeGetNext(n));
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header) && nodeCasToReference(n, n_header, z)) {
        return nodeGetKV(n, n_header).value();
      }
    }
  }

  /**
   * Returns the number of key-value mappings in this map. Beware that, unlike
   * in most collections, this method is NOT a constant-time operation. Because
   * of the asynchronous nature of these maps, determining the current number of
   * elements requires traversing them all to count them. Additionally, it is
   * possible for the size to change during execution of this method, in which
   * case the returned result will be inaccurate. Thus, this method is typically
   * not very useful in concurrent applications.
   *
   * @return: the number of elements in this map
   */
  public int size() {
    int count = 0;
    for (long n = findFirst(); !isNullNode(n); n = nodeGetNext(n)) {
      if (!nodeDeleted(n)) {
        count += 1;
      }
    }
    return count;
  }

  public boolean isEmpty() {
    return isNullNode(findFirst());
  }

  public boolean containsKey(Object key) {
    return doGet(key) != null;
  }

  public boolean containsValue(Object value) {
    if (value == null) {
      throw new NullPointerException();
    }
    for (long n = findFirst(); !isNullNode(n); n = nodeGetNext(n)) {
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)
          && value.equals(nodeGetKV(n, n_header).value())) {
        return true;
      }
    }
    return false;
  }

  public V get(Object key) {
    return doGet(key);
  }

  /**
   * Ensure the given node is NOT a marker
   *
   * @param node
   */
  final private void nodeEnsureNotMarker(long node) {
    if ((node & MASK_NODE_DELETE_MARKER) != 0) {
      throw new RuntimeException(
          "There may be a bug in CompatedConcurrentSkipListMap, node should not be a marker, node="
              + node);
    }
  }

  /**
   * Get the given node's key-value pair's snapshot, the snapshot is determined
   * by offset
   *
   * @param node
   * @param offset
   *          offset snapshot of this node
   * @return
   */
  final private KVPair<K, V> nodeGetKV(long node, long header) {
    assert !isNullNode(node);
    assert node != HEAD_NODE;
    long refNode = headerReference(header);
    if (!isNullNode(refNode)) {
      return nodeGetKV2(refNode);
    }
    if (headerOnPage(header)) {
      int length = headerDataLength(header);
      long dataPos = dataPos(node, header);
      byte[] data = getDataOnPage(dataPos);
      int lo = getLocalOffsetOnPage(dataPos);
      return typeHelper.decomposite(data, lo, length);
    } else {
      int heapKVOffset = headerDataLength(header);
      KVPair<K, V> kv = getOnHeapKV(heapKVOffset);
      return kv;
    }
  }

  /**
   * Get the given node's key-value pair,
   *
   * @param node
   * @return
   */
  final private KVPair<K, V> nodeGetKV2(long node) {
    return nodeGetKV(node, nodeHeader(node));
  }

  /**
   * Specialized variant of findNode to perform Map.get. Does a weak traversal,
   * not bothering to fix any deleted index nodes, returning early if it happens
   * to see key in index, and passing over any deleted base nodes, falling back
   * to getUsingFindNode only if it would otherwise return value from an ongoing
   * deletion. Also uses "bound" to eliminate need for some comparisons (see
   * Pugh Cookbook). Also folds uses of null checks and node-skipping because
   * markers have null keys.
   *
   * @param key
   * @return the value related to the key.
   */
  @SuppressWarnings("unchecked")
  private V doGet(Object key) {
    K k = (K) key;
    if (k == null) {
      throw new InvalidParameterException("Invalid key type");
    }
    long bound = NULL_NODE;
    int level = getHeadHighestLevel();
    long q = HEAD_NODE;
    long r = indexGetNext(q, level);
    long n = NULL_NODE;
    int c;
    for (;;) {
      // Traverse right
      if (r != NULL_NODE && (n = r) != bound) {
        long n_header = nodeHeader(n);
        if ((c = compareKeyAt(k, n, n_header)) > 0) {
          q = r;
          r = indexGetNext(r, level);
          continue;
        } else if (c == 0) {
          if (headerDeleted(n_header)) {
            return getUsingFindNode(k); // maybe lost race when n is deleting
          } else {
            return nodeGetKV(n, n_header).value();
          }
        } else {
          bound = n;
        }
      }

      // Traverse down
      if ((level = level - 1) <= 0) {
        break;
      } else {
        r = indexGetNext(q, level);
      }
    }

    // Traverse on base
    for (n = nodeGetNext(q); !isNullNode(n); n = nodeGetNext(n)) {
      long n_header = nodeHeader(n);
      if ((c = compareKeyAt(k, n, n_header)) == 0) {
        if (headerDeleted(n_header)) {
          return getUsingFindNode(k); // maybe lost race when n is deleting
        } else {
          return nodeGetKV(n, n_header).value();
        }
      } else if (c < 0) {
        break;
      }
    }
    return null;
  }

  /**
   * Performs map.get via findNode. Used as a backup if doGet encounters an
   * in-progress deletion.
   *
   * @param key
   * @return
   */
  private V getUsingFindNode(K key) {
    /**
     * Loop needed here and elsewhere in case value field goes null just as it
     * is about to be returned, in which case we lost a race with a deletion, so
     * must retry.
     */
    for (;;) {
      long n = findNode(key);
      if (isNullNode(n)) {
        return null;
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)) {
        return nodeGetKV(n, n_header).value();
      }
    }
  }

  public V put(K key, V value) {
    if (value == null) {
      throw new NullPointerException();
    }
    return (V) doPut(key, value, false, false);
  }

  public PutAndFetch<V> putAndFetch(K key, V value) {
    return (PutAndFetch<V>) doPut(key, value, false, true);
  }

  /**
   * Main insertion method. Adds element if not present, or replaces value if
   * present and onlyIfAbsent is false.
   *
   * @param key
   * @param value
   * @param onlyIfAbsent
   *          if should not insert if already present
   * @return the old value, or null if newly inserted
   */
  private Object doPut(K key, V value, boolean onlyIfAbsent, boolean fetch) {
    long z = NULL_NODE;
    boolean succ = false;
    try {
      for (;;) {
        long b = findPredecessor(key);
        long n = nodeGetNext(b);
        long f = NULL_NODE;
        int c = 0;

        for (;;) {
          if (n != 0) { // b has a succ node
            f = nodeGetNext(n);
            if (n != nodeGetNext(b)) { // inconsistent read, restart
              break;
            }
            long n_header = nodeHeader(n);
            if (headerDeleted(n_header)) { // n is deleted
              nodeHelpDelete(n, b, f);
              break;
            }
            if (isMarker(n) || nodeDeleted(b)) { // b is deleted
              break;
            }
            c = compareKeyAt(key, n, n_header);
            if (c > 0) {
              b = n;
              n = f;
              continue;
            }

            // update kv on node-n
            if (c == 0) {
              if (z == NULL_NODE) { // create new node if needed
                z = createNode(key, value, f);
              }
              if (onlyIfAbsent || (succ = nodeCasToReference(n, n_header, z))) {
                if (fetch) {
                  KVPair<K, V> old = nodeGetKV(n, n_header);
                  KVPair<K, V> current = nodeGetKV2(z);
                  assert old != null;
                  assert current != null;
                  return new PutAndFetch<V>(old.value(), current.value());
                } else {
                  KVPair<K, V> kv = nodeGetKV(n, n_header);
                  return kv.value();
                }
              } else {
                break; // restart if lost race to replace value
              }
            }
          }

          if (z == 0) {
            // new node hasn't been created, create a new node and set its next
            // to n
            z = createNode(key, value, n);
          } else {
            // new node is already created, set its next to n
            nodeSetNext(z, n);
          }

          // append z after b
          if (!(succ = nodeCasSetNext(b, n, z))) {
            break; // restart if lost race to append z after b
          }

          int level = nodeLevel(z);
          if (level > 0) {
            insertIndex(z, level, key);
          }

          if (fetch) {
            return new PutAndFetch<V>(null, nodeGetKV2(z).value());
          } else {
            return null;
          }
        } // end for
      } // end for
    } finally {
      // New node is created but failed to insert into map,
      // do some cleaning work
      if (!succ && !isNullNode(z)) {
        trackTrashNode(z, nodeHeader(z));
      }
    }
  }

  /**
   * insert node z into index
   * @param z
   *          insert node
   * @param level
   *          the node's level is supposed
   * @param key
   *          the key of the insert node
   */
  private void insertIndex(long z, int level, K key) {
    int max = getHeadHighestLevel();
    if (level <= max) {
      addIndex(z, level, max, key);
    } else { // Add a new level
      int newLevel = level = max + 1;
      casHeadHighestLevel(max, newLevel);
      addIndex(z, level, level, key);
    }
  }

  /**
   * The main method to insert a node into index
   * @param node
   * @param indexLevel The level the node
   * @param maxLevel The map's max level,
   * @param key
   */
  private void addIndex(long node, int indexLevel, int maxLevel, K key) {
    // Track next level to insert in case of retries
    int insertionLevel = indexLevel;
    if (key == null) {
      throw new NullPointerException();
    }
    // Similar to findPredecessor, but adding index nodes along path to key.
    for (;;) {
      int level = maxLevel;
      long q = HEAD_NODE;
      long r = indexGetNext(q, level);
      long t = node;
      for (;;) {
        if (r != NULL_NODE) {
          // compare before deletion check avoids needing recheck
          long r_header = nodeHeader(r);
          int c = compareKeyAt(key, r, r_header);
          if (headerDeleted(r_header)) {
            if (!indexUnlink(q, r, level)) {
              break;
            }
            r = indexGetNext(r, level);
            continue;
          }
          if (c > 0) {
            q = r;
            r = indexGetNext(r, level);
            continue;
          }
        }

        if (level == insertionLevel) {
          // Don't insert index if node already deleted
          if (nodeDeleted(t)) {
            findNode(key); // cleans up
            return;
          }
          if (!indexLink(q, r, t, level)) {
            break; // restart
          }
          if (--insertionLevel == 0) {
            // need final deletion check before return
            if (nodeDeleted(t)) {
              findNode(key);
            }
            return;
          }
        }

        level -= 1;
        r = indexGetNext(q, level);
      }
    }
  }

  /**
   * Find the node of a given key, fix any deleted node on way
   * @param key
   * @return
   */
  private long findNode(K key) {
    for (;;) {
      long b = findPredecessor(key);
      long n = nodeGetNext(b);
      for (;;) {
        if (isNullNode(n)) {
          return NULL_NODE;
        }
        long f = nodeGetNext(n);
        if (n != nodeGetNext(b)) { // inconsistent read
          break;
        }
        long n_header = nodeHeader(n);
        if (headerDeleted(n_header)) { // n is deleted
          nodeHelpDelete(n, b, f);
          break;
        }
        if (isMarker(n) || nodeDeleted(b)) { // b is deleted
          break;
        }
        int c = compareKeyAt(key, n, n_header);
        if (c == 0) {
          return n;
        }
        if (c < 0) {
          return NULL_NODE;
        }
        b = n;
        n = f;
      }
    }
  }

  /**
   * Get head node's max level
   */
  final private int getHeadHighestLevel() {
    return maxLevel.get();
  }

  /**
   * Compare and set the head node's level
   * @param cmp The head node's expected level
   * @param val The new level will be set to the head node
   * @return true if succeed
   */
  final private boolean casHeadHighestLevel(int cmp, int val) {
    return maxLevel.compareAndSet(cmp, val);
  }

  /**
   * Find a base-level node with key strictly less than given key.
   * Unlink indexes to deleted nodes found along the way.
   * @param key
   * @return
   */
  private long findPredecessor(K key) {
    if (key == null) {
      throw new NullPointerException();
    }
    for (;;) {
      long q = HEAD_NODE;
      int level = getHeadHighestLevel();
      long r = indexGetNext(q, level);
      for (;;) {
        if (!isNullNode(r)) {
          long r_header = nodeHeader(r);
          if (headerDeleted(r_header)) {
            if (!indexUnlink(q, r, level)) {
              break; // restart
            }
            r = indexGetNext(q, level); // reread
            continue;
          }
          if (compareKeyAt(key, r, r_header) > 0) {
            q = r;
            r = indexGetNext(r, level);
            continue;
          }
        }
        level -= 1; // Go down
        if (level > 0) {
          r = indexGetNext(q, level);
        } else {
          return q;
        }
      }
    }
  }

  /**
   * Link index at level from q->right to q->newRight->Right
   * @param q
   * @param right
   * @param newRight
   * @param level
   * @return True if succeed
   */
  private boolean indexLink(long q, long right, long newRight, int level) {
    pageSet(indexPos(newRight, level), right);
    return !nodeDeleted(q)
        && pageCompareAndSet(indexPos(q, level), right, newRight);
  }

  /**
   * Unlink rightNode at level
   *
   * @param node
   * @param rightNode
   * @param level
   * @return True if succeed
   */
  final private boolean indexUnlink(long node, long rightNode, int level) {
    assert !isNullNode(node);
    assert rightNode != HEAD_NODE;
    long newRight = indexGetNext(rightNode, level);
    return !nodeDeleted(node)
        && pageCompareAndSet(indexPos(node, level), rightNode, newRight);
  }

  /**
   * Get the node's next node at index level
   * @param node
   * @param level
   * @return
   */
  final private long indexGetNext(long node, int level) {
    assert level > 0;
    return getAt(indexPos(node, level));
  }

  /**
   * Whether a node is NULL
   * @param node
   * @return True if the node is NULL
   */
  final private boolean isNullNode(long node) {
    return (node & ~MASK_NODE_DELETE_MARKER) == 0;
  }


  /**
   * Whether a node is a marker
   * @param next
   * @return True if the node is a makrer
   */
  final private boolean isMarker(long next) {
    return (next & MASK_NODE_DELETE_MARKER) != 0;
  }

  /**
   * Get real node of a coded marker.
   * @param marker
   * @return The real node
   */
  final private long markerDecode(long marker) {
    return MASK_REAL_NODE & marker;
  }

  /**
   * Encode a node as marker
   * @param node
   * @return The marked node
   */
  final private long markerEncode(long node) {
    return (node | MASK_NODE_DELETE_MARKER);
  }


  /**
   * Validate the reference
   * @param ref
   */
  final private void ensureReferenceValid(long ref) {
    if (ref <= HEAD_NODE || ref > HEADER_M_REFERENCE) {
      throw new RuntimeException("Bug! refNode=" + ref);
    }
  }


  final private long headerReference(long header) {
    return header & HEADER_M_REFERENCE;
  }

  final private long headerEstimateNodeSize(long header) {
    return estimatedNodeSize(headerDataLength(header), headerLevel(header),
        headerOnPage(header));
  }

  final private boolean isNodeReference(long header) {
    return headerReference(header) != NULL_NODE;
  }

  final private long dataPosWithLevel(long node, int level) {
    return indexPos(node, level) + 1;
  }

  final private long dataPos(long node, long header) {
    return dataPosWithLevel(node, headerLevel(header));
  }

  /**
   * Get position of node's header
   * @param node
   * @return
   */
  final private long nodePosHeader(long node) {
    return node & MASK_REAL_NODE;
  }

  /**
   * Get position of node's base next
   * @param node
   * @return
   */
  final private long nodePosNext(long node) {
    return (node & MASK_REAL_NODE) + 1;
  }

  /**
   * Get position of node's index area
   * @param node
   * @return
   */
  final private long nodePosIndex(long node) {
    return (node & MASK_REAL_NODE) + 2;
  }

  /**
   * @param node
   * @param level
   * @return
   */
  final private long indexPos(long node, int level) {
    return nodePosIndex(node) + level - 1;
  }

  /**
   * Compare and set the node's next from n to z
   * @param node
   * @param n
   * @param z
   * @return
   */
  final private boolean nodeCasSetNext(long node, long n, long z) {
    assert !isNullNode(node);
    return pageCompareAndSet(nodePosNext(node), n, z);
  }

  /**
   * Set the node's base next
   * @param node
   * @param next
   */
  final private void nodeSetNext(long node, long next) {
    pageSet(nodePosNext(node), next);
  }

  /**
   * Set the node's header
   * @param node
   * @param header
   */
  final private void nodeSetHeader(long node, long header) {
    pageSet(nodePosHeader(node), header);
  }

  /**
   * Accumulate trash info into memory usage summary
   * @param node
   * @param header
   * @param seenOffset
   */
  final private void trackTrashNode(long node, long header) {
    if (isNodeReference(header)) {
      header = nodeHeader(headerReference(header));
    }

    this.trashData.addAndGet(headerEstimateNodeSize(header));
    if (!headerOnPage(header)) {
      long pos = dataPos(node, header);
      UnsafeAccess.toLong(getDataOnPage(pos),getLocalOffsetOnPage(pos));
    }
  }

  /**
   * Try to change a node's offset to delete state
   * @param node
   * @param seenOffset
   * @return
   */
  final private boolean nodeCasToDeleted(long node, long header) {
    if (headerDeleted(header) == NODE_DELETED) {
      throw new RuntimeException("Bug:Impossible.");
    }
    long newHeader = header | HEADER_M_DELETED;
    long pos = nodePosHeader(node);
    boolean ret = pageCompareAndSet(pos, header, newHeader);
    if (ret) {
      trackTrashNode(node, header);
    }
    return ret;
  }

  /**
   * Try to change a node's offset to a reference to another node
   *
   * @param node
   * @param seenOffset
   * @param refNode
   * @return
   */
  private boolean nodeCasToReference(long node, long oldHeader, long refNode) {
    if (headerDeleted(oldHeader)) {
      throw new RuntimeException("Bug: Impossible.");
    }
    ensureReferenceValid(refNode);
    long newHeader = (refNode & HEADER_M_REFERENCE) | (oldHeader & (~HEADER_M_REFERENCE));
    boolean ret = pageCompareAndSet(nodePosHeader(node), oldHeader, newHeader);
    if (ret) {
      trackTrashNode(node, oldHeader);
    }
    return ret;
  }


  /**
   * Try to set the node's base next to a marker
   * @param node
   * @param f
   * @return
   */
  final private boolean nodeAppendMarker(long node, long f) {
    nodeEnsureNotMarker(node);
    long fWithMarker = markerEncode(f);
    return nodeCasSetNext(node, f, fWithMarker);
  }

  /**
   * Get a node's index level
   * @param node
   * @return
   */
  final private int nodeLevel(long node) {
    return headerLevel(nodeHeader(node));
  }

  /**
   * Get the node's header
   * @param node
   * @return
   */
  final private long nodeHeader(long node) {
    long ret = getAt(nodePosHeader(node));
    return ret;
  }

  /**
   * If a deleted is observed by other threads, they will call this method to
   * help for this node's deletion
   *
   * @param node
   * @param b
   * @param f
   */
  final private void nodeHelpDelete(long node, long b, long f) {
    long n_next = nodeGetNext(node);
    long b_next = nodeGetNext(b);
    if (f == n_next && node == b_next) {
      if (!isMarker(f)) { // not already marked
        nodeAppendMarker(node, f);
      } else { // already marked
        long new_b_next = isMarker(node) ? f : markerDecode(f);
        nodeCasSetNext(b, node, new_b_next);
      }
    }
  }

  /**
   * Get node's base next
   * @param node
   * @return
   */
  final private long nodeGetNext(long node) {
    assert !isNullNode(node);
    long next =  getAt(nodePosNext(node));
    return next;
  }

  /**
   * Is node deleted
   * @param node
   * @return
   */
  final private boolean nodeDeleted(long node) {
    assert !isNullNode(node);
    long header = nodeHeader(node);
    return headerDeleted(header);
  }

  // //////////////////////////////////////////
  // manipulating methods for header
  // //////////////////////////////////////////

  /**
   * Is the header indicate that node is on data page
   * @param header
   * @return
   */
  final private boolean headerOnPage(long header) {
    return (header & HEADER_M_PAGED) != 0 ? ON_HEAP : ON_PAGE;
  }

  final private boolean headerDeleted(long header) {
    return (header & HEADER_M_DELETED) != 0 ? NODE_DELETED : NODE_PRESENT;
  }

  /**
   * Get the node's index level from header
   * @param header
   * @return
   */
  final private int headerLevel(long header) {
    return (int)((header & HEADER_M_LEVEL) >> HEADER_SHIFT_LEVEL);
  }

  final private int headerDataLength(long header) {
    return (int)((header & HEADER_M_LENGTH) >> HEADER_SHIFT_LENGTH);
  }

  /**
   * Encode a header by parameters below
   */
  final private long headerMake(boolean deleted, boolean onPage,
      long heapKVOffset, long dataLength, int level) {
    long ret = 0;
    ret = (deleted == NODE_DELETED) ? (ret | HEADER_M_DELETED) : ret;
    ret = (onPage == ON_HEAP) ? (ret | HEADER_M_PAGED) : ret;
    ret = ret | (((long)level) << HEADER_SHIFT_LEVEL);
    ret = ret | (((onPage == ON_PAGE) ?  dataLength : heapKVOffset) << HEADER_SHIFT_LENGTH);
    return ret;
  }

  final private int alignOffsetToLocalOffset(int i) {
    return i << 3;
  }

  final private long getAt(long globalOffset) {
    byte[] data = getDataOnPage(globalOffset);
    int pos = getLocalOffsetOnPage(globalOffset);
    return UnsafeAccess.toLongVolatile(data, pos);
  }

  /**
   * CAS data at position i
   * @param i
   * @param expect The  data expected to be
   * @param update
   * @return
   */
  private boolean pageCompareAndSet(long i, long expect, long update) {
    byte[] data = getDataOnPage(i);
    int pos = getLocalOffsetOnPage(i);
    return UnsafeAccess.compareAndSetLong(data, pos, expect, update);
  }

  /**
   * Set value at position i directly
   * @param i
   * @param value
   */
  private void pageSet(long i, long value) {
    byte[] data = getDataOnPage(i);
    int pos = getLocalOffsetOnPage(i);
    UnsafeAccess.putLongVolatile(data, pos, value);
  }

  /**
   * Get how many byte would the node use
   *
   */
  private int estimatedNodeSize(int kvSize, int level, boolean onPage) {
    int longCount = 2; // for header & next;
    longCount += level; // for level
    if (onPage == ON_PAGE) {
      if (kvSize <= 0) {
        throw new RuntimeException("Impossible: bug");
      }
      longCount += ((kvSize - 1) >> 3) + 1;
    } else {
      longCount += 1; // for data length of on heap kv
    }
    return longCount << 3;
  }

  /**
   * Create a node and store key value into map
   * @param key
   * @param value
   * @param next the node's next node at base-level
   * @return
   */
  private long createNode(K key, V value, long next) {
    int level = randomLevel();
    boolean onPage;
    int kvSize = typeHelper.getCompactedSize(key, value);
    long node = NULL_NODE;

    onPage = kvSize > settings.heapKVThreshold ? ON_HEAP : ON_PAGE;
    int nodeSize = estimatedNodeSize(kvSize, level, onPage);
    node = allocatePageSpace(nodeSize, level);
    long heapKVOffset = NULL_NODE;
    if (onPage == ON_PAGE) {
      putDataOnPage(key, value, node, level, kvSize);
    } else {
      heapKVOffset = putDataOnHeap(key, value, node, level, kvSize);
    }
    long header = headerMake(NODE_PRESENT, onPage, heapKVOffset, kvSize, level);
    nodeSetHeader(node, header);
    nodeSetNext(node, next);

    if (onPage == ON_HEAP) {
      aliveHeapKVCount.incrementAndGet();
      aliveHeapKVSize.addAndGet(kvSize);
    }
    return node;
  }

  private int estimatedNodeSizeHeadNodeSize() {
    int dataLength = 1;
    return estimatedNodeSize(dataLength, LEVEL_LIMIT, ON_PAGE);
  }

  private long createHeadNode() {
    int dataLength = 1;
    long header = headerMake(NODE_PRESENT, ON_PAGE, 0, dataLength, LEVEL_LIMIT);
    int nodeSize = estimatedNodeSizeHeadNodeSize();
    long node = allocatePageSpace(nodeSize, LEVEL_LIMIT);
    assert node == HEAD_NODE;
    nodeSetHeader(node, header);
    return node;
  }

  /**
   * Allocate space for a new node
   * @param size How many integers this node needs
   * @return
   */
  private long allocatePageSpace(int size, int level) {
    long node = dataPageManager.allocate(size);
    int inclusiveStart = getLocalOffsetOnPage(nodePosHeader(node));
    int exclusiveEnd = getLocalOffsetOnPage(indexPos(node, level) + 1);
    byte[] data = getDataOnPage(node);
    CCSMapUtils.clear(data, inclusiveStart, (exclusiveEnd - inclusiveStart));
    return node;
  }

  /**
   * Generate a random level, the random level won't be larger than the map's
   * highest level + 1
   *
   * @return a random level
   */
  private int randomLevel() {
    int x = randomSeed;
    x ^= x << 13;
    x ^= x >>> 17;
    randomSeed = x ^= x << 5;
    if ((x & 0x8001) != 0) // test highest and lowest bits
      return 0;
    int level = 1;
    while (((x >>>= 1) & 1) != 0)
      ++level;
    int maxLevel = getHeadHighestLevel();
    return (level > maxLevel + 1) ? maxLevel + 1 : level;
  }

  /**
   * Get data page of a given offset
   * @param offset
   * @return
   */
  final private byte[] getDataOnPage(long globalOffset) {
    return dataPageManager.getPage(globalOffset);
  }

  final private int getLocalOffsetOnPage(long globalOffset) {
    int inPageOffset = dataPageManager.getInPageOffset(globalOffset);
    return alignOffsetToLocalOffset(inPageOffset);
  }


  /**
   * Place key and value on to the page, return a global offset to indicate
   * where the data is.
   *
   * @param key
   * @param value
   * @param sz The size of compacted key-value
   * @return Global offset indicating where the data is.
   */
  final private void putDataOnPage(K key, V value, long node, int level, int kvsize) {
    long dataPos = dataPosWithLevel(node, level);
    byte[] data = getDataOnPage(dataPos);
    int localOffset = getLocalOffsetOnPage(dataPos);
    typeHelper.compact(key, value, data,
        localOffset, kvsize);
  }

  /**
   * Place key-value on heap, return a global offset to indicate where the data
   * is
   *
   * @param key
   * @param value
   * @return Global offset indicating where the data is.
   */
  final private long putDataOnHeap(K key, V value, long node, int level, int kvSize) {
    long offset = heapKVManager.allocate(1);
    Object[] a = heapKVManager.getPage(offset);
    KVPair<K, V> newKV = new KVPair<K, V>(key, value);
    a[heapKVManager.getInPageOffset(offset)] = newKV;
    // put on heap kv's size on the first data's position of page
    pageSet(dataPosWithLevel(node, level), kvSize);
    return offset;
  }

  /**
   * Get key-value by offset
   * @param offset
   * @return
   */
  @SuppressWarnings("unchecked")
  final private KVPair<K, V> getOnHeapKV(long offset) {
    Object[] a = heapKVManager.getPage(offset);
    int lo = heapKVManager.getInPageOffset(offset);
    return (KVPair<K, V>)a[lo];
  }

  public V remove(Object key) {
    return doRemove(key, null);
  }

  public void clear() {
    while (!isEmpty()) {
      doRemoveFirstEntry();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Collection<V> values() {
    Values<V> vs = values;
    return (vs != null) ? vs : (values = new Values(this));
  }

  public Set<Map.Entry<K, V>> entrySet() {
    EntrySet<K, V> es = entrySet;
    return (es != null) ? es : (entrySet = new EntrySet<K, V>(this));
  }

  public Map.Entry<K, V> lowerEntry(K key) {
    return getNear(key, LT);
  }

  public K lowerKey(K key) {
    long n = findNear(key, LT);
    return (isNullNode(n)) ? null : nodeGetKV2(n).key();
  }

  public Map.Entry<K, V> floorEntry(K key) {
    return getNear(key, LT | EQ);
  }

  public K floorKey(K key) {
    Map.Entry<K, V> entry = floorEntry(key);
    return entry == null ? null : entry.getKey();
  }

  public Map.Entry<K, V> ceilingEntry(K key) {
    return getNear(key, GT | EQ);
  }

  public K ceilingKey(K key) {
    Map.Entry<K, V> entry = ceilingEntry(key);
    return entry == null ? null : entry.getKey();
  }

  public Map.Entry<K, V> higherEntry(K key) {
    return getNear(key, GT);
  }

  public K higherKey(K key) {
    Map.Entry<K, V> entry = getNear(key, GT);
    return (entry == null) ? null : entry.getKey();
  }

  /**
   * Find first node of the map. If no node in this map, return NULL_NODE
   * @return
   */
  private long findFirst() {
    for (;;) {
      long b = HEAD_NODE;
      long n = nodeGetNext(b);
      if (isNullNode(n)) {
        return NULL_NODE;
      }
      if (!nodeDeleted(n)) {
        return n;
      }
      nodeHelpDelete(n, b, nodeGetNext(n));
    }
  }

  /**
   * Find last node of the map. If no node in this map, return NULL_NODE
   *
   * @return
   */
  private long findLast() {
    long q = HEAD_NODE;
    int level = getHeadHighestLevel();
    for (;;) {
      long r;
      if ((r = indexGetNext(q, level)) != NULL_NODE) {
        if (nodeDeleted(r)) {
          indexUnlink(q, r, level);
          q = HEAD_NODE; // restart
          level = getHeadHighestLevel();
        } else {
          q = r;
        }
      } else if ((level = level - 1) > 0) {
        continue;
      } else {
        long b = q;
        long n = nodeGetNext(b);
        for (;;) {
          if (isNullNode(n)) {
            return (b == HEAD_NODE) ? NULL_NODE : b;
          }
          long f = nodeGetNext(n);
          if (n != nodeGetNext(b)) { // inconsistent read
            break;
          }
          long n_header = nodeHeader(n);
          if (headerDeleted(n_header)) { // n is deleted
            nodeHelpDelete(n, b, f);
            break;
          }
          if (isMarker(n) || nodeDeleted(b)) { // b is deleted
            break;
          }
          b = n;
          n = f;
        }
        q = HEAD_NODE;
        level = getHeadHighestLevel();
      }
    }
  }

  /**
   * Get the entry near the key. The node's relation to the key is specified by
   * the rel
   *
   * @param key
   * @param rel LT, EQ, GT, LT|EQ or GT|EQ
   * @return
   */
  private AbstractMap.SimpleImmutableEntry<K, V> getNear(K key, int rel) {
    for (;;) {
      long n = findNear(key, rel);
      if (isNullNode(n)) {
        return null;
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)) {
        KVPair<K, V> kv = nodeGetKV(n, n_header);
        return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
      }
    }
  }

  /**
   * Get the node near the key. The node's relation to the key is specified by
   * the rel
   *
   * @param kkey
   * @param rel
   * @return
   */
  private long findNear(K kkey, int rel) {
    for (;;) {
      long b = findPredecessor(kkey);
      long n = nodeGetNext(b);
      for (;;) {
        if (isNullNode(n)) {
          return ((rel & LT) == 0 || b == HEAD_NODE) ? NULL_NODE : b;
        }
        long f = nodeGetNext(n);
        if (n != nodeGetNext(b)) { // inconsistent read
          break;
        }
        long n_header = nodeHeader(n);
        if (headerDeleted(n_header)) { // n is deleted
          nodeHelpDelete(n, b, f);
          break;
        }
        if (isMarker(n) || nodeDeleted(b)) { // b is deleted
          break;
        }
        int c = compareKeyAt(kkey, n, n_header);
        if ((c == 0 && (rel & EQ) != 0) || (c < 0 && (rel & LT) == 0)) {
          return n;
        }
        if (c <= 0 && (rel & LT) != 0) {
          return (b == HEAD_NODE) ? NULL_NODE : b;
        }
        b = n;
        n = f;
      }
    }
  }

  public Entry<K, V> firstEntry() {
    for (;;) {
      long n = findFirst();
      if (isNullNode(n)) {
        return null;
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)) {
        KVPair<K, V> kv = nodeGetKV(n, n_header);
        return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
      }
    }
  }

  public Map.Entry<K, V> lastEntry() {
    for (;;) {
      long n = findLast();
      if (isNullNode(n)) {
        return null;
      }
      long n_header = nodeHeader(n);
      if (!headerDeleted(n_header)) {
        KVPair<K, V> kv = nodeGetKV(n, n_header);
        return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
      }
    }
  }

  /**
   * Remove first entry in the map
   * @return Removed entry. If no entry to remove, return null
   */
  private Map.Entry<K, V> doRemoveFirstEntry() {
    for (;;) {
      long b = HEAD_NODE;
      long n = nodeGetNext(b);
      if (isNullNode(n)) {
        return null;
      }
      long f = nodeGetNext(n);
      if (n != nodeGetNext(b)) { // inconsistent read
        continue;
      }
      long n_header = nodeHeader(n);
      if (headerDeleted(n_header)) {
        nodeHelpDelete(n, b, f);
        continue;
      }
      if (!nodeCasToDeleted(n, n_header)) {
        continue;
      }
      if (!nodeAppendMarker(n, f) || !nodeCasSetNext(b, n, f)) {
        findFirst(); // retry
      }
      clearIndexToFirst();
      KVPair<K, V> kv = nodeGetKV(n, n_header);
      return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
    }
  }

  /**
   * Clears out index nodes associated with deleted first entry.
   */
  private void clearIndexToFirst() {
    for (;;) {
      long q = HEAD_NODE;
      int level = getHeadHighestLevel();
      for (;;) {
        long r = indexGetNext(q, level);
        if (r != NULL_NODE && nodeDeleted(r) && !indexUnlink(q, r, level)) {
          break; // retry
        }
        if ((level = level - 1) <= 0) {
          if (indexGetNext(HEAD_NODE, getHeadHighestLevel()) == NULL_NODE) {
            tryReduceLevel();
          }
          return;
        }
      }
    }
  }

  /**
   * Get the node before last node by traversing index
   * @return The node before the last node.
   */
  private long findPredecessorOfLast() {
    for (;;) {
      long q = HEAD_NODE;
      int level = getHeadHighestLevel();
      for (;;) {
        long r;
        if ((r = indexGetNext(q, level)) != NULL_NODE) {
          if (nodeDeleted(r)) {
            indexUnlink(q, r, level);
            break; // lost race, restart
          }
          // proceed as far across as possible without overshooting
          if (!isNullNode(nodeGetNext(r))) {
            q = r;
            continue;
          }
        }
        if ((level = level - 1) <= 0) {
          return q;
        }
      }
    }
  }

  /**
   * Remove last entry in the map
   * @return removed entry
   */
  private Map.Entry<K, V> doRemoveLastEntry() {
    for (;;) {
      long b = findPredecessorOfLast();
      long n = nodeGetNext(b);
      if (isNullNode(n)) {
        if (b == HEAD_NODE) {
          return null;
        } else {
          continue; // all b's successors are deleted; retry
        }
      }
      // Traverse next
      for (;;) {
        long f = nodeGetNext(n);
        if (n != nodeGetNext(b)) { // inconsistent read
          break;
        }
        long n_header = nodeHeader(n);
        if (headerDeleted(n_header)) { // n is deleted
          nodeHelpDelete(n, b, f);
          break;
        }
        if (isMarker(n) || nodeDeleted(b)) { // b is deleted
          break;
        }
        if (!isNullNode(f)) {
          b = n;
          n = f;
          continue;
        }
        if (!nodeCasToDeleted(n, n_header)) {
          break;
        }
        KVPair<K, V> kv = nodeGetKV(n, n_header);
        if (!nodeAppendMarker(n, f) || !nodeCasSetNext(b, n, f)) {
          findNode(kv.key()); // Retry via findNode
        } else {
          findPredecessor(kv.key()); // clean index
          if (indexGetNext(HEAD_NODE, getHeadHighestLevel()) == NULL_NODE) {
            tryReduceLevel();
          }
        }
        return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
      }
    }
  }


  public Map.Entry<K, V> pollFirstEntry() {
    return doRemoveFirstEntry();
  }

  public java.util.Map.Entry<K, V> pollLastEntry() {
    return doRemoveLastEntry();
  }

  /**
   * Wrapper for key comparator
   * @param <T>
   */
  private class KeyComparator<T> implements Comparator<T> {
    @SuppressWarnings("unchecked")
    public int compare(T o1, T o2) {
      K k1 = (K) o1;
      K k2 = (K) o2;
      return typeHelper.compare(k1, k2);
    }
  }

  public Comparator<? super K> comparator() {
    return new KeyComparator<K>();
  }

  public K firstKey() {
    long n = findFirst();
    if (isNullNode(n)) {
      throw new NoSuchElementException();
    }
    return nodeGetKV2(n).key();
  }

  public K lastKey() {
    long n = findLast();
    if (isNullNode(n)) {
      throw new NoSuchElementException();
    }
    return nodeGetKV2(n).key();
  }

  public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive,
      K toKey, boolean toInclusive) {
    if (fromKey == null || toKey == null) {
      throw new NullPointerException();
    }
    return new SubMap<K, V>(this, fromKey, fromInclusive, toKey, toInclusive,
        false);
  }

  public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    if (toKey == null) {
      throw new NullPointerException();
    }
    return new SubMap<K, V>(this, null, false, toKey, inclusive, false);
  }

  public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    if (fromKey == null) {
      throw new NullPointerException();
    }
    return new SubMap<K, V>(this, fromKey, inclusive, null, false, false);
  }

  public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  public ConcurrentNavigableMap<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }

  public ConcurrentNavigableMap<K, V> descendingMap() {
    ConcurrentNavigableMap<K, V> dm = descendingMap;
    return (dm != null) ? dm : (descendingMap = new SubMap<K, V>(this, null,
        false, null, false, true));
  }

  public NavigableSet<K> navigableKeySet() {
    return keySet();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public NavigableSet<K> keySet() {
    KeySet<K> ks = keySet;
    return (ks != null) ? ks : (keySet = new KeySet(this));
  }

  public NavigableSet<K> descendingKeySet() {
    return descendingMap().navigableKeySet();
  }

  // Base of iterator classes:
  abstract class Iter<T> implements Iterator<T> {
    // the last node returned by next()
    long lastReturned;

    // the next node to return from next();
    long next;
    // the next node's offset
    long next_header;

    // Initializes ascending iterator for entire range.
    Iter() {
      for (;;) {
        next = findFirst();
        if (isNullNode(next)) {
          break;
        }
        long x = nodeHeader(next);
        if (!headerDeleted(x)) {
          next_header = x;
          break;
        }
      }
    }

    public final boolean hasNext() {
      return !isNullNode(next);
    }

    final void advance() {
      if (isNullNode(next)) {
        throw new NoSuchElementException();
      }
      lastReturned = next;
      for (;;) {
        next = nodeGetNext(next);
        if (isNullNode(next)) {
          break;
        }
        long x = nodeHeader(next);
        if (!headerDeleted(x)) {
          next_header = x;
          break;
        }
      }
    }

    public void remove() {
      long l = lastReturned;
      if (isNullNode(l)) {
        throw new IllegalStateException();
      }
      // It would not be worth all of the overhead to directly
      // unlink from here. Using remove is fast enough.
      CompactedConcurrentSkipListMap.this.remove(nodeGetKV2(l).key());
      lastReturned = NULL_NODE;
    }
  }

  final class ValueIterator extends Iter<V> {
    public V next() {
      KVPair<K, V> kv = nodeGetKV(next, next_header);
      advance();
      return kv.value();
    }
  }

  final class KeyIterator extends Iter<K> {
    public K next() {
      KVPair<K, V> kv = nodeGetKV(next, next_header);
      advance();
      return kv.key();
    }
  }

  final class EntryIterator extends Iter<Map.Entry<K, V>> {
    public Entry<K, V> next() {
      KVPair<K, V> kv = nodeGetKV(next, next_header);
      advance();
      return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
    }
  }

  Iterator<K> keyIterator() {
    return new KeyIterator();
  }

  Iterator<V> valueIterator() {
    return new ValueIterator();
  }

  Iterator<Map.Entry<K, V>> entryIterator() {
    return new EntryIterator();
  }

  static final class SubMap<K, V> extends AbstractMap<K, V> implements
      ConcurrentNavigableMap<K, V>, Cloneable, Serializable {
    private static final long serialVersionUID = 1L;
    // Underlying map
    private final CompactedConcurrentSkipListMap<K, V> m;
    // lower bound key, or null if from start
    private final K lo;
    // upper bound key, or null if to end
    private final K hi;
    // inclusion flag for lo
    private final boolean loInclusive;
    // inclusion flag for hi
    private final boolean hiInclusive;
    // direction
    private final boolean isDescending;
    // Lazily initialized view holders
    private transient KeySet<K> keySetView;
    private transient Collection<V> valuesView;
    private transient Set<Map.Entry<K, V>> entrySetView;

    public SubMap(CompactedConcurrentSkipListMap<K, V> map, K fromKey,
        boolean fromInclusive, K toKey, boolean toInclusive,
        boolean isDescending) {
      if (fromKey != null && toKey != null
          && map.typeHelper.compare(fromKey, toKey) > 0) {
        throw new IllegalArgumentException("inconsistent range");
      }
      this.m = map;
      this.lo = fromKey;
      this.hi = toKey;
      this.loInclusive = fromInclusive;
      this.hiInclusive = toInclusive;
      this.isDescending = isDescending;
    }

    private boolean tooLow(K key) {
      if (lo != null) {
        int c = m.compare(key, lo);
        if (c < 0 || (c == 0 && !loInclusive)) {
          return true;
        }
      }
      return false;
    }

    private boolean tooLowNode(long node) {
      assert !m.isNullNode(node);
      if (lo != null) {
        int c = m.compareKeyAt(lo, node);
        if (c > 0 || (c == 0 && !loInclusive)) {
          return true;
        }
      }
      return false;
    }

    private boolean tooHigh(K key) {
      if (hi != null) {
        int c = m.compare(key, hi);
        if (c > 0 || (c == 0 && !hiInclusive)) {
          return true;
        }
      }
      return false;
    }

    private boolean tooHighNode(long node) {
      if (hi != null) {
        int c = m.compareKeyAt(hi, node);
        if (c < 0 || (c == 0 && !hiInclusive)) {
          return true;
        }
      }
      return false;
    }

    private boolean keyInBounds(K key) {
      return !tooLow(key) && !tooHigh(key);
    }

    private boolean nodeInBounds(long node) {
      return !tooLowNode(node) && !tooHighNode(node);
    }

    private void checkKeyBounds(K key) throws IllegalArgumentException {
      if (key == null) {
        throw new NullPointerException();
      }
      if (!keyInBounds(key)) {
        throw new IllegalArgumentException("key out of range");
      }
    }

    // Returns true if node key is less than upper bound of range
    private boolean isBeforeEnd(long node) {
      if (m.isNullNode(node)) {
        return false;
      }
      if (hi == null) {
        return true;
      }
      return !tooHighNode(node);
    }

    private Map.Entry<K, V> getNearEntry(K key, int rel) {
      if (isDescending) { // adjust relation for direction
        if ((rel & LT) == 0) {
          rel |= LT;
        } else {
          rel &= ~LT;
        }
      }
      if (tooLow(key)) {
        return ((rel & LT) != 0) ? null : lowestEntry();
      }
      if (tooHigh(key)) {
        return ((rel & LT) != 0) ? highestEntry() : null;
      }
      for (;;) {
        long n = m.findNear(key, rel);
        if (m.isNullNode(n) || !nodeInBounds(n)) {
          return null;
        }
        long n_header = m.nodeHeader(n);
        if (!m.headerDeleted(n_header)) {
          KVPair<K, V> kv = m.nodeGetKV(n, n_header);
          return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
        }
      }
    }

    private K getNearKey(K key, int rel) {
      Map.Entry<K, V> entry = getNearEntry(key, rel);
      return entry == null ? null : entry.getKey();
    }

    private long loNode() {
      if (lo == null) {
        return m.findFirst();
      } else if (loInclusive) {
        return m.findNear(lo, GT | EQ);
      } else {
        return m.findNear(lo, GT);
      }
    }

    private long hiNode() {
      if (hi == null) {
        return m.findLast();
      } else if (hiInclusive) {
        return m.findNear(hi, LT | EQ);
      } else {
        return m.findNear(hi, LT);
      }
    }

    private Map.Entry<K, V> lowestEntry() {
      for (;;) {
        long n = loNode();
        if (!isBeforeEnd(n)) {
          return null;
        }
        long n_header = m.nodeHeader(n);
        if (m.headerDeleted(n_header)) {
          return null;
        }
        KVPair<K, V> kv = m.nodeGetKV(n, n_header);
        return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
      }
    }

    private K lowestKey() {
      Map.Entry<K, V> entry = lowestEntry();
      if (entry != null) {
        return entry.getKey();
      }
      throw new NoSuchElementException();
    }

    private Map.Entry<K, V> highestEntry() {
      for (;;) {
        long n = hiNode();
        if (m.isNullNode(n) || !nodeInBounds(n)) {
          return null;
        }
        long n_header = m.nodeHeader(n);
        if (!m.headerDeleted(n_header)) {
          KVPair<K, V> kv = m.nodeGetKV(n, n_header);
          return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), kv.value());
        }
      }
    }

    private K highestKey() {
      Map.Entry<K, V> entry = highestEntry();
      if (entry != null) {
        return entry.getKey();
      }
      throw new NoSuchElementException();
    }

    @Override
    public V put(K key, V value) {
      checkKeyBounds(key);
      return m.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
      if (key == null) {
        throw new NullPointerException();
      }
      K k = (K) key;
      return ((!keyInBounds(k)) ? null : m.get(k));
    }

    public V putIfAbsent(K key, V value) {
      checkKeyBounds(key);
      return m.putIfAbsent(key, value);
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
      K k = (K) key;
      return (!keyInBounds(k)) ? null : m.remove(k);
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object key, Object value) {
      K k = (K) key;
      return keyInBounds(k) && m.remove(k, value);
    }

    private Map.Entry<K, V> removeHighest() {
      for (;;) {
        long n = hiNode();
        if (m.isNullNode(n)) {
          return null;
        }
        if (!nodeInBounds(n)) {
          return null;
        }
        KVPair<K, V> kv = m.nodeGetKV2(n);
        V v = m.doRemove(kv.key(), null);
        if (v != null) {
          return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), v);
        }
      }
    }

    private Map.Entry<K, V> removeLowest() {
      for (;;) {
        long n = loNode();
        if (m.isNullNode(n)) {
          return null;
        }
        if (!nodeInBounds(n)) {
          return null;
        }
        KVPair<K, V> kv = m.nodeGetKV2(n);
        V v = m.doRemove(kv.key(), null);
        if (v != null) {
          return new AbstractMap.SimpleImmutableEntry<K, V>(kv.key(), v);
        }
      }
    }

    public boolean replace(K key, V oldValue, V newValue) {
      checkKeyBounds(key);
      ;
      return m.replace(key, oldValue, newValue);
    }

    public V replace(K key, V value) {
      checkKeyBounds(key);
      return m.replace(key, value);
    }

    public Map.Entry<K, V> lowerEntry(K key) {
      return getNearEntry(key, LT);
    }

    public K lowerKey(K key) {
      return getNearKey(key, LT);
    }

    public Map.Entry<K, V> floorEntry(K key) {
      return getNearEntry(key, (LT | EQ));
    }

    public K floorKey(K key) {
      return getNearKey(key, (LT | EQ));
    }

    public Map.Entry<K, V> ceilingEntry(K key) {
      return getNearEntry(key, (GT | EQ));
    }

    public K ceilingKey(K key) {
      return getNearKey(key, (GT | EQ));
    }

    public Map.Entry<K, V> higherEntry(K key) {
      return getNearEntry(key, (GT));
    }

    public K higherKey(K key) {
      return getNearKey(key, (GT));
    }

    public java.util.Map.Entry<K, V> firstEntry() {
      return isDescending ? highestEntry() : lowestEntry();
    }

    public java.util.Map.Entry<K, V> lastEntry() {
      return isDescending ? lowestEntry() : highestEntry();
    }

    public Map.Entry<K, V> pollFirstEntry() {
      return isDescending ? removeHighest() : removeLowest();
    }

    public java.util.Map.Entry<K, V> pollLastEntry() {
      return isDescending ? removeLowest() : removeHighest();
    }

    public Comparator<? super K> comparator() {
      Comparator<? super K> cmp = m.comparator();
      if (isDescending) {
        return Collections.reverseOrder(cmp);
      } else {
        return cmp;
      }
    }

    public K firstKey() {
      return isDescending ? highestKey() : lowestKey();
    }

    public K lastKey() {
      return isDescending ? lowestKey() : highestKey();
    }

    private SubMap<K, V> newSubMap(K fromKey, boolean fromInclusive, K toKey,
        boolean toInclusive) {
      if (isDescending) {
        K tk = fromKey;
        fromKey = toKey;
        toKey = tk;
        boolean ti = fromInclusive;
        fromInclusive = toInclusive;
        toInclusive = ti;
      }
      if (lo != null) {
        if (fromKey == null) {
          fromKey = lo;
          fromInclusive = loInclusive;
        } else {
          int c = m.compare(fromKey, lo);
          if (c < 0 || (c == 0 && !loInclusive && fromInclusive)) {
            throw new IllegalArgumentException("key out of range");
          }
        }
      }
      if (hi != null) {
        if (toKey == null) {
          toKey = hi;
          toInclusive = hiInclusive;
        } else {
          int c = m.compare(toKey, hi);
          if (c > 0 || (c == 0 && !hiInclusive && toInclusive)) {
            throw new IllegalArgumentException("Key out of range");
          }
        }
      }
      return new SubMap<K, V>(m, fromKey, fromInclusive, toKey, toInclusive,
          isDescending);
    }

    public ConcurrentNavigableMap<K, V> subMap(K fromKey,
        boolean fromInclusive, K toKey, boolean toInclusive) {
      if (fromKey == null || toKey == null) {
        throw new NullPointerException();
      }
      return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
      if (toKey == null) {
        throw new NullPointerException();
      }
      return newSubMap(null, false, toKey, inclusive);
    }

    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
      if (fromKey == null) {
        throw new NullPointerException();
      }
      return newSubMap(fromKey, inclusive, null, false);
    }

    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
      return subMap(fromKey, true, toKey, false);
    }

    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
      return headMap(toKey, false);
    }

    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
      return tailMap(fromKey, true);
    }

    public ConcurrentNavigableMap<K, V> descendingMap() {
      return new SubMap<K, V>(m, lo, loInclusive, hi, hiInclusive,
          !isDescending);
    }

    public NavigableSet<K> navigableKeySet() {
      return keySet();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public NavigableSet<K> keySet() {
      KeySet<K> ks = keySetView;
      return (ks != null) ? ks : (keySetView = new KeySet(this));
    }

    public NavigableSet<K> descendingKeySet() {
      return descendingMap().keySet();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
      Set<Map.Entry<K, V>> es = entrySetView;
      return (es != null) ? es : (entrySetView = new EntrySet(this));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Collection<V> values() {
      Collection<V> vs = valuesView;
      return (vs != null) ? vs : (valuesView = new Values(this));
    }

    @Override
    public int size() {
      int count = 0;
      for (long n = loNode(); isBeforeEnd(n); n = m.nodeGetNext(n)) {
        if (!m.nodeDeleted(n)) {
          ++count;
        }
      }
      return count;
    }

    @Override
    public void clear() {
      for (long n = loNode(); isBeforeEnd(n); n = m.nodeGetNext(n)) {
        if (!m.nodeDeleted(n)) {
          KVPair<K, V> kv = m.nodeGetKV2(n);
          m.remove(kv.key());
        }
      }
    }

    @Override
    public boolean containsValue(Object value) {
      if (value == null) {
        throw new NullPointerException();
      }
      for (long n = loNode(); isBeforeEnd(n); n = m.nodeGetNext(n)) {
        long n_header = m.nodeHeader(n);
        if (!m.headerDeleted(n_header)) {
          KVPair<K, V> kv = m.nodeGetKV2(n);
          if (value.equals(kv.value())) {
            return true;
          }
        }
      }
      return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
      K k = (K) key;
      return keyInBounds(k) && m.containsKey(key);
    }

    public Iterator<Map.Entry<K, V>> entryIterator() {
      return new SubMapEntryIterator();
    }

    @Override
    public boolean isEmpty() {
      return !isBeforeEnd(loNode());
    }

    public Iterator<K> keyIterator() {
      return new SubMapKeyIterator();
    }

    public Iterator<V> valueIterator() {
      return new SubMapValueIterator();
    }

    abstract class SubMapIter<T> implements Iterator<T> {
      long lastReturned;
      KVPair<K, V> lastReturned_KV; // just like cache
      long next;
      long next_header;

      public SubMapIter() {
        for (;;) {
          next = isDescending ? hiNode() : loNode();
          if (m.isNullNode(next)) {
            break;
          }
          long x = m.nodeHeader(next);
          if (!m.headerDeleted(next_header)) {
            if (!nodeInBounds(next)) {
              next = NULL_NODE;
            } else {
              next_header = x;
            }
            break;
          }
        }
      }

      public final boolean hasNext() {
        return !m.isNullNode(next);
      }

      final void advance() {
        if (m.isNullNode(next)) {
          throw new NoSuchElementException();
        }
        lastReturned_KV = m.nodeGetKV(next, next_header);
        lastReturned = next;
        if (isDescending) {
          descend();
        } else {
          ascend();
        }
      }

      private void ascend() {
        for (;;) {
          next = m.nodeGetNext(next);
          if (m.isNullNode(next)) {
            break;
          }
          long x = m.nodeHeader(next);
          if (!m.headerDeleted(x)) {
            if (tooHighNode(next)) {
              next = NULL_NODE;
            } else {
              next_header = x;
            }
            break;
          }
        }
      }

      private void descend() {
        for (;;) {
          next = m.findNear(lastReturned_KV.key(), LT);
          if (m.isNullNode(next)) {
            break;
          }
          long x = m.nodeHeader(next);
          if (!m.headerDeleted(x)) {
            if (tooLowNode(next)) {
              next = NULL_NODE;
            } else {
              next_header = x;
            }
            break;
          }
        }
      }

      public void remove() {
        if (m.isNullNode(lastReturned)) {
          throw new IllegalStateException();
        }
        m.remove(lastReturned_KV.key());
        lastReturned = NULL_NODE;
      }
    }

    final class SubMapEntryIterator extends SubMapIter<Map.Entry<K, V>> {
      public Map.Entry<K, V> next() {
        if (m.isNullNode(next)) {
          throw new IllegalStateException();
        }
        advance();
        return new AbstractMap.SimpleImmutableEntry<K, V>(lastReturned_KV.key(),
            lastReturned_KV.value());
      }
    }

    final class SubMapKeyIterator extends SubMapIter<K> {
      public K next() {
        if (m.isNullNode(next)) {
          throw new IllegalStateException();
        }
        advance();
        return lastReturned_KV.key();
      }
    }

    final class SubMapValueIterator extends SubMapIter<V> {
      public V next() {
        if (m.isNullNode(next)) {
          throw new IllegalStateException();
        }
        advance();
        return lastReturned_KV.value();
      }
    }
  }

  static final <X> List<X> toList(Collection<X> c) {
    List<X> list = new ArrayList<X>();
    for (X e : c) {
      list.add(e);
    }
    return list;
  }

  static final class KeySet<E> extends AbstractSet<E> implements
      NavigableSet<E> {
    private final ConcurrentNavigableMap<E, Object> m;

    KeySet(ConcurrentNavigableMap<E, Object> map) {
      m = map;
    }

    public Comparator<? super E> comparator() {
      return m.comparator();
    }

    public E first() {
      return m.firstKey();
    }

    public E last() {
      return m.lastKey();
    }

    public E lower(E e) {
      return m.lowerKey(e);
    }

    public E floor(E e) {
      return m.floorKey(e);
    }

    public E ceiling(E e) {
      return m.ceilingKey(e);
    }

    public E higher(E e) {
      return m.higherKey(e);
    }

    public E pollFirst() {
      Map.Entry<E, Object> e = m.pollFirstEntry();
      return e == null ? null : e.getKey();
    }

    public E pollLast() {
      Map.Entry<E, Object> e = m.pollLastEntry();
      return e == null ? null : e.getKey();
    }

    public NavigableSet<E> descendingSet() {
      return new CompactedConcurrentSkipListSet<E>(m.descendingMap());
    }

    public Iterator<E> descendingIterator() {
      return descendingSet().iterator();
    }

    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive,
        E toElement, boolean toInclusive) {
      return new CompactedConcurrentSkipListSet<E>(m.subMap(fromElement,
          fromInclusive, toElement, toInclusive));
    }

    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
      return new CompactedConcurrentSkipListSet<E>(m.headMap(toElement,
          inclusive));
    }

    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
      return new CompactedConcurrentSkipListSet<E>(m.tailMap(fromElement,
          inclusive));
    }

    public SortedSet<E> subSet(E fromElement, E toElement) {
      return new CompactedConcurrentSkipListSet<E>(m.subMap(fromElement,
          toElement));
    }

    public SortedSet<E> headSet(E toElement) {
      return new CompactedConcurrentSkipListSet<E>(m.headMap(toElement));
    }

    public SortedSet<E> tailSet(E fromElement) {
      return new CompactedConcurrentSkipListSet<E>(m.tailMap(fromElement));
    }

    @Override
    public Iterator<E> iterator() {
      if (m instanceof CompactedConcurrentSkipListMap) {
        return ((CompactedConcurrentSkipListMap<E, Object>) m).keyIterator();
      } else {
        return ((CompactedConcurrentSkipListMap.SubMap<E, Object>) m).keyIterator();
      }
    }

    @Override
    public int size() {
      return m.size();
    }

    public boolean isEmpty() {
      return m.isEmpty();
    }

    public boolean contains(Object o) {
      return m.containsKey(o);
    }

    public boolean remove(Object o) {
      return m.remove(o) != null;
    }

    public void clear() {
      m.clear();
    }

    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Set)) {
        return false;
      }
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException e) {
        return false;
      } catch (NullPointerException e) {
        return false;
      }
    }

    public Object[] toArray() {
      return toList(this).toArray();
    }

    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
  }

  static final class EntrySet<K1, V1> extends AbstractSet<Map.Entry<K1, V1>> {
    private final ConcurrentNavigableMap<K1, V1> m;

    EntrySet(ConcurrentNavigableMap<K1, V1> map) {
      m = map;
    }

    public Iterator<java.util.Map.Entry<K1, V1>> iterator() {
      if (m instanceof CompactedConcurrentSkipListMap) {
        return ((CompactedConcurrentSkipListMap<K1, V1>) m).entryIterator();
      } else {
        return ((SubMap<K1, V1>) m).entryIterator();
      }
    }

    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<K1, V1> e = (Map.Entry<K1, V1>) o;
      V1 v = m.get(e.getKey());
      return v != null && v.equals(e.getValue());
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<K1, V1> e = (Map.Entry<K1, V1>) o;
      return m.remove(e.getKey(), e.getValue());
    }

    public boolean isEmpty() {
      return m.isEmpty();
    }

    public int size() {
      return m.size();
    }

    public void clear() {
      m.clear();
    }

    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof Set)) {
        return false;
      }
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException e) {
        return false;
      } catch (NullPointerException e) {
        return false;
      }
    }

    public Object[] toArray() {
      return toList(this).toArray();
    }

    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
  }

  static final class Values<E> extends AbstractCollection<E> {
    private final ConcurrentNavigableMap<Object, E> m;

    Values(ConcurrentNavigableMap<Object, E> map) {
      m = map;
    }

    public Iterator<E> iterator() {
      if (m instanceof CompactedConcurrentSkipListMap) {
        return ((CompactedConcurrentSkipListMap<Object, E>) m).valueIterator();
      } else {
        return ((SubMap<Object, E>) m).valueIterator();
      }
    }

    public boolean isEmpty() {
      return m.isEmpty();
    }

    public int size() {
      return m.size();
    }

    public boolean contains(Object o) {
      return m.containsValue(o);
    }

    public void clear() {
      m.clear();
    }

    public Object[] toArray() {
      return toList(this).toArray();
    }

    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
  }

}

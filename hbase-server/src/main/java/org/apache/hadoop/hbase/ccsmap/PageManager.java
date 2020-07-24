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

import java.security.InvalidParameterException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.UnsafeAccess;

/**
 * A simple page manager can support data/heapkv pages.
 * @param <P>
 */
@InterfaceAudience.Private
public abstract class PageManager<P> {
  final String name;
  final int maxPage;
  final int pageSize;
  final int pageLocationShift;
  final int alignedPageOffsetMask;
  final int maxAllocatingPages;
  final int alignShift;

  final long maxSize;
  final long maxAlignedSize;

  Object[] pages;
  final AtomicLong alignedTail = new AtomicLong(0);

  /**
   * how many pages we can use
   */
  final AtomicInteger usingPages = new AtomicInteger(0);

  /**
   * total pages including which are being created.
   */
  final AtomicInteger totalPages = new AtomicInteger(0);

  protected abstract P allocatePage(int size);

  protected abstract P allocateFirstPage();

  /**
   * A simple manger for allocating space on page, positioning, allocating
   * pages.
   *
   * PageManager won't concern what is stored in side the page.
   *
   * @param name
   *          The page manager's name
   * @param maxPage
   *          How many pages the manager can support.
   * @param pageSize
   *          Single page's size
   * @param maxAllocatingPages
   *          How many new pages could be allocating at the same time
   * @param alignShift
   *          How much the offset passed-in and return shild be left shfit to real byte address
   */
  PageManager(String name, int maxPage, int pageSize, int maxAllocatingPages, int alignShift) {
    this.name = name;
    this.maxPage = maxPage;
    this.alignShift = alignShift;
    if ((pageSize & (pageSize - 1)) != 0) {
      throw new InvalidParameterException("");
    }
    if (pageSize <= 0) {
      throw new InvalidParameterException("Invalid page size: " + pageSize);
    }
    this.pageSize = pageSize;
    this.pageLocationShift = CCSMapUtils.getShiftFromX((this.pageSize >> alignShift));
    this.alignedPageOffsetMask = (this.pageSize >> alignShift) - 1;

    this.maxAllocatingPages = maxAllocatingPages;
    this.maxSize = (long) maxPage * (long) pageSize;
    this.maxAlignedSize = this.maxSize >> alignShift;
    this.pages = new Object[this.maxPage];

    // To avoid waste of pages for empty map, we allocate a small page for
    // skiplist's head
    doAllocatePage(0);
  }

  /**
   * Single page's size
   * @return
   */
  public final int pageSize() {
    return this.pageSize;
  }

  /**
   * The max page count manager could support
   * @return
   */
  public final int maxPage() {
    return this.maxPage;
  }

  /**
   * The max space this page manager could support
   * @return
   */
  public final long maxSize() {
    return this.maxSize;
  }

  /**
   * How much space is already used
   * @return
   */
  public final long used() {
    return (this.alignedTail.get() << alignShift);
  }

  /**
   * How much space is allocated
   * @return
   */
  public final long capacity() {
    long ignoreFirstPage = this.usingPages.get() - 1;
    return (long) ignoreFirstPage * this.pageSize;
  }

  /**
   * How many pages have been used
   * @return
   */
  public final int getAllocatedPages() {
    return this.usingPages.get() - 1;
  }

  /**
   * Get the page for a given offset;
   *
   * @param offset
   * @return
   */
  @SuppressWarnings("unchecked")
  final public P getPage(long offset) {
    int pageIndex = (int) (offset >> this.pageLocationShift);
    Object ret = this.pages[pageIndex];
    if (ret == null) {
      ret = UnsafeAccess.getObjectVolatile(pages, pageIndex);
    }
    return (P)ret;
  }

  /**
   * get real offset inside its page for given offset
   */
  final public int getInPageOffset(long offset) {
    return (int) (offset & alignedPageOffsetMask);
  }

  /**
   * @size The required space for a new node, in bytes
   *
   * @return
   */
  final public long allocate(int size) {
    if (size <= 0) {
      throw new RuntimeException("Impossible, invalid size=" + size);
    }
    if (size > this.pageSize) {
      throw new SpaceNotEnoughExcpetion(name
        + " : allocating size is bigger than page size");
    }
    int alignedSize = ((size - 1) >> alignShift) + 1;
    for (;;) {
      long current = alignedTail.get();
      long bound = ((current >> pageLocationShift) + 1) << pageLocationShift;
      long ret = (bound - current >= alignedSize) ? current : bound;
      // it is no need to check ret + size, since (ret) & (ret + size) will
      // always on same page
      if (ret >= maxAlignedSize) {
        throw new SpaceNotEnoughExcpetion(name, used(), maxSize(), alignedSize << alignShift);
      }
      // page not enough
      if ((ret >> pageLocationShift) + 1 > usingPages.get()) {
        doAllocatePage((int) ((ret >> pageLocationShift) + 1));
        continue; // restart
      }
      // lost race for allocating memory;
      if (!alignedTail.compareAndSet(current, ret + alignedSize)) {
        continue;
      }
      return ret;
    }
  }

  /**
   * Move the tail pointer for allocation to second page of the whole space.
   *
   * @return The tail pointer before moving. I.E, the space has been used in
   *         the first page
   */
  final public long skipFirstPage() {
    assert getAllocatedPages() == 0;
    assert (alignedTail.get() >> pageLocationShift) == 0;
    int nextPageIndex = 1;
    long next = (nextPageIndex << pageLocationShift);
    return this.alignedTail.getAndSet(next) << alignShift;
  }

  /**
   * Allocate a new page, and put it on to pages[]
   * @param index new page's index
   */
  private void doAllocatePage(int index) {
    // Skip allocating new page if too many threads are creating new page.
    if (this.totalPages.get() - index >= this.maxAllocatingPages) {
      return;
    }
    P newPage = index == 0 ? allocateFirstPage() : allocatePage(this.pageSize);
    totalPages.incrementAndGet();
    // Put newPage on page, restart if lost race
    for (;;) {
      int pos = usingPages.get();
      if (!UnsafeAccess.compareAndSwapObject(pages, pos, null, newPage)) {
        continue;
      }
      usingPages.incrementAndGet();
      break;
    }
  }
}

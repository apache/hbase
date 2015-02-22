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

package org.apache.hadoop.hbase.codec.prefixtree.decode;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Pools PrefixTreeArraySearcher objects. Each Searcher can consist of hundreds or thousands of
 * objects and 1 is needed for each HFile during a Get operation. With tens of thousands of
 * Gets/second, reusing these searchers may save a lot of young gen collections.
 * <p/>
 * Alternative implementation would be a ByteBufferSearcherPool (not implemented yet).
 */
@InterfaceAudience.Private
public class ArraySearcherPool {

  /**
   * One decoder is needed for each storefile for each Get operation so we may need hundreds at the
   * same time, however, decoding is a CPU bound activity so should limit this to something in the
   * realm of maximum reasonable active threads.
   */
  private static final Integer MAX_POOL_SIZE = 1000;

  protected Queue<PrefixTreeArraySearcher> pool
    = new LinkedBlockingQueue<PrefixTreeArraySearcher>(MAX_POOL_SIZE);

  public PrefixTreeArraySearcher checkOut(ByteBuffer buffer, boolean includesMvccVersion) {
    PrefixTreeArraySearcher searcher = pool.poll();//will return null if pool is empty
    searcher = DecoderFactory.ensureArraySearcherValid(buffer, searcher, includesMvccVersion);
    return searcher;
  }

  public void checkIn(PrefixTreeArraySearcher searcher) {
    searcher.releaseBlockReference();
    pool.offer(searcher);
  }

  @Override
  public String toString() {
    return ("poolSize:" + pool.size());
  }

}

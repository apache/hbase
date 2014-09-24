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

package org.apache.hadoop.hbase.codec.prefixtree.encode;

import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;


/**
 * Pool to enable reusing the Encoder objects which can consist of thousands of smaller objects and
 * would be more garbage than the data in the block.  A new encoder is needed for each block in
 * a flush, compaction, RPC response, etc.
 *
 * It is not a pool in the traditional sense, but implements the semantics of a traditional pool
 * via ThreadLocals to avoid sharing between threads.  Sharing between threads would not be
 * very expensive given that it's accessed per-block, but this is just as easy.
 *
 * This pool implementation assumes there is a one-to-one mapping between a single thread and a
 * single flush or compaction.
 */
@InterfaceAudience.Private
public class ThreadLocalEncoderPool implements EncoderPool{

  private static final ThreadLocal<PrefixTreeEncoder> ENCODER
      = new ThreadLocal<PrefixTreeEncoder>();

  /**
   * Get the encoder attached to the current ThreadLocal, or create a new one and attach it to the
   * current thread.
   */
  @Override
  public PrefixTreeEncoder checkOut(OutputStream os, boolean includeMvccVersion) {
    PrefixTreeEncoder builder = ENCODER.get();
    builder = EncoderFactory.prepareEncoder(builder, os, includeMvccVersion);
    ENCODER.set(builder);
    return builder;
  }

  @Override
  public void checkIn(PrefixTreeEncoder encoder) {
    // attached to thread on checkOut, so shouldn't need to do anything here

    // do we need to worry about detaching encoders from compaction threads or are the same threads
    // used over and over
  }

}

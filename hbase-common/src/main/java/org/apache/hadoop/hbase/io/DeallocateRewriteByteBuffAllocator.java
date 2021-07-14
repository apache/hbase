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
package org.apache.hadoop.hbase.io;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ByteBuffAllocator that rewrite the bytebuffers right after released.
 * It can be used for test whether there are prematurely releasing backing bytebuffers.
 */
@InterfaceAudience.Private
public class DeallocateRewriteByteBuffAllocator  extends ByteBuffAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(
    DeallocateRewriteByteBuffAllocator.class);

  DeallocateRewriteByteBuffAllocator(boolean reservoirEnabled, int maxBufCount, int bufSize,
    int minSizeForReservoirUse) {
    super(reservoirEnabled, maxBufCount, bufSize, minSizeForReservoirUse);
  }

  @Override
  protected void putbackBuffer(ByteBuffer buf) {
    if (buf.capacity() != bufSize || (reservoirEnabled ^ buf.isDirect())) {
      LOG.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
      return;
    }
    buf.clear();
    byte[] tmp = generateTmpBytes(buf.capacity());
    buf.put(tmp, 0, tmp.length);
    super.putbackBuffer(buf);
  }

  private byte[] generateTmpBytes(int length) {
    StringBuilder result = new StringBuilder();
    while (result.length() < length) {
      result.append("-");
    }
    return Bytes.toBytes(result.substring(0, length));
  }
}

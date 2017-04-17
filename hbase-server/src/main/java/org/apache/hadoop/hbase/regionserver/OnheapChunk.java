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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 * An on heap chunk implementation.
 */
@InterfaceAudience.Private
public class OnheapChunk extends Chunk {

  OnheapChunk(int size) {
    super(size);
  }

  public void init() {
    assert nextFreeOffset.get() == UNINITIALIZED;
    try {
      if (data == null) {
        data = ByteBuffer.allocate(this.size);
      }
    } catch (OutOfMemoryError e) {
      boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
      assert failInit; // should be true.
      throw e;
    }
    // Mark that it's ready for use
    boolean initted = nextFreeOffset.compareAndSet(UNINITIALIZED, 0);
    // We should always succeed the above CAS since only one thread
    // calls init()!
    Preconditions.checkState(initted, "Multiple threads tried to init same chunk");
  }
}

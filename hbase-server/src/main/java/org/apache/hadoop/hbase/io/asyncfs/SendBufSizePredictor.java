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
package org.apache.hadoop.hbase.io.asyncfs;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to predict the next send buffer size.
 */
@InterfaceAudience.Private
class SendBufSizePredictor {

  // LIMIT is 128MB
  private static final int LIMIT = 128 * 1024 * 1024;

  // buf's initial capacity - 4KB
  private int capacity = 4 * 1024;

  int initialSize() {
    return capacity;
  }

  int guess(int bytesWritten) {
    // if the bytesWritten is greater than the current capacity
    // always increase the capacity in powers of 2.
    if (bytesWritten > this.capacity) {
      // Ensure we don't cross the LIMIT
      if ((this.capacity << 1) <= LIMIT) {
        // increase the capacity in the range of power of 2
        this.capacity = this.capacity << 1;
      }
    } else {
      // if we see that the bytesWritten is lesser we could again decrease
      // the capacity by dividing it by 2 if the bytesWritten is satisfied by
      // that reduction
      if ((this.capacity >> 1) >= bytesWritten) {
        this.capacity = this.capacity >> 1;
      }
    }
    return this.capacity;
  }
}

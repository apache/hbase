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

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface denotes a scanner as one which can ship cells. Scan operation do many RPC requests
 * to server and fetch N rows/RPC. These are then shipped to client. At the end of every such batch
 * {@link #shipped()} will get called. <br>
 * Scans of large numbers of fully filtered blocks (due to Filter, or sparse columns, etc) can cause
 * excess memory to be held while waiting for {@link #shipped()} to be called. Therefore, there's a
 * checkpoint mechanism via {@link #checkpoint(State)}. These enable fully filtered blocks to be
 * eagerly released, since they are not referenced by cells being returned to clients.
 */
@InterfaceAudience.Private
public interface Shipper {

  /**
   * Called after a batch of rows scanned and set to be returned to client. Any in between cleanup
   * can be done here.
   */
  void shipped() throws IOException;

  enum State {
    START,
    FILTERED
  }

  /**
   * Called during processing of a batch of scanned rows, before returning to the client. Allows
   * releasing of blocks which have been totally skipped in the result set due to filters. <br>
   * Should be called with {@link State#START} at the beginning of a request for a row. This will
   * set state necessary to handle {@link State#FILTERED}. Calling with {@link State#FILTERED} will
   * release any blocks which have been fully processed since the last call to
   * {@link #checkpoint(State)}. Calling again with {@link State#START} will reset the pointers.
   */
  void checkpoint(State state);

  /**
   * Used by upstream callers to notify the shipper that the current block should be retained for
   * shipping when {@link #shipped()} or {@link #checkpoint(State)} are called. Otherwise, the block
   * will be released immediately once it's no longer needed. Only has an effect after
   * {@link #checkpoint(State)} has been called at least once.
   */
  void retainBlock();
}

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
package org.apache.hadoop.hbase.client;

import java.util.Optional;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the low level API for asynchronous scan.
 * <p>
 * All results that match the given scan object will be passed to this class by calling
 * {@link #onNext(Result[], ScanController)}. {@link #onComplete()} means the scan is finished, and
 * {@link #onError(Throwable)} means we hit an unrecoverable error and the scan is terminated.
 * {@link #onHeartbeat(ScanController)} means the RS is still working but we can not get a valid
 * result to call {@link #onNext(Result[], ScanController)}. This is usually because the matched
 * results are too sparse, for example, a filter which almost filters out everything is specified.
 * <p>
 * Notice that, all the methods here will be called directly in the thread which we send request to
 * HBase service. So if you want the asynchronous scanner fetch data from HBase in background while
 * you process the returned data, you need to move the processing work to another thread to make the
 * {@link #onNext(Result[], ScanController)} call return immediately. And please do NOT do any time
 * consuming tasks in these methods unless you know what you are doing.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AdvancedScanResultConsumer extends ScanResultConsumerBase {

  /**
   * Used to resume a scan.
   */
  @InterfaceAudience.Public
  interface ScanResumer {

    /**
     * Resume the scan. You are free to call it multiple time but only the first call will take
     * effect.
     */
    void resume();
  }

  /**
   * Used to suspend or stop a scan, or get a scan cursor if available.
   * <p>
   * Notice that, you should only call the {@link #suspend()} or {@link #terminate()} inside onNext
   * or onHeartbeat method. A IllegalStateException will be thrown if you call them at other places.
   * <p>
   * You can only call one of the {@link #suspend()} and {@link #terminate()} methods(of course you
   * are free to not call them both), and the methods are not reentrant. An IllegalStateException
   * will be thrown if you have already called one of the methods.
   */
  @InterfaceAudience.Public
  interface ScanController {

    /**
     * Suspend the scan.
     * <p>
     * This means we will stop fetching data in background, i.e., will not call onNext any more
     * before you resume the scan.
     * @return A resumer used to resume the scan later.
     */
    ScanResumer suspend();

    /**
     * Terminate the scan.
     * <p>
     * This is useful when you have got enough results and want to stop the scan in onNext method,
     * or you want to stop the scan in onHeartbeat method because it has spent too many time.
     */
    void terminate();

    /**
     * Get the scan cursor if available.
     * @return The scan cursor.
     */
    Optional<Cursor> cursor();
  }

  /**
   * Indicate that we have receive some data.
   * @param results the data fetched from HBase service.
   * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
   *          instance is only valid within scope of onNext method. You can only call its method in
   *          onNext, do NOT store it and call it later outside onNext.
   */
  void onNext(Result[] results, ScanController controller);

  /**
   * Indicate that there is a heartbeat message but we have not cumulated enough cells to call
   * {@link #onNext(Result[], ScanController)}.
   * <p>
   * Note that this method will always be called when RS returns something to us but we do not have
   * enough cells to call {@link #onNext(Result[], ScanController)}. Sometimes it may not be a
   * 'heartbeat' message for RS, for example, we have a large row with many cells and size limit is
   * exceeded before sending all the cells for this row. For RS it does send some data to us and the
   * time limit has not been reached, but we can not return the data to client so here we call this
   * method to tell client we have already received something.
   * <p>
   * This method give you a chance to terminate a slow scan operation.
   * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
   *          instance is only valid within the scope of onHeartbeat method. You can only call its
   *          method in onHeartbeat, do NOT store it and call it later outside onHeartbeat.
   */
  default void onHeartbeat(ScanController controller) {
  }
}

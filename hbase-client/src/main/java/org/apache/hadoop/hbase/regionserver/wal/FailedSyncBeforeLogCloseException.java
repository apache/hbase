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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when we fail close of the write-ahead-log file.
 * Package private.  Only used inside this package.
 */
@InterfaceAudience.Public
public class FailedSyncBeforeLogCloseException extends FailedLogCloseException {
  private static final long serialVersionUID = 1759152841462990925L;

  public FailedSyncBeforeLogCloseException() {
    super();
  }

  /**
   * @param msg
   */
  public FailedSyncBeforeLogCloseException(String msg) {
    super(msg);
  }

  public FailedSyncBeforeLogCloseException(final String msg, final Throwable t) {
    super(msg, t);
  }

  public FailedSyncBeforeLogCloseException(final Throwable t) {
    super(t);
  }
}

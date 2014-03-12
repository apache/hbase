/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * This class preserves parameters of connections to region servers.
 */
public class HConnectionParams {
  private static HConnectionParams instance = null;

  private int numRetries;
  private final long pause;
  private final int rpcTimeout;
  private final long rpcRetryTimeout;

  // The number of times we will retry after receiving a RegionOverloadedException from the
  // region server. Defaults to 0 (i.e. we will throw the exception and let the client handle retries)
  // may not always be what you want. But, for the purposes of the HBaseThrift client, that this is
  // created for, we do not want the thrift layer to hold up IPC threads handling retries.
  private int maxServerRequestedRetries;

  public static HConnectionParams getInstance(Configuration conf) {
    if (instance == null) {
      instance = new HConnectionParams(conf);
    }

    return instance;
  }

  private HConnectionParams(Configuration conf) {
    this.numRetries = conf.getInt(HConstants.CLIENT_RETRY_NUM_STRING, HConstants.DEFAULT_CLIENT_RETRY_NUM);
    this.maxServerRequestedRetries = conf.getInt(
        HConstants.SERVER_REQUESTED_RETRIES_STRING, HConstants.DEFAULT_SERVER_REQUESTED_RETRIES);


    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    this.rpcRetryTimeout = conf.getLong(
        HConstants.CLIENT_RPC_RETRY_TIMEOUT_STRING, HConstants.DEFAULT_CLIENT_RPC_RETRY_TIMEOUT);
  }

  public int getNumRetries() {
    return this.numRetries;
  }

  public int getMaxServerRequestedRetries() {
    return this.maxServerRequestedRetries;
  }

  public long getPauseTime(int tries) {
    if (tries >= HConstants.RETRY_BACKOFF.length)
      tries = HConstants.RETRY_BACKOFF.length - 1;
    return this.pause * HConstants.RETRY_BACKOFF[tries];
  }

  public int getRpcTimeout() {
    return this.rpcTimeout;
  }

  public long getRpcRetryTimeout() {
    return this.rpcRetryTimeout;
  }
}

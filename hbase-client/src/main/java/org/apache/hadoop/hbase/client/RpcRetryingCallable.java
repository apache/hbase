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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * A RetryingCallable for RPC connection operations.
 * @param <V> return type
 */
abstract class RpcRetryingCallable<V> implements RetryingCallable<V>, Closeable {
  @Override
  public void prepare(boolean reload) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "";
  }

  @Override
  public long sleep(long pause, int tries) {
    return ConnectionUtils.getPauseTime(pause, tries);
  }

  @Override
  // Same trick as in RegionServerCallable so users don't have to copy/paste so much boilerplate
  // and so we contain references to protobuf.
  public V call(int callTimeout) throws IOException {
    try {
      return rpcCall(callTimeout);
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  protected abstract V rpcCall(int callTimeout) throws Exception;
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.RpcCallback;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Simple {@link RpcCallback} implementation providing a
 * {@link java.util.concurrent.Future}-like {@link BlockingRpcCallback#get()} method, which
 * will block util the instance's {@link BlockingRpcCallback#run(Object)} method has been called.
 * {@code R} is the RPC response type that will be passed to the {@link #run(Object)} method.
 */
@InterfaceAudience.Private
public class BlockingRpcCallback<R> implements RpcCallback<R> {
  private R result;
  private boolean resultSet = false;

  /**
   * Called on completion of the RPC call with the response object, or {@code null} in the case of
   * an error.
   * @param parameter the response object or {@code null} if an error occurred
   */
  @Override
  public void run(R parameter) {
    synchronized (this) {
      result = parameter;
      resultSet = true;
      this.notify();
    }
  }

  /**
   * Returns the parameter passed to {@link #run(Object)} or {@code null} if a null value was
   * passed.  When used asynchronously, this method will block until the {@link #run(Object)}
   * method has been called.
   * @return the response object or {@code null} if no response was passed
   */
  public synchronized R get() throws IOException {
    while (!resultSet) {
      try {
        this.wait();
      } catch (InterruptedException ie) {
        InterruptedIOException exception = new InterruptedIOException(ie.getMessage());
        exception.initCause(ie);
        throw exception;
      }
    }
    return result;
  }
}

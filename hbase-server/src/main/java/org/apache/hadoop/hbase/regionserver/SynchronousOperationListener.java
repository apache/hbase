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

import com.google.common.base.Throwables;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * An OperationListener supporting getting result directly. Temporarily used when AsyncRegion is
 * not fully non-blocking. When call getResult of this class, the operation must have been done.
 */
@InterfaceAudience.Private
public class SynchronousOperationListener<T> implements OperationListener<T> {

  private T result;
  private boolean done;
  private Throwable error;

  @Override
  public void completed(T result) {
    done = true;
    this.result = result;
  }

  @Override
  public void failed(Throwable error) {
    done = true;
    this.error = error;
  }

  /**
   * We call this method after calling operation of AsyncRegion synchronously in the same thread.
   * So no need to lock and success/fail must has been called.
   */
  public T getResult() throws IOException {
    assert done;
    if (error != null) {
      // We also need throw unchecked throwable
      Throwables.propagateIfPossible(error, IOException.class);
      // Wrap to IOE if it is not IOE or unchecked
      throw new IOException(error);
    }
    return result;
  }
}

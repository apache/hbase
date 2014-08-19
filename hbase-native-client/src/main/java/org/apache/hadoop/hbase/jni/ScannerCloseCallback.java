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
 *
 */
package org.apache.hadoop.hbase.jni;

import com.stumbleupon.async.Callback;

public class ScannerCloseCallback<R, T> implements Callback<R, T> {

  private long callback_;
  private long scanner_;
  private long extra_;

  public ScannerCloseCallback(long callback, long scanner, long extra) {
    this.callback_ = callback;
    this.scanner_ = scanner;
    this.extra_ = extra;
  }

  @Override
  public R call(T arg) throws Exception {
    Throwable t = null;
    if (arg instanceof Throwable) {
      t = (Throwable) arg;
    }
    CallbackHandlers.scannerCloseCallBack(t,
        callback_, scanner_, extra_);
    return null;
  }
}

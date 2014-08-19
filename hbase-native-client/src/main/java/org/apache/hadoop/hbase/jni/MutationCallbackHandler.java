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

import java.util.ArrayList;

import org.hbase.async.KeyValue;

import com.stumbleupon.async.Callback;

public class MutationCallbackHandler<R, T> implements Callback<R, T> {
  private final long callback;
  private final long extra;
  private final long client;
  private final long mutation;
  private MutationProxy mutationProxy;

  public MutationCallbackHandler(MutationProxy mutationProxy,
      long callback, long client, long mutation, long extra) {
    this.mutationProxy = mutationProxy;
    this.callback = callback;
    this.client = client;
    this.mutation = mutation;
    this.extra = extra;
  }

  @Override
  @SuppressWarnings("unchecked")
  public R call(T arg) throws Exception {
    ResultProxy result = null;
    Throwable t = null;
    if (arg instanceof Throwable) {
      t = (Throwable) arg;
    } else if (arg != null && arg instanceof ArrayList<?>) {
      result = new ResultProxy(mutationProxy.getTable(),
          mutationProxy.getNamespace(), (ArrayList<KeyValue>) arg);
    }

    CallbackHandlers.mutationCallBack(t,
        callback, client, mutation, result, extra);
    return null;
  }
}

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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

public class ClientProxy implements Closeable {
  private final HBaseClient client_;

  ClientProxy(final Configuration conf) {
    final String zkQuorum = conf.get("hbase.zookeeper.ensemble");
    final String baseZNode = conf.get("zookeeper.znode.parent");
    this.client_ = new HBaseClient(zkQuorum, baseZNode);
  }

  Scanner newScanner(byte[] table) {
    return client_.newScanner(table);
  }

  public void sendMutation(final MutationProxy mutationProxy,
      final long callback, final long client,
      final long mutation, final long extra) {
    mutationProxy.send(client_,
        new MutationCallbackHandler<Object, Object>(
            mutationProxy, callback, client, mutation, extra));
  }

  public void sendGet(final GetProxy getProxy,
      final long callback, final long client,
      final long get, final long extra) {
    getProxy.send(client_,
        new GetCallbackHandler<Object, ArrayList<KeyValue>>(
            getProxy, callback, client, get, extra));
  }

  public void flush(final long callback,
      final long client, final long extra)
          throws IOException {
    this.client_.flush().addBoth(
        new FlushCallbackHandler<Object, Object>(
            callback, client, extra));
  }

  @Override
  public void close() throws IOException {
    close(0, 0, 0);
  }

  public void close(final long callback,
      final long client, final long extra)
      throws IOException {
    this.client_.shutdown().addBoth(
        new ClientShutdownCallback<Object, Object>(
            callback, client, extra));
  }
}

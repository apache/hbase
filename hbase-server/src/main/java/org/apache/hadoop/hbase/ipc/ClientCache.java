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

package org.apache.hadoop.hbase.ipc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Cache a client using its socket factory as the hash key.
 * Enables reuse/sharing of clients on a per SocketFactory basis. A client 
 * establishes certain configuration dependent characteristics like timeouts, 
 * tcp-keepalive (true or false), etc. For more details on the characteristics,
 * look at {@link HBaseClient#HBaseClient(Configuration, SocketFactory)}
 * Creation of dynamic proxies to protocols creates the clients (and increments
 * reference count once created), and stopping of the proxies leads to clearing
 * out references and when the reference drops to zero, the cache mapping is 
 * cleared. 
 */
class ClientCache {
  private Map<SocketFactory, HBaseClient> clients =
    new HashMap<SocketFactory, HBaseClient>();

  protected ClientCache() {}

  /**
   * Construct & cache an IPC client with the user-provided SocketFactory
   * if no cached client exists.
   *
   * @param conf Configuration
   * @param factory socket factory
   * @return an IPC client
   */
  @SuppressWarnings("unchecked")
  protected synchronized HBaseClient getClient(Configuration conf, SocketFactory factory) {

    HBaseClient client = clients.get(factory);
    if (client == null) {
      Class<? extends HBaseClient> hbaseClientClass = (Class<? extends HBaseClient>) conf
          .getClass(HConstants.HBASECLIENT_IMPL, HBaseClient.class);

      // Make an hbase client instead of hadoop Client.
      try {
        Constructor<? extends HBaseClient> cst = hbaseClientClass.getConstructor(
            Configuration.class, SocketFactory.class);
        client = cst.newInstance(conf, factory);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("No matching constructor in "+hbaseClientClass.getName(), e);
      }

      clients.put(factory, client);
    } else {
      client.incCount();
    }
    return client;
  }

  /**
   * Stop a RPC client connection
   * A RPC client is closed only when its reference count becomes zero.
   * @param client client to stop
   */
  protected void stopClient(HBaseClient client) {
    synchronized (this) {
      client.decCount();
      if (client.isZeroReference()) {
        clients.remove(client.getSocketFactory());
      }
    }
    if (client.isZeroReference()) {
      client.stop();
    }
  }
}

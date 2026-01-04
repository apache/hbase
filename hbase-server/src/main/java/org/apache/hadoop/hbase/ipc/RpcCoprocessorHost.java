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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RpcCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RpcCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RpcObserver;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@InterfaceAudience.Private
public class RpcCoprocessorHost extends CoprocessorHost<RpcCoprocessor, RpcCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(RpcCoprocessorHost.class);

  private static class RpcEnvironment extends BaseEnvironment<RpcCoprocessor>
    implements RpcCoprocessorEnvironment {

    public RpcEnvironment(RpcCoprocessor impl, int priority, int seq, Configuration conf) {
      super(impl, priority, seq, conf);
    }
  }

  public RpcCoprocessorHost(final Configuration conf) {
    // RPCServer cannot be aborted, so we don't pass Abortable down here.
    super(null);
    this.conf = conf;
    boolean coprocessorsEnabled =
      conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_COPROCESSORS_ENABLED);
    LOG.trace("System coprocessor loading is {}", (coprocessorsEnabled ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, RPC_COPROCESSOR_CONF_KEY);
  }

  @Override
  public RpcCoprocessorEnvironment createEnvironment(RpcCoprocessor instance, int priority,
    int sequence, Configuration conf) {
    return new RpcEnvironment(instance, priority, sequence, conf);
  }

  @Override
  public RpcCoprocessor checkAndGetInstance(Class<?> implClass)
    throws InstantiationException, IllegalAccessException {
    try {
      if (RpcCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(RpcCoprocessor.class).getDeclaredConstructor().newInstance();
      } else {
        LOG.error("{} is not of type RpcCoprocessor. Check the configuration of {}",
          implClass.getName(), CoprocessorHost.RPC_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private final ObserverGetter<RpcCoprocessor, RpcObserver> rpcObserverGetter =
    RpcCoprocessor::getRpcObserver;

  abstract class RpcObserverOperation extends ObserverOperationWithoutResult<RpcObserver> {
    public RpcObserverOperation() {
      this(null);
    }

    public RpcObserverOperation(User user) {
      this(user, false);
    }

    public RpcObserverOperation(boolean bypassable) {
      this(null, bypassable);
    }

    public RpcObserverOperation(User user, boolean bypassable) {
      super(rpcObserverGetter, createObserverRpcCallContext(user), bypassable);
    }
  }

  public void preAuthorizeConnection(RPCProtos.ConnectionHeader connectionHeader,
    InetAddress remoteAddr) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RpcObserverOperation() {
      @Override
      protected void call(RpcObserver observer) throws IOException {
        observer.preAuthorizeConnection(this, connectionHeader, remoteAddr);
      }
    });
  }

  public void postAuthorizeConnection(final String userName,
    final X509Certificate[] clientCertificates) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RpcObserverOperation() {
      @Override
      protected void call(RpcObserver observer) throws IOException {
        observer.postAuthorizeConnection(this, userName, clientCertificates);
      }
    });
  }
}

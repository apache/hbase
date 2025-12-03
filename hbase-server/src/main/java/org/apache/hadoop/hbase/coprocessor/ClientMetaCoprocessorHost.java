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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ClientMetaCoprocessorHost extends CoprocessorHost<ClientMetaCoprocessor, ClientMetaCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(ClientMetaCoprocessorHost.class);

  private static class ClientMetaEnvironment extends BaseEnvironment<ClientMetaCoprocessor>
    implements ClientMetaCoprocessorEnvironment {

    public ClientMetaEnvironment(ClientMetaCoprocessor impl, int priority, int seq, Configuration conf) {
      super(impl, priority, seq, conf);
    }
  }

  public ClientMetaCoprocessorHost(Configuration conf) {
    // RPCServer cannot be aborted, so we don't pass Abortable down here.
    super(null);
    this.conf = conf;
    boolean coprocessorsEnabled =
      conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_COPROCESSORS_ENABLED);
    LOG.trace("System coprocessor loading is {}", (coprocessorsEnabled ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, CLIENT_META_COPROCESSOR_CONF_KEY);
  }

  @Override
  public ClientMetaCoprocessorEnvironment createEnvironment(ClientMetaCoprocessor instance,
    int priority, int sequence, Configuration conf) {
    return new ClientMetaEnvironment(instance, priority, sequence, conf);
  }

  @Override
  public ClientMetaCoprocessor checkAndGetInstance(Class<?> implClass)
    throws InstantiationException, IllegalAccessException {
    try {
      if (ClientMetaCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(ClientMetaCoprocessor.class).getDeclaredConstructor().newInstance();
      } else {
        LOG.error("{} is not of type ClientMetaCoprocessor. Check the configuration of {}",
          implClass.getName(), CLIENT_META_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private final ObserverGetter<ClientMetaCoprocessor, ClientMetaObserver> clientMetaObserverGetter =
    ClientMetaCoprocessor::getClientMetaObserver;

  public void preGetClusterId() throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<ClientMetaObserver>(clientMetaObserverGetter) {
      @Override
      protected void call(ClientMetaObserver observer) throws IOException {
        observer.preGetClusterId(this);
      }
    });
  }

  public String postGetClusterId(String clusterId) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return clusterId;
    }

    return execOperationWithResult(
      new ObserverOperationWithResult<ClientMetaObserver, String>(clientMetaObserverGetter, clusterId) {
        @Override
        protected String call(ClientMetaObserver observer) throws IOException {
          return observer.postGetClusterId(this, getResult());
        }
      });
  }

  public void preGetActiveMaster() throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<ClientMetaObserver>(clientMetaObserverGetter) {
      @Override
      protected void call(ClientMetaObserver observer) throws IOException {
        observer.preGetActiveMaster(this);
      }
    });
  }

  public ServerName postGetActiveMaster(ServerName serverName) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return serverName;
    }

    return execOperationWithResult(
      new ObserverOperationWithResult<ClientMetaObserver, ServerName>(clientMetaObserverGetter, serverName) {
        @Override
        protected ServerName call(ClientMetaObserver observer) throws IOException {
          return observer.postGetActiveMaster(this, getResult());
        }
      });
  }

  public void preGetMasters() throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<ClientMetaObserver>(clientMetaObserverGetter) {
      @Override
      protected void call(ClientMetaObserver observer) throws IOException {
        observer.preGetMasters(this);
      }
    });
  }

  public Map<ServerName, Boolean> postGetMasters(Map<ServerName, Boolean> serverNames)
    throws IOException {
    if  (coprocEnvironments.isEmpty()) {
      return  serverNames;
    }

    return execOperationWithResult(
      new ObserverOperationWithResult<ClientMetaObserver, Map<ServerName, Boolean>>(clientMetaObserverGetter, serverNames) {
        @Override
        protected Map<ServerName, Boolean> call(ClientMetaObserver observer) throws IOException {
          return observer.postGetMasters(this, getResult());
        }
      });
  }

  public void preGetBootstrapNodes() throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<ClientMetaObserver>(clientMetaObserverGetter) {
      @Override
      protected void call(ClientMetaObserver observer) throws IOException {
        observer.preGetBootstrapNodes(this);
      }
    });
  }

  public List<ServerName> postGetBootstrapNodes(List<ServerName> bootstrapNodes) throws IOException {
    if  (coprocEnvironments.isEmpty()) {
      return bootstrapNodes;
    }

    return execOperationWithResult(
      new ObserverOperationWithResult<ClientMetaObserver, List<ServerName>>(clientMetaObserverGetter, bootstrapNodes) {
        @Override
        protected List<ServerName> call(ClientMetaObserver observer) throws IOException {
          return observer.postGetBootstrapNodes(this, getResult());
        }
      });
  }

  public void preGetMetaLocations() throws IOException {
    execOperation(coprocEnvironments.isEmpty()
      ? null
      : new ObserverOperationWithoutResult<ClientMetaObserver>(clientMetaObserverGetter) {
      @Override
      protected void call(ClientMetaObserver observer) throws IOException {
        observer.preGetMetaLocations(this);
      }
    });
  }

  public List<HRegionLocation> postGetMetaLocations(List<HRegionLocation> metaLocations) throws IOException {
    if  (coprocEnvironments.isEmpty()) {
      return metaLocations;
    }

    return execOperationWithResult(
      new ObserverOperationWithResult<ClientMetaObserver, List<HRegionLocation>>(clientMetaObserverGetter, metaLocations) {
        @Override
        protected List<HRegionLocation> call(ClientMetaObserver observer) throws IOException {
          return observer.postGetMetaLocations(this, getResult());
        }
      });
  }
}

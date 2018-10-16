/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.google.protobuf.Service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SharedConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RegionServerCoprocessorHost extends
    CoprocessorHost<RegionServerCoprocessor, RegionServerCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionServerCoprocessorHost.class);

  private RegionServerServices rsServices;

  public RegionServerCoprocessorHost(RegionServerServices rsServices,
      Configuration conf) {
    super(rsServices);
    this.rsServices = rsServices;
    this.conf = conf;
    // Log the state of coprocessor loading here; should appear only once or
    // twice in the daemon log, depending on HBase version, because there is
    // only one RegionServerCoprocessorHost instance in the RS process
    boolean coprocessorsEnabled = conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_COPROCESSORS_ENABLED);
    boolean tableCoprocessorsEnabled = conf.getBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_USER_COPROCESSORS_ENABLED);
    LOG.info("System coprocessor loading is " + (coprocessorsEnabled ? "enabled" : "disabled"));
    LOG.info("Table coprocessor loading is " +
      ((coprocessorsEnabled && tableCoprocessorsEnabled) ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, REGIONSERVER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public RegionServerEnvironment createEnvironment(
      RegionServerCoprocessor instance, int priority, int sequence, Configuration conf) {
    // If a CoreCoprocessor, return a 'richer' environment, one laden with RegionServerServices.
    return instance.getClass().isAnnotationPresent(CoreCoprocessor.class)?
      new RegionServerEnvironmentForCoreCoprocessors(instance, priority, sequence, conf,
            this.rsServices):
      new RegionServerEnvironment(instance, priority, sequence, conf, this.rsServices);
  }

  @Override
  public RegionServerCoprocessor checkAndGetInstance(Class<?> implClass)
      throws InstantiationException, IllegalAccessException {
    try {
      if (RegionServerCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(RegionServerCoprocessor.class).getDeclaredConstructor()
            .newInstance();
      } else if (SingletonCoprocessorService.class.isAssignableFrom(implClass)) {
        // For backward compatibility with old CoprocessorService impl which don't extend
        // RegionCoprocessor.
        SingletonCoprocessorService tmp = implClass.asSubclass(SingletonCoprocessorService.class)
            .getDeclaredConstructor().newInstance();
        return new CoprocessorServiceBackwardCompatiblity.RegionServerCoprocessorService(tmp);
      } else {
        LOG.error("{} is not of type RegionServerCoprocessor. Check the configuration of {}",
            implClass.getName(), CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private ObserverGetter<RegionServerCoprocessor, RegionServerObserver> rsObserverGetter =
      RegionServerCoprocessor::getRegionServerObserver;

  abstract class RegionServerObserverOperation extends
      ObserverOperationWithoutResult<RegionServerObserver> {
    public RegionServerObserverOperation() {
      super(rsObserverGetter);
    }

    public RegionServerObserverOperation(User user) {
      super(rsObserverGetter, user);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // RegionServerObserver operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  public void preStop(String message, User user) throws IOException {
    // While stopping the region server all coprocessors method should be executed first then the
    // coprocessor should be cleaned up.
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execShutdown(new RegionServerObserverOperation(user) {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preStopRegionServer(this);
      }

      @Override
      public void postEnvCall() {
        // invoke coprocessor stop method
        shutdown(this.getEnvironment());
      }
    });
  }

  public void preRollWALWriterRequest() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preRollWALWriterRequest(this);
      }
    });
  }

  public void postRollWALWriterRequest() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postRollWALWriterRequest(this);
      }
    });
  }

  public void preReplicateLogEntries()
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preReplicateLogEntries(this);
      }
    });
  }

  public void postReplicateLogEntries()
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postReplicateLogEntries(this);
      }
    });
  }

  public ReplicationEndpoint postCreateReplicationEndPoint(final ReplicationEndpoint endpoint)
      throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return endpoint;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionServerObserver, ReplicationEndpoint>(
            rsObserverGetter, endpoint) {
      @Override
      public ReplicationEndpoint call(RegionServerObserver observer) throws IOException {
        return observer.postCreateReplicationEndPoint(this, getResult());
      }
    });
  }

  public void preClearCompactionQueues() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preClearCompactionQueues(this);
      }
    });
  }

  public void postClearCompactionQueues() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postClearCompactionQueues(this);
      }
    });
  }

  public void preExecuteProcedures() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preExecuteProcedures(this);
      }
    });
  }

  public void postExecuteProcedures() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postExecuteProcedures(this);
      }
    });
  }

  /**
   * Coprocessor environment extension providing access to region server
   * related services.
   */
  private static class RegionServerEnvironment extends BaseEnvironment<RegionServerCoprocessor>
      implements RegionServerCoprocessorEnvironment {
    private final MetricRegistry metricRegistry;
    private final RegionServerServices services;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST",
        justification="Intentional; FB has trouble detecting isAssignableFrom")
    public RegionServerEnvironment(final RegionServerCoprocessor impl, final int priority,
        final int seq, final Configuration conf, final RegionServerServices services) {
      super(impl, priority, seq, conf);
      // If coprocessor exposes any services, register them.
      for (Service service : impl.getServices()) {
        services.registerService(service);
      }
      this.services = services;
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForRSCoprocessor(impl.getClass().getName());
    }

    @Override
    public OnlineRegions getOnlineRegions() {
      return this.services;
    }

    @Override
    public ServerName getServerName() {
      return this.services.getServerName();
    }

    @Override
    public Connection getConnection() {
      return new SharedConnection(this.services.getConnection());
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return this.services.createConnection(conf);
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(metricRegistry);
    }
  }

  /**
   * Special version of RegionServerEnvironment that exposes RegionServerServices for Core
   * Coprocessors only. Temporary hack until Core Coprocessors are integrated into Core.
   */
  private static class RegionServerEnvironmentForCoreCoprocessors extends RegionServerEnvironment
      implements HasRegionServerServices {
    final RegionServerServices regionServerServices;

    public RegionServerEnvironmentForCoreCoprocessors(final RegionServerCoprocessor impl,
          final int priority, final int seq, final Configuration conf,
          final RegionServerServices services) {
       super(impl, priority, seq, conf, services);
       this.regionServerServices = services;
    }

    /**
     * @return An instance of RegionServerServices, an object NOT for general user-space Coprocessor
     * consumption.
     */
    @Override
    public RegionServerServices getRegionServerServices() {
      return this.regionServerServices;
    }
  }
}

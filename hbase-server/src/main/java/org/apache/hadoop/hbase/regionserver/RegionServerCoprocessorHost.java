/**
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

import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
public class RegionServerCoprocessorHost extends
    CoprocessorHost<RegionServerCoprocessor, RegionServerCoprocessorEnvironment> {

  private static final Log LOG = LogFactory.getLog(RegionServerCoprocessorHost.class);

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
    return new RegionServerEnvironment(instance, priority, sequence, conf, this.rsServices);
  }

  @Override
  public RegionServerCoprocessor checkAndGetInstance(Class<?> implClass)
      throws InstantiationException, IllegalAccessException {
    if (RegionServerCoprocessor.class.isAssignableFrom(implClass)) {
      return (RegionServerCoprocessor)implClass.newInstance();
    } else if (SingletonCoprocessorService.class.isAssignableFrom(implClass)) {
      // For backward compatibility with old CoprocessorService impl which don't extend
      // RegionCoprocessor.
      return new CoprocessorServiceBackwardCompatiblity.RegionServerCoprocessorService(
          (SingletonCoprocessorService)implClass.newInstance());
    } else {
      LOG.error(implClass.getName() + " is not of type RegionServerCoprocessor. Check the "
          + "configuration " + CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY);
      return null;
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
    execShutdown(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation(user) {
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
    return execOperationWithResult(endpoint, coprocEnvironments.isEmpty() ? null :
        new ObserverOperationWithResult<RegionServerObserver, ReplicationEndpoint>(
            rsObserverGetter) {
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

  /**
   * Coprocessor environment extension providing access to region server
   * related services.
   */
  private static class RegionServerEnvironment extends BaseEnvironment<RegionServerCoprocessor>
      implements RegionServerCoprocessorEnvironment {
    private final RegionServerServices regionServerServices;
    private final MetricRegistry metricRegistry;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST",
        justification="Intentional; FB has trouble detecting isAssignableFrom")
    public RegionServerEnvironment(final RegionServerCoprocessor impl, final int priority,
        final int seq, final Configuration conf, final RegionServerServices services) {
      super(impl, priority, seq, conf);
      this.regionServerServices = services;
      // If coprocessor exposes any services, register them.
      for (Service service : impl.getServices()) {
        regionServerServices.registerService(service);
      }
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForRSCoprocessor(impl.getClass().getName());
    }

    @Override
    public CoprocessorRegionServerServices getCoprocessorRegionServerServices() {
      return regionServerServices;
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
}

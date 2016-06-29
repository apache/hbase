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
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class RegionServerCoprocessorHost extends
    CoprocessorHost<RegionServerCoprocessorHost.RegionServerEnvironment> {

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
  public RegionServerEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, int priority, int sequence, Configuration conf) {
    return new RegionServerEnvironment(implClass, instance, priority,
      sequence, conf, this.rsServices);
  }

  public void preStop(String message) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preStopRegionServer(ctx);
      }
      @Override
      public void postEnvCall(RegionServerEnvironment env) {
        // invoke coprocessor stop method
        shutdown(env);
      }
    });
  }

  public boolean preMerge(final HRegion regionA, final HRegion regionB, final User user) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preMerge(ctx, regionA, regionB);
      }
    });
  }

  public void postMerge(final HRegion regionA, final HRegion regionB, final HRegion mergedRegion,
                        final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postMerge(ctx, regionA, regionB, mergedRegion);
      }
    });
  }

  public boolean preMergeCommit(final HRegion regionA, final HRegion regionB,
      final @MetaMutationAnnotation List<Mutation> metaEntries, final User user)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preMergeCommit(ctx, regionA, regionB, metaEntries);
      }
    });
  }

  public void postMergeCommit(final HRegion regionA, final HRegion regionB,
      final HRegion mergedRegion, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postMergeCommit(ctx, regionA, regionB, mergedRegion);
      }
    });
  }

  public void preRollBackMerge(final HRegion regionA, final HRegion regionB, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preRollBackMerge(ctx, regionA, regionB);
      }
    });
  }

  public void postRollBackMerge(final HRegion regionA, final HRegion regionB, final User user)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation(user) {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postRollBackMerge(ctx, regionA, regionB);
      }
    });
  }

  public void preRollWALWriterRequest() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preRollWALWriterRequest(ctx);
      }
    });
  }

  public void postRollWALWriterRequest() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postRollWALWriterRequest(ctx);
      }
    });
  }

  public void preReplicateLogEntries(final List<WALEntry> entries, final CellScanner cells)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preReplicateLogEntries(ctx, entries, cells);
      }
    });
  }

  public void postReplicateLogEntries(final List<WALEntry> entries, final CellScanner cells)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postReplicateLogEntries(ctx, entries, cells);
      }
    });
  }

  public ReplicationEndpoint postCreateReplicationEndPoint(final ReplicationEndpoint endpoint)
      throws IOException {
    return execOperationWithResult(endpoint, coprocessors.isEmpty() ? null
        : new CoprocessOperationWithResult<ReplicationEndpoint>() {
          @Override
          public void call(RegionServerObserver oserver,
              ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
            setResult(oserver.postCreateReplicationEndPoint(ctx, getResult()));
          }
        });
  }

  private <T> T execOperationWithResult(final T defaultValue,
      final CoprocessOperationWithResult<T> ctx) throws IOException {
    if (ctx == null)
      return defaultValue;
    ctx.setResult(defaultValue);
    execOperation(ctx);
    return ctx.getResult();
  }

  private static abstract class CoprocessorOperation
      extends ObserverContext<RegionServerCoprocessorEnvironment> {
    public CoprocessorOperation() {
      this(RpcServer.getRequestUser());
    }

    public CoprocessorOperation(User user) {
      super(user);
    }

    public abstract void call(RegionServerObserver oserver,
        ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException;

    public void postEnvCall(RegionServerEnvironment env) {
    }
  }

  private static abstract class CoprocessOperationWithResult<T> extends CoprocessorOperation {
    private T result = null;

    public void setResult(final T result) {
      this.result = result;
    }

    public T getResult() {
      return this.result;
    }
  }

  private boolean execOperation(final CoprocessorOperation ctx) throws IOException {
    if (ctx == null) return false;
    boolean bypass = false;
    List<RegionServerEnvironment> envs = coprocessors.get();
    for (int i = 0; i < envs.size(); i++) {
      RegionServerEnvironment env = envs.get(i);
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx.prepare(env);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ctx.call((RegionServerObserver)env.getInstance(), ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
      ctx.postEnvCall(env);
    }
    return bypass;
  }

  /**
   * Coprocessor environment extension providing access to region server
   * related services.
   */
  static class RegionServerEnvironment extends CoprocessorHost.Environment
      implements RegionServerCoprocessorEnvironment {

    private RegionServerServices regionServerServices;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BC_UNCONFIRMED_CAST",
        justification="Intentional; FB has trouble detecting isAssignableFrom")
    public RegionServerEnvironment(final Class<?> implClass,
        final Coprocessor impl, final int priority, final int seq,
        final Configuration conf, final RegionServerServices services) {
      super(impl, priority, seq, conf);
      this.regionServerServices = services;
      for (Object itf : ClassUtils.getAllInterfaces(implClass)) {
        Class<?> c = (Class<?>) itf;
        if (SingletonCoprocessorService.class.isAssignableFrom(c)) {// FindBugs: BC_UNCONFIRMED_CAST
          this.regionServerServices.registerService(
            ((SingletonCoprocessorService) impl).getService());
          break;
        }
      }
    }

    @Override
    public RegionServerServices getRegionServerServices() {
      return regionServerServices;
    }
  }

  /**
   * Environment priority comparator. Coprocessors are chained in sorted
   * order.
   */
  static class EnvironmentPriorityComparator implements
      Comparator<CoprocessorEnvironment> {
    public int compare(final CoprocessorEnvironment env1,
        final CoprocessorEnvironment env2) {
      if (env1.getPriority() < env2.getPriority()) {
        return -1;
      } else if (env1.getPriority() > env2.getPriority()) {
        return 1;
      }
      if (env1.getLoadSequence() < env2.getLoadSequence()) {
        return -1;
      } else if (env1.getLoadSequence() > env2.getLoadSequence()) {
        return 1;
      }
      return 0;
    }
  }
}

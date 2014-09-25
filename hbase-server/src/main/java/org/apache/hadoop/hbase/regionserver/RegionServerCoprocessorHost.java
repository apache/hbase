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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class RegionServerCoprocessorHost extends
    CoprocessorHost<RegionServerCoprocessorHost.RegionServerEnvironment> {

  private RegionServerServices rsServices;

  public RegionServerCoprocessorHost(RegionServerServices rsServices,
      Configuration conf) {
    super(rsServices);
    this.rsServices = rsServices;
    this.conf = conf;
    // load system default cp's from configuration.
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

  public boolean preMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preMerge(ctx, regionA, regionB);
      }
    });
  }

  public void postMerge(final HRegion regionA, final HRegion regionB, final HRegion mergedRegion)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postMerge(ctx, regionA, regionB, mergedRegion);
      }
    });
  }

  public boolean preMergeCommit(final HRegion regionA, final HRegion regionB,
      final @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preMergeCommit(ctx, regionA, regionB, metaEntries);
      }
    });
  }

  public void postMergeCommit(final HRegion regionA, final HRegion regionB,
      final HRegion mergedRegion) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.postMergeCommit(ctx, regionA, regionB, mergedRegion);
      }
    });
  }

  public void preRollBackMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
      @Override
      public void call(RegionServerObserver oserver,
          ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException {
        oserver.preRollBackMerge(ctx, regionA, regionB);
      }
    });
  }

  public void postRollBackMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new CoprocessorOperation() {
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

  private static abstract class CoprocessorOperation
      extends ObserverContext<RegionServerCoprocessorEnvironment> {
    public CoprocessorOperation() {
    }

    public abstract void call(RegionServerObserver oserver,
        ObserverContext<RegionServerCoprocessorEnvironment> ctx) throws IOException;

    public void postEnvCall(RegionServerEnvironment env) {
    }
  }

  private boolean execOperation(final CoprocessorOperation ctx) throws IOException {
    if (ctx == null) return false;

    boolean bypass = false;
    for (RegionServerEnvironment env: coprocessors) {
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

    public RegionServerEnvironment(final Class<?> implClass,
        final Coprocessor impl, final int priority, final int seq,
        final Configuration conf, final RegionServerServices services) {
      super(impl, priority, seq, conf);
      this.regionServerServices = services;
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

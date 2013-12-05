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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.MetaMutationAnnotation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

public class RegionServerCoprocessorHost extends
    CoprocessorHost<RegionServerCoprocessorHost.RegionServerEnvironment> {

  private RegionServerServices rsServices;

  public RegionServerCoprocessorHost(RegionServerServices rsServices,
      Configuration conf) {
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
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        ((RegionServerObserver) env.getInstance()).preStopRegionServer(ctx);
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    boolean bypass = false;
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).preMerge(ctx, regionA, regionB);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  public void postMerge(final HRegion regionA, final HRegion regionB, final HRegion mergedRegion)
      throws IOException {
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).postMerge(ctx, regionA, regionB, mergedRegion);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preMergeCommit(final HRegion regionA, final HRegion regionB,
      final @MetaMutationAnnotation List<Mutation> metaEntries) throws IOException {
    boolean bypass = false;
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).preMergeCommit(ctx, regionA, regionB,
            metaEntries);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        bypass |= ctx.shouldBypass();
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  public void postMergeCommit(final HRegion regionA, final HRegion regionB,
      final HRegion mergedRegion) throws IOException {
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).postMergeCommit(ctx, regionA, regionB,
            mergedRegion);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preRollBackMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).preRollBackMerge(ctx, regionA, regionB);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postRollBackMerge(final HRegion regionA, final HRegion regionB) throws IOException {
    ObserverContext<RegionServerCoprocessorEnvironment> ctx = null;
    for (RegionServerEnvironment env : coprocessors) {
      if (env.getInstance() instanceof RegionServerObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        try {
          ((RegionServerObserver) env.getInstance()).postRollBackMerge(ctx, regionA, regionB);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
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

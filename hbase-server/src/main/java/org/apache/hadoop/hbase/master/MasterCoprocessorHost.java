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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

import java.io.IOException;
import java.util.List;

/**
 * Provides the coprocessor framework and environment for master oriented
 * operations.  {@link HMaster} interacts with the loaded coprocessors
 * through this class.
 */
@InterfaceAudience.Private
public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessorHost.MasterEnvironment> {

  /**
   * Coprocessor environment extension providing access to master related
   * services.
   */
  static class MasterEnvironment extends CoprocessorHost.Environment
      implements MasterCoprocessorEnvironment {
    private MasterServices masterServices;

    public MasterEnvironment(final Class<?> implClass, final Coprocessor impl,
        final int priority, final int seq, final Configuration conf,
        final MasterServices services) {
      super(impl, priority, seq, conf);
      this.masterServices = services;
    }

    public MasterServices getMasterServices() {
      return masterServices;
    }
  }

  private MasterServices masterServices;

  MasterCoprocessorHost(final MasterServices services, final Configuration conf) {
    this.conf = conf;
    this.masterServices = services;
    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterEnvironment createEnvironment(final Class<?> implClass,
      final Coprocessor instance, final int priority, final int seq,
      final Configuration conf) {
    for (Class<?> c : implClass.getInterfaces()) {
      if (CoprocessorService.class.isAssignableFrom(c)) {
        masterServices.registerService(((CoprocessorService)instance).getService());
      }
    }
    return new MasterEnvironment(implClass, instance, priority, seq, conf,
        masterServices);
  }

  @Override
  protected void abortServer(final CoprocessorEnvironment env, final Throwable e) {
    abortServer("master", masterServices, env, e);
  }

  public boolean preCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preCreateNamespace(ctx, ns);
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
    }
    return bypass;
  }

  public void postCreateNamespace(final NamespaceDescriptor ns) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postCreateNamespace(ctx, ns);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preDeleteNamespace(final String namespaceName) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preDeleteNamespace(ctx, namespaceName);
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
    }
    return bypass;
  }

  public void postDeleteNamespace(final String namespaceName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postDeleteNamespace(ctx, namespaceName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preModifyNamespace(final NamespaceDescriptor ns) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preModifyNamespace(ctx, ns);
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
    }
    return bypass;
  }

  public void postModifyNamespace(final NamespaceDescriptor ns) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postModifyNamespace(ctx, ns);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  /* Implementation of hooks for invoking MasterObservers */

  public void preCreateTable(final HTableDescriptor htd, final HRegionInfo[] regions)
    throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preCreateTable(ctx, htd, regions);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postCreateTable(final HTableDescriptor htd, final HRegionInfo[] regions)
    throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postCreateTable(ctx, htd, regions);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preCreateTableHandler(final HTableDescriptor htd, final HRegionInfo[] regions)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preCreateTableHandler(ctx, htd, regions);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postCreateTableHandler(final HTableDescriptor htd, final HRegionInfo[] regions)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postCreateTableHandler(ctx, htd, regions);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preDeleteTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preDeleteTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postDeleteTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postDeleteTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preDeleteTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preDeleteTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postDeleteTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postDeleteTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preModifyTable(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preModifyTable(ctx, tableName, htd);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postModifyTable(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postModifyTable(ctx, tableName, htd);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preModifyTableHandler(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preModifyTableHandler(ctx, tableName, htd);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postModifyTableHandler(final TableName tableName, final HTableDescriptor htd)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postModifyTableHandler(ctx, tableName, htd);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preAddColumn(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preAddColumn(ctx, tableName, column);
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
    }
    return bypass;
  }

  public void postAddColumn(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postAddColumn(ctx, tableName, column);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preAddColumnHandler(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preAddColumnHandler(ctx, tableName, column);
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
    }
    return bypass;
  }

  public void postAddColumnHandler(final TableName tableName, final HColumnDescriptor column)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postAddColumnHandler(ctx, tableName, column);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preModifyColumn(final TableName tableName, final HColumnDescriptor descriptor)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preModifyColumn(ctx, tableName, descriptor);
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
    }
    return bypass;
  }

  public void postModifyColumn(final TableName tableName, final HColumnDescriptor descriptor)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postModifyColumn(ctx, tableName, descriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preModifyColumnHandler(final TableName tableName,
      final HColumnDescriptor descriptor) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preModifyColumnHandler(ctx, tableName, descriptor);
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
    }
    return bypass;
  }

  public void postModifyColumnHandler(final TableName tableName,
      final HColumnDescriptor descriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postModifyColumnHandler(ctx, tableName, descriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preDeleteColumn(final TableName tableName, final byte [] c) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preDeleteColumn(ctx, tableName, c);
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
    }
    return bypass;
  }

  public void postDeleteColumn(final TableName tableName, final byte [] c) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postDeleteColumn(ctx, tableName, c);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preDeleteColumnHandler(final TableName tableName, final byte[] c)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preDeleteColumnHandler(ctx, tableName, c);
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
    }
    return bypass;
  }

  public void postDeleteColumnHandler(final TableName tableName, final byte[] c)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postDeleteColumnHandler(ctx, tableName, c);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preEnableTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preEnableTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postEnableTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postEnableTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preEnableTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preEnableTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postEnableTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postEnableTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preDisableTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preDisableTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postDisableTable(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postDisableTable(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preDisableTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preDisableTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postDisableTableHandler(final TableName tableName) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postDisableTableHandler(ctx, tableName);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preMove(final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preMove(ctx, region, srcServer, destServer);
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
    }
    return bypass;
  }

  public void postMove(final HRegionInfo region, final ServerName srcServer,
      final ServerName destServer) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postMove(ctx, region, srcServer, destServer);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preAssign(final HRegionInfo regionInfo) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preAssign(ctx, regionInfo);
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
    }
    return bypass;
  }

  public void postAssign(final HRegionInfo regionInfo) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postAssign(ctx, regionInfo);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preUnassign(final HRegionInfo regionInfo, final boolean force)
      throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preUnassign(ctx, regionInfo, force);
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
    }
    return bypass;
  }

  public void postUnassign(final HRegionInfo regionInfo, final boolean force) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postUnassign(ctx, regionInfo, force);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preRegionOffline(final HRegionInfo regionInfo) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preRegionOffline(ctx, regionInfo);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postRegionOffline(final HRegionInfo regionInfo) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).postRegionOffline(ctx, regionInfo);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preBalance() throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preBalance(ctx);
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
    }
    return bypass;
  }

  public void postBalance(final List<RegionPlan> plans) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postBalance(ctx, plans);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preBalanceSwitch(final boolean b) throws IOException {
    boolean balance = b;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          balance = ((MasterObserver)env.getInstance()).preBalanceSwitch(ctx, balance);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
    return balance;
  }

  void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postBalanceSwitch(ctx, oldValue, newValue);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preShutdown() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preShutdown(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preStopMaster() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preStopMaster(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preMasterInitialization() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preMasterInitialization(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postStartMaster() throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postStartMaster(ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preSnapshot(ctx, snapshot, hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postSnapshot(ctx, snapshot, hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preCloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preCloneSnapshot(ctx, snapshot,
            hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postCloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postCloneSnapshot(ctx, snapshot,
            hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preRestoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preRestoreSnapshot(ctx, snapshot,
            hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postRestoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postRestoreSnapshot(ctx, snapshot,
            hTableDescriptor);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void preDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).preDeleteSnapshot(ctx, snapshot);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public void postDeleteSnapshot(final SnapshotDescription snapshot) throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postDeleteSnapshot(ctx, snapshot);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

  public boolean preGetTableDescriptors(final List<TableName> tableNamesList,
      final List<HTableDescriptor> descriptors) throws IOException {
    boolean bypass = false;
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env : coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver) env.getInstance()).preGetTableDescriptors(ctx,
            tableNamesList, descriptors);
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
    }
    return bypass;
  }

  public void postGetTableDescriptors(final List<HTableDescriptor> descriptors)
      throws IOException {
    ObserverContext<MasterCoprocessorEnvironment> ctx = null;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((MasterObserver)env.getInstance()).postGetTableDescriptors(ctx, descriptors);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        if (ctx.shouldComplete()) {
          break;
        }
      }
    }
  }

}

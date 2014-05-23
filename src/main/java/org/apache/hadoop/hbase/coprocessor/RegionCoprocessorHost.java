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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections.map.AbstractReferenceMap;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.environments.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.observers.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.observers.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link HRegion}.
 */
public class RegionCoprocessorHost extends
    CoprocessorHost<RegionCoprocessorHost.RegionEnvironment> {
  private static final Log LOG = LogFactory.getLog(RegionCoprocessorHost.class);
  private static ReferenceMap sharedDataMap = new ReferenceMap(
      AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK);
  private HRegion region;

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public RegionCoprocessorHost(final HRegion region,
       final Configuration conf) {
    this.conf = conf;
    this.region = region;
    this.pathPrefix = Integer.toString(this.region.getRegionInfo().hashCode());
    // if configuration contains coprocessor for this table then we should do
    // the loading - otherwise not!

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, REGION_COPROCESSOR_CONF_KEY);

    // load system default cp's for user tables from configuration.
    //TODO: check whether this checks for ROOT too
    if (!region.getRegionInfo().getTableDesc().isMetaTable()
        && !region.getRegionInfo().getTableDesc().isRootRegion()) {
      loadSystemCoprocessors(conf, USER_REGION_COPROCESSOR_CONF_KEY);
    }
  }

  /**
   * Used mainly when we dynamically reload the configuration
   *
   * @throws IOException
   */
  public void reloadCoprocessors(Configuration newConf) throws IOException {
    Map<String, List<Pair<String, String>>> map = this
        .readCoprocessorsFromConf(newConf);
    String coprocTable = this.region.getTableDesc().getNameAsString();
    List<Pair<String, String>> forTable = map.get(coprocTable);
    // reload system default cp's from configuration.
    reloadSysCoprocessorsOnConfigChange(newConf, REGION_COPROCESSOR_CONF_KEY);
    // reload system default cp's for user tables from configuration.
    if (!region.getRegionInfo().getTableDesc().isMetaRegion()
        && !region.getRegionInfo().getTableDesc().isRootRegion()) {
      if (forTable != null) {
        reloadCoprocessorsFromHdfs(coprocTable, forTable, newConf);
      }
      reloadSysCoprocessorsOnConfigChange(newConf,
          USER_REGION_COPROCESSOR_CONF_KEY);
    }
  }

  @Override
  public RegionEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, int priority, int seq, Configuration conf, String confKey) {
    // Check if it's an Endpoint.
    // Due to current dynamic protocol design, Endpoint
    // uses a different way to be registered and executed.
    // It uses a visitor pattern to invoke registered Endpoint
    // method.
    //TODO: we probably are not going to need this since
//    for (Class<?> c : implClass.getInterfaces()) {
//      if (CoprocessorService.class.isAssignableFrom(c)) {
//        region.registerService( ((CoprocessorService)instance).getService() );
//      }
//    }
    ConcurrentMap<String, Object> classData;
    // make sure only one thread can add maps
    synchronized (sharedDataMap) {
      // as long as at least one RegionEnvironment holds on to its classData it will
      // remain in this map
      classData = (ConcurrentMap<String, Object>)sharedDataMap.get(implClass.getName());
      if (classData == null) {
        classData = new ConcurrentHashMap<String, Object>();
        sharedDataMap.put(implClass.getName(), classData);
      }
    }
    return new RegionEnvironment(instance, priority, seq, conf, region,
        classData, confKey);
  }

  /**
   * HBASE-4014 : This is used by coprocessor hooks which are not declared to throw exceptions.
   *
   * For example, {@link
   * org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost#preOpen()} and
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost#postOpen()} are such hooks.
   *
   * See also
   * {@link org.apache.hadoop.hbase.master.MasterCoprocessorHost#handleCoprocessorThrowable(
   *    CoprocessorEnvironment, Throwable)}
   * @param env The coprocessor that threw the exception.
   * @param e The exception that was thrown.
   */
  private void handleCoprocessorThrowableNoRethrow(
      final CoprocessorEnvironment env, final Throwable e) {
    try {
      handleCoprocessorThrowable(env,e);
    } catch (IOException ioe) {
      // We cannot throw exceptions from the caller hook, so ignore.
      LOG.warn(
        "handleCoprocessorThrowable() threw an IOException while attempting to handle Throwable " +
        e + ". Ignoring.",e);
    }
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @param durability The durability used
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Put put, final WALEdit edit, boolean durability)
      throws IOException {
    boolean bypass = false;
    ObserverContext<RegionCoprocessorEnvironment> ctx = null;
    for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        ctx = ObserverContext.createAndPrepare(env, ctx);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ((RegionObserver)env.getInstance()).prePut(ctx, put, edit, durability);
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

  /**
   * Encapsulation of the environment of each coprocessor
   */
  static class RegionEnvironment extends CoprocessorHost.Environment
      implements RegionCoprocessorEnvironment {

    private HRegion region;
    ConcurrentMap<String, Object> sharedData;
    private byte[] tableName;
    private String keyForLoading;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf, final HRegion region,
        final ConcurrentMap<String, Object> sharedData, String keyForLoading) {
      super(impl, priority, seq, conf, keyForLoading);
      this.region = region;
      this.sharedData = sharedData;
      this.tableName = region.getTableDesc().getName();
    }

    /** @return the region */
    @Override
    public HRegion getRegion() {
      return region;
    }


    public void shutdown() {
      super.shutdown();
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
      return sharedData;
    }

    @Override
    public byte[] getTableName() {
      return tableName;
    }

    public String getKeyForLoading() {
      return keyForLoading;
    }
  }
}

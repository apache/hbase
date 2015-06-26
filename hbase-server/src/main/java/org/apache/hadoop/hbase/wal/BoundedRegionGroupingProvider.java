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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
// imports for classes still in regionserver.wal
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;

/**
 * A WAL Provider that pre-creates N WALProviders and then limits our grouping strategy to them.
 * Control the number of delegate providers via "hbase.wal.regiongrouping.numgroups." Control
 * the choice of delegate provider implementation and the grouping strategy the same as
 * {@link RegionGroupingProvider}.
 */
@InterfaceAudience.Private
public class BoundedRegionGroupingProvider extends RegionGroupingProvider {
  private static final Log LOG = LogFactory.getLog(BoundedRegionGroupingProvider.class);

  static final String NUM_REGION_GROUPS = "hbase.wal.regiongrouping.numgroups";
  static final int DEFAULT_NUM_REGION_GROUPS = 2;
  private WALProvider[] delegates;
  private AtomicInteger counter = new AtomicInteger(0);

  @Override
  public void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, final String providerId) throws IOException {
    super.init(factory, conf, listeners, providerId);
    // no need to check for and close down old providers; our parent class will throw on re-invoke
    delegates = new WALProvider[Math.max(1, conf.getInt(NUM_REGION_GROUPS,
        DEFAULT_NUM_REGION_GROUPS))];
    for (int i = 0; i < delegates.length; i++) {
      delegates[i] = factory.getProvider(DELEGATE_PROVIDER, DEFAULT_DELEGATE_PROVIDER, listeners,
          providerId + i);
    }
    LOG.info("Configured to run with " + delegates.length + " delegate WAL providers.");
  }

  @Override
  WALProvider populateCache(final byte[] group) {
    final WALProvider temp = delegates[counter.getAndIncrement() % delegates.length];
    final WALProvider extant = cached.putIfAbsent(group, temp);
    // if someone else beat us to initializing, just take what they set.
    // note that in such a case we skew load away from the provider we picked at first
    return extant == null ? temp : extant;
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (WALProvider provider : delegates) {
      try {
        provider.shutdown();
      } catch (IOException exception) {
        LOG.error("Problem shutting down provider '" + provider + "': " + exception.getMessage());
        LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
        failure = exception;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (WALProvider provider : delegates) {
      try {
        provider.close();
      } catch (IOException exception) {
        LOG.error("Problem closing provider '" + provider + "': " + exception.getMessage());
        LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
        failure = exception;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * iff the given WALFactory is using the BoundedRegionGroupingProvider for meta and/or non-meta,
   * count the number of files (rolled and active). if either of them isn't, count 0
   * for that provider.
   * @param walFactory may not be null.
   */
  public static long getNumLogFiles(WALFactory walFactory) {
    long result = 0;
    if (walFactory.provider instanceof BoundedRegionGroupingProvider) {
      BoundedRegionGroupingProvider groupProviders =
          (BoundedRegionGroupingProvider)walFactory.provider;
      for (int i = 0; i < groupProviders.delegates.length; i++) {
        result +=
            ((FSHLog)((DefaultWALProvider)(groupProviders.delegates[i])).log).getNumLogFiles();
      }
    }
    WALProvider meta = walFactory.metaProvider.get();
    if (meta instanceof BoundedRegionGroupingProvider) {
      for (int i = 0; i < ((BoundedRegionGroupingProvider)meta).delegates.length; i++) {
        result += ((FSHLog)
            ((DefaultWALProvider)(((BoundedRegionGroupingProvider)meta).delegates[i])).log)
            .getNumLogFiles();      }
    }
    return result;
  }

  /**
   * iff the given WALFactory is using the BoundedRegionGroupingProvider for meta and/or non-meta,
   * count the size of files (rolled and active). if either of them isn't, count 0
   * for that provider.
   * @param walFactory may not be null.
   */
  public static long getLogFileSize(WALFactory walFactory) {
    long result = 0;
    if (walFactory.provider instanceof BoundedRegionGroupingProvider) {
      BoundedRegionGroupingProvider groupProviders =
          (BoundedRegionGroupingProvider)walFactory.provider;
      for (int i = 0; i < groupProviders.delegates.length; i++) {
        result +=
            ((FSHLog)((DefaultWALProvider)(groupProviders.delegates[i])).log).getLogFileSize();
      }
    }
    WALProvider meta = walFactory.metaProvider.get();
    if (meta instanceof BoundedRegionGroupingProvider) {
      for (int i = 0; i < ((BoundedRegionGroupingProvider)meta).delegates.length; i++) {
        result += ((FSHLog)
            ((DefaultWALProvider)(((BoundedRegionGroupingProvider)meta).delegates[i])).log)
            .getLogFileSize();
      }
    }
    return result;
  }
}

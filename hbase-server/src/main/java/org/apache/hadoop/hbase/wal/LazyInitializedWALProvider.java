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
package org.apache.hadoop.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.wal.WALFactory.Providers;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A lazy initialized WAL provider for holding the WALProvider for some special tables, such as
 * hbase:meta, hbase:replication, etc.
 */
@InterfaceAudience.Private
class LazyInitializedWALProvider implements Closeable {

  private final WALFactory factory;

  private final String providerId;

  private final String providerConfigName;

  private final Abortable abortable;

  private final AtomicReference<WALProvider> holder = new AtomicReference<>();

  LazyInitializedWALProvider(WALFactory factory, String providerId, String providerConfigName,
    Abortable abortable) {
    this.factory = factory;
    this.providerId = providerId;
    this.providerConfigName = providerConfigName;
    this.abortable = abortable;
  }

  WALProvider getProvider() throws IOException {
    Configuration conf = factory.getConf();
    for (;;) {
      WALProvider provider = this.holder.get();
      if (provider != null) {
        return provider;
      }
      Class<? extends WALProvider> clz = null;
      if (conf.get(providerConfigName) == null) {
        try {
          clz = conf.getClass(WALFactory.WAL_PROVIDER, Providers.defaultProvider.clazz,
            WALProvider.class);
        } catch (Throwable t) {
          // the WAL provider should be an enum. Proceed
        }
      }
      if (clz == null) {
        clz = factory.getProviderClass(providerConfigName,
          conf.get(WALFactory.WAL_PROVIDER, WALFactory.DEFAULT_WAL_PROVIDER));
      }
      provider = WALFactory.createProvider(clz);
      provider.init(factory, conf, providerId, this.abortable);
      provider.addWALActionsListener(new MetricsWAL());
      if (this.holder.compareAndSet(null, provider)) {
        return provider;
      } else {
        // someone is ahead of us, close and try again.
        provider.close();
      }
    }
  }

  /**
   * Get the provider if it already initialized, otherwise just return {@code null} instead of
   * creating it.
   */
  WALProvider getProviderNoCreate() {
    return holder.get();
  }

  @Override
  public void close() throws IOException {
    WALProvider provider = this.holder.get();
    if (provider != null) {
      provider.close();
    }
  }

  void shutdown() throws IOException {
    WALProvider provider = this.holder.get();
    if (provider != null) {
      provider.shutdown();
    }
  }
}

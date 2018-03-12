
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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link WAL}.
 */
@InterfaceAudience.Private
public class WALCoprocessorHost
    extends CoprocessorHost<WALCoprocessor, WALCoprocessorEnvironment> {
  private static final Logger LOG = LoggerFactory.getLogger(WALCoprocessorHost.class);

  /**
   * Encapsulation of the environment of each coprocessor
   */
  static class WALEnvironment extends BaseEnvironment<WALCoprocessor>
    implements WALCoprocessorEnvironment {

    private final WAL wal;

    private final MetricRegistry metricRegistry;

    @Override
    public WAL getWAL() {
      return wal;
    }

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     * @param seq load sequence
     * @param conf configuration
     * @param wal WAL
     */
    private WALEnvironment(final WALCoprocessor impl, final int priority, final int seq,
        final Configuration conf, final WAL wal) {
      super(impl, priority, seq, conf);
      this.wal = wal;
      this.metricRegistry = MetricsCoprocessor.createRegistryForWALCoprocessor(
          impl.getClass().getName());
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }
  }

  private final WAL wal;

  /**
   * Constructor
   * @param log the write ahead log
   * @param conf the configuration
   */
  public WALCoprocessorHost(final WAL log, final Configuration conf) {
    // We don't want to require an Abortable passed down through (FS)HLog, so
    // this means that a failure to load of a WAL coprocessor won't abort the
    // server. This isn't ideal, and means that security components that
    // utilize a WALObserver will have to check the observer initialization
    // state manually. However, WALObservers will eventually go away so it
    // should be an acceptable state of affairs.
    super(null);
    this.wal = log;
    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, WAL_COPROCESSOR_CONF_KEY);
  }

  @Override
  public WALEnvironment createEnvironment(final WALCoprocessor instance, final int priority,
      final int seq, final Configuration conf) {
    return new WALEnvironment(instance, priority, seq, conf, this.wal);
  }

  @Override
  public WALCoprocessor checkAndGetInstance(Class<?> implClass) throws IllegalAccessException,
      InstantiationException {
    if (WALCoprocessor.class.isAssignableFrom(implClass)) {
      try {
        return implClass.asSubclass(WALCoprocessor.class).getDeclaredConstructor().newInstance();
      } catch (NoSuchMethodException | InvocationTargetException e) {
        throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
      }
    } else {
      LOG.error(implClass.getName() + " is not of type WALCoprocessor. Check the "
          + "configuration " + CoprocessorHost.WAL_COPROCESSOR_CONF_KEY);
      return null;
    }
  }

  private ObserverGetter<WALCoprocessor, WALObserver> walObserverGetter =
      WALCoprocessor::getWALObserver;

  abstract class WALObserverOperation extends
      ObserverOperationWithoutResult<WALObserver> {
    public WALObserverOperation() {
      super(walObserverGetter);
    }
  }

  public void preWALWrite(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    // Not bypassable.
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new WALObserverOperation() {
      @Override
      public void call(WALObserver oserver) throws IOException {
        oserver.preWALWrite(this, info, logKey, logEdit);
      }
    });
  }

  public void postWALWrite(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new WALObserverOperation() {
      @Override
      protected void call(WALObserver observer) throws IOException {
        observer.postWALWrite(this, info, logKey, logEdit);
      }
    });
  }

  /**
   * Called before rolling the current WAL
   * @param oldPath the path of the current wal that we are replacing
   * @param newPath the path of the wal we are going to create
   */
  public void preWALRoll(Path oldPath, Path newPath) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new WALObserverOperation() {
      @Override
      protected void call(WALObserver observer) throws IOException {
        observer.preWALRoll(this, oldPath, newPath);
      }
    });
  }

  /**
   * Called after rolling the current WAL
   * @param oldPath the path of the wal that we replaced
   * @param newPath the path of the wal we have created and now is the current
   */
  public void postWALRoll(Path oldPath, Path newPath) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new WALObserverOperation() {
      @Override
      protected void call(WALObserver observer) throws IOException {
        observer.postWALRoll(this, oldPath, newPath);
      }
    });
  }
}

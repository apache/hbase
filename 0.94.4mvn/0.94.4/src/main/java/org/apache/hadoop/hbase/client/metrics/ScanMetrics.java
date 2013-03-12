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

package org.apache.hadoop.hbase.client.metrics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;


/**
 * Provides client-side metrics related to scan operations
 * The data can be passed to mapreduce framework or other systems.
 * Currently metrics framework won't be able to support the scenario
 * where multiple scan instances run on the same machine trying to
 * update the same metric. We use metrics objects in the class,
 * so that it can be easily switched to metrics framework later when it support
 * this scenario.
 * Some of these metrics are general for any client operation such as put
 * However, there is no need for this. So they are defined under scan operation
 * for now.
 */
public class ScanMetrics implements Writable {

  private static final byte SCANMETRICS_VERSION = (byte)1;
  private static final Log LOG = LogFactory.getLog(ScanMetrics.class);
  private MetricsRegistry registry = new MetricsRegistry();

  /**
   * number of RPC calls
   */
  public final MetricsTimeVaryingLong countOfRPCcalls =
    new MetricsTimeVaryingLong("RPC_CALLS", registry);

  /**
   * number of remote RPC calls
   */
  public final MetricsTimeVaryingLong countOfRemoteRPCcalls =
    new MetricsTimeVaryingLong("REMOTE_RPC_CALLS", registry);

  /**
   * sum of milliseconds between sequential next calls
   */
  public final MetricsTimeVaryingLong sumOfMillisSecBetweenNexts =
    new MetricsTimeVaryingLong("MILLIS_BETWEEN_NEXTS", registry);

  /**
   * number of NotServingRegionException caught
   */
  public final MetricsTimeVaryingLong countOfNSRE =
    new MetricsTimeVaryingLong("NOT_SERVING_REGION_EXCEPTION", registry);

  /**
   * number of bytes in Result objects from region servers
   */
  public final MetricsTimeVaryingLong countOfBytesInResults =
    new MetricsTimeVaryingLong("BYTES_IN_RESULTS", registry);

  /**
   * number of bytes in Result objects from remote region servers
   */
  public final MetricsTimeVaryingLong countOfBytesInRemoteResults =
    new MetricsTimeVaryingLong("BYTES_IN_REMOTE_RESULTS", registry);

  /**
   * number of regions
   */
  public final MetricsTimeVaryingLong countOfRegions =
    new MetricsTimeVaryingLong("REGIONS_SCANNED", registry);

  /**
   * number of RPC retries
   */
  public final MetricsTimeVaryingLong countOfRPCRetries =
    new MetricsTimeVaryingLong("RPC_RETRIES", registry);

  /**
   * number of remote RPC retries
   */
  public final MetricsTimeVaryingLong countOfRemoteRPCRetries =
    new MetricsTimeVaryingLong("REMOTE_RPC_RETRIES", registry);

  /**
   * constructor
   */
  public ScanMetrics () {
  }

  /**
   * serialize all the MetricsTimeVaryingLong
   */
  public void write(DataOutput out) throws IOException {
    out.writeByte(SCANMETRICS_VERSION);
    Collection<MetricsBase> mbs = registry.getMetricsList();

    // we only handle MetricsTimeVaryingLong for now.
    int metricsCount = 0;
    for (MetricsBase mb : mbs) {
      if ( mb instanceof MetricsTimeVaryingLong) {
        metricsCount++;
      } else {
        throw new IOException("unsupported metrics type. metrics name: "
          + mb.getName() + ", metrics description: " + mb.getDescription());
      }
    }

    out.writeInt(metricsCount);
    for (MetricsBase mb : mbs) {
      out.writeUTF(mb.getName());
      out.writeLong(((MetricsTimeVaryingLong) mb).getCurrentIntervalValue());
    }
  }

  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version > (int)SCANMETRICS_VERSION) {
      throw new IOException("version " + version + " not supported");
    }

    int metricsCount = in.readInt();
    for (int i=0; i<metricsCount; i++) {
      String metricsName = in.readUTF();
      long v = in.readLong();
      MetricsBase mb = registry.get(metricsName);
      if ( mb instanceof MetricsTimeVaryingLong) {
        ((MetricsTimeVaryingLong) mb).inc(v);
      } else {
        LOG.warn("unsupported metrics type. metrics name: "
          + mb.getName() + ", metrics description: " + mb.getDescription());
      }
    }
  }

  public MetricsTimeVaryingLong[] getMetricsTimeVaryingLongArray() {
    Collection<MetricsBase> mbs = registry.getMetricsList();
    ArrayList<MetricsTimeVaryingLong> mlv =
      new ArrayList<MetricsTimeVaryingLong>();
    for (MetricsBase mb : mbs) {
      if ( mb instanceof MetricsTimeVaryingLong) {
        mlv.add((MetricsTimeVaryingLong) mb);
      }
    }
    return mlv.toArray(new MetricsTimeVaryingLong[mlv.size()]);
  }

}

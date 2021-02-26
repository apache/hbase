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

package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsIOSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "IO";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about FileSystem IO";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


  String FS_READ_TIME_HISTO_KEY = "fsReadTime";
  String FS_PREAD_TIME_HISTO_KEY = "fsPReadTime";
  String FS_WRITE_HISTO_KEY = "fsWriteTime";

  String CHECKSUM_FAILURES_KEY = "fsChecksumFailureCount";

  String FS_READ_TIME_HISTO_DESC
    = "Latency of HFile's sequential reads on this region server in milliseconds";
  String FS_PREAD_TIME_HISTO_DESC
    = "Latency of HFile's positional reads on this region server in milliseconds";
  String FS_WRITE_TIME_HISTO_DESC
    = "Latency of HFile's writes on this region server in milliseconds";

  String CHECKSUM_FAILURES_DESC = "Number of checksum failures for the HBase HFile checksums at the"
      + " HBase level (separate from HDFS checksums)";


  /**
   * Update the fs sequential read time histogram
   * @param t time it took, in milliseconds
   */
  void updateFsReadTime(long t);

  /**
   * Update the fs positional read time histogram
   * @param t time it took, in milliseconds
   */
  void updateFsPReadTime(long t);

  /**
   * Update the fs write time histogram
   * @param t time it took, in milliseconds
   */
  void updateFsWriteTime(long t);
}

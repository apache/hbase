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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Convert HBase tabular data from multiple scanners into a format that
 * is consumable by Map/Reduce.
 *
 * <p>
 * Usage example
 * </p>
 *
 * <pre>
 * List&lt;Scan&gt; scans = new ArrayList&lt;Scan&gt;();
 *
 * Scan scan1 = new Scan();
 * scan1.setStartRow(firstRow1);
 * scan1.setStopRow(lastRow1);
 * scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table1);
 * scans.add(scan1);
 *
 * Scan scan2 = new Scan();
 * scan2.setStartRow(firstRow2);
 * scan2.setStopRow(lastRow2);
 * scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table2);
 * scans.add(scan2);
 *
 * TableMapReduceUtil.initTableMapperJob(scans, TableMapper.class, Text.class,
 *     IntWritable.class, job);
 * </pre>
 */
@InterfaceAudience.Public
public class MultiTableInputFormat extends MultiTableInputFormatBase implements
    Configurable {

  /** Job parameter that specifies the scan list. */
  public static final String SCANS = "hbase.mapreduce.scans";

  /** The configuration. */
  private Configuration conf = null;

  /**
   * Returns the current configuration.
   *
   * @return The current configuration.
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the configuration. This is used to set the details for the tables to
   *  be scanned.
   *
   * @param configuration The configuration to set.
   * @see org.apache.hadoop.conf.Configurable#setConf(
   *        org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    String[] rawScans = conf.getStrings(SCANS);
    if (rawScans.length <= 0) {
      throw new IllegalArgumentException("There must be at least 1 scan configuration set to : "
          + SCANS);
    }
    List<Scan> scans = new ArrayList<>();

    for (int i = 0; i < rawScans.length; i++) {
      try {
        scans.add(TableMapReduceUtil.convertStringToScan(rawScans[i]));
      } catch (IOException e) {
        throw new RuntimeException("Failed to convert Scan : " + rawScans[i] + " to string", e);
      }
    }
    this.setScans(scans);
  }
}

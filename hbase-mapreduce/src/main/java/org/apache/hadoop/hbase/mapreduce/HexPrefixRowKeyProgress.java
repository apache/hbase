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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link RowKeyProgress} implementation for tables whose row keys start with a hex-encoded prefix
 * (e.g. MD5 hashes like {@code "a3f2b1..."}). Only the hex prefix is used for progress estimation;
 * bytes beyond the prefix length are ignored.
 * <p>
 * The prefix length is configurable via {@link #PREFIX_LENGTH_KEY} and defaults to
 * {@link #DEFAULT_PREFIX_LENGTH}.
 * <p>
 * Configure via:
 *
 * <pre>
 * conf.setClass("hbase.mapreduce.rowkey.progress.class", HexPrefixRowKeyProgress.class,
 *   RowKeyProgress.class);
 * conf.setInt("hbase.mapreduce.rowkey.progress.hex.prefix.length", 8);
 * </pre>
 */
@InterfaceAudience.Public
public class HexPrefixRowKeyProgress extends Configured implements RowKeyProgress {
  public static final String PREFIX_LENGTH_KEY =
    "hbase.mapreduce.rowkey.progress.hex.prefix.length";
  public static final int DEFAULT_PREFIX_LENGTH = 4;

  private int prefixLength = DEFAULT_PREFIX_LENGTH;
  private double start;
  private double stop;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      this.prefixLength = conf.getInt(PREFIX_LENGTH_KEY, DEFAULT_PREFIX_LENGTH);
    }
  }

  @Override
  public void setStartStopRows(byte[] startRow, byte[] stopRow) {
    this.start = hexPrefixToDouble(startRow);
    this.stop = hexPrefixToDouble(stopRow);
  }

  @Override
  public float getProgress(byte[] currentRow) {
    if (currentRow == null || stop <= start) {
      return 0.0f;
    }
    double current = hexPrefixToDouble(currentRow);
    float progress = (float) ((current - start) / (stop - start));
    return Math.min(1.0f, Math.max(0.0f, progress));
  }

  private double hexPrefixToDouble(byte[] row) {
    if (row == null) {
      return 0;
    }
    int len = Math.min(prefixLength, row.length);
    double d = 0;
    for (int i = 0; i < prefixLength; i++) {
      d *= 16;
      if (i < len) {
        d += hexCharToInt(row[i]);
      }
    }
    return d;
  }

  private static int hexCharToInt(byte b) {
    if (b >= '0' && b <= '9') {
      return b - '0';
    }
    if (b >= 'a' && b <= 'f') {
      return 10 + (b - 'a');
    }
    return 0;
  }
}

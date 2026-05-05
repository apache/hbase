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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link RowKeyProgress} implementation for hex-encoded row keys (e.g. MD5/SHA prefixes). Non-hex
 * bytes contribute zero.
 */
@InterfaceAudience.Public
public class HexStringRowKeyProgress implements RowKeyProgress {
  /**
   * Cap on hex characters interpreted. A {@code double} mantissa carries ~53 bits (~13 hex chars);
   * reading more adds no information and risks precision loss.
   */
  private static final int MAX_PREFIX_LENGTH = 13;

  /**
   * Hex characters past the start/stop divergence point to include for resolution. 4 hex chars = 65
   * 536 buckets, finer than any progress bar can display.
   */
  private static final int RESOLUTION_PADDING = 4;

  private int prefixLength;
  private double start;
  private double stop;

  @Override
  public void setStartStopRows(byte[] startRow, byte[] stopRow) {
    int common = commonPrefixLength(startRow, stopRow);
    this.prefixLength = Math.min(common + RESOLUTION_PADDING, MAX_PREFIX_LENGTH);
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

  private static int commonPrefixLength(byte[] a, byte[] b) {
    if (a == null || b == null) {
      return 0;
    }
    return Bytes.findCommonPrefix(a, b, a.length, b.length, 0, 0);
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

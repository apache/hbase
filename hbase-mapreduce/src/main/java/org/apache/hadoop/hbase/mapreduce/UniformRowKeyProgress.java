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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link RowKeyProgress} implementation that treats row keys as raw byte sequences. Converts the
 * leading bytes to a big-endian unsigned numeric value and computes progress as a linear fraction
 * of the key space.
 */
@InterfaceAudience.Public
public class UniformRowKeyProgress implements RowKeyProgress {
  private static final int BYTES_FOR_PROGRESS = Double.BYTES;

  private double start;
  private double stop;

  @Override
  public void setStartStopRows(byte[] startRow, byte[] stopRow) {
    this.start = rowKeyToDouble(startRow);
    this.stop = rowKeyToDouble(stopRow);
  }

  @Override
  public float getProgress(byte[] currentRow) {
    if (currentRow == null || stop <= start) {
      return 0.0f;
    }
    double current = rowKeyToDouble(currentRow);
    float progress = (float) ((current - start) / (stop - start));
    return Math.min(1.0f, Math.max(0.0f, progress));
  }

  /**
   * Interpret the leading bytes of a row key as an unsigned big-endian value. Keys shorter than
   * {@link #BYTES_FOR_PROGRESS} bytes are treated as if right-padded with zeros.
   */
  private static double rowKeyToDouble(byte[] row) {
    if (row == null) {
      return 0;
    }
    double d = 0;
    for (int i = 0; i < BYTES_FOR_PROGRESS; i++) {
      d *= 256; // shift left by one byte (2^8) to build a big-endian base-256 number
      if (i < row.length) {
        d += (row[i] & 0xFF);
      }
    }
    return d;
  }
}

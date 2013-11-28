/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.io.hfile.histogram;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.HFileStat;

public interface NumericHistogram {
  public static class Bucket {
    private double count;
    private double start;
    private double end;
    private Map<Enum<?>, Double> stats;

    public Bucket(double start, double end, double count,
        Map<Enum<?>, Double> stats) {
      this.count = count;
      this.start = start;
      this.end = end;
      this.stats = stats;
    }

    public double getCount() {
      return count;
    }

    public double getStart() {
      return start;
    }

    public double getEnd() {
      return end;
    }

    public double getStat(HFileStat stat) {
      return this.stats.get(stat);
    }

    public static class Builder {
      private double startRow;
      private double endRow;
      private double numRows;
      private Map<Enum<?>, Double> hfileStats;

      public Builder() {
        this.hfileStats = new HashMap<Enum<?>, Double>();
      }

      public Builder setStartRow(double startRow) {
        this.startRow = startRow;
        return this;
      }

      public Builder setEndRow(double endRow) {
        this.endRow = endRow;
        return this;
      }

      public Builder setNumRows(double numRows) {
        this.numRows = numRows;
        return this;
      }

      public Builder addHFileStat(HFileStat stat, Double count) {
        this.hfileStats.put(stat, count);
        return this;
      }

      public Bucket create() {
        return new Bucket(this.startRow, this.endRow, this.numRows,
            this.hfileStats);
      }
    }

    public String print() {
      StringBuilder sb = new StringBuilder();
      sb.append("Bucket : ");
      sb.append(" start: ");
      sb.append(this.start);
      sb.append(" end: ");
      sb.append(this.end);
      sb.append(" count: ");
      sb.append(this.count);
      return sb.toString();
    }
  }

  /**
   * Adds a double value into the histogram.
   * @param dataPoint
   */
  public void add(double dataPoint);

  /**
   * Returns the list of buckets which represent the equi-depth histogram.
   * @return
   */
  public List<Bucket> getUniformBuckets();

  /**
   * Clears the state and allocates the histogram.
   * @param num_bins
   */
  public void allocate(int num_bins);

  public int getBinCount();

  public NumericHistogram merge(NumericHistogram hist);
}

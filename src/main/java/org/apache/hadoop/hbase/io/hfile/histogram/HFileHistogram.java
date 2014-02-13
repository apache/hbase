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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Captures histogram of statistics about the distribution of rows in the HFile.
 * This needs to be serialized onto the HFile.
 */
public interface HFileHistogram {
  /**
   * This enum provides the set of additional stats the Histogram can store.
   * (TODO) manukranthk : Integrate HFileStats.
   */
  public static enum HFileStat {
    KEYVALUECOUNT
  }

  public final String HFILEHISTOGRAM_BINCOUNT = "hfile.histogram.bin.count";
  public final int DEFAULT_HFILEHISTOGRAM_BINCOUNT = 100;

  public static class Bucket implements Writable {
    private byte[] startRow;
    private byte[] endRow;
    private double numRows;
    private Map<HFileStat, Double> hfileStats;

    private Bucket(byte[] startRow, byte[] endRow, double numRows,
        Map<HFileStat, Double> hfileStats) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.numRows = numRows;
      this.hfileStats = hfileStats;
    }

    public Bucket() {
    }

    public double getCount() {
      return numRows;
    }

    /**
     * Returns the number of key values that this bucket holds.
     *
     * @return
     */
    public double getNumKvs() {
      return this.hfileStats.get(HFileStat.KEYVALUECOUNT);
    }

    /**
     * @return returns a copy of the endRow
     */
    public byte[] getEndRow() {
      return Bytes.copyOfByteArray(this.endRow);
    }

    /**
     * @return returns a copy of last
     */
    public byte[] getStartRow() {
      return Bytes.copyOfByteArray(this.startRow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.startRow = Bytes.readByteArray(in);
      this.endRow = Bytes.readByteArray(in);
      this.numRows = in.readDouble();
      int numStats = in.readInt();
      this.hfileStats = new TreeMap<HFileStat, Double>();
      for (int i = 0; i < numStats; i++) {
        String ordinal = Bytes.toString(Bytes.readByteArray(in));
        double val = in.readDouble();
        hfileStats.put(HFileStat.valueOf(ordinal), val);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, this.startRow);
      Bytes.writeByteArray(out, this.endRow);
      out.writeDouble(numRows);
      out.writeInt(this.hfileStats.size());
      for (Entry<HFileStat, Double> entry : hfileStats.entrySet()) {
        Bytes.writeByteArray(out, Bytes.toBytes(entry.getKey().name()));
        out.writeDouble(entry.getValue());
      }
    }

    public String print() {
      StringBuilder sb = new StringBuilder(3 * this.startRow.length);
      sb.append("Bucket : ");
      sb.append(" , startRow : ");
      sb.append(Bytes.toStringBinary(this.startRow));
      sb.append(" , endRow : ");
      sb.append(Bytes.toStringBinary(this.endRow));
      sb.append(" , count : ");
      sb.append(this.numRows);
      return sb.toString();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(endRow);
      long temp;
      temp = Double.doubleToLongBits(numRows);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      result = prime * result + Arrays.hashCode(startRow);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Bucket other = (Bucket) obj;
      if (!Arrays.equals(endRow, other.endRow))
        return false;
      if (Double.doubleToLongBits(numRows) != Double
          .doubleToLongBits(other.numRows))
        return false;
      if (!Arrays.equals(startRow, other.startRow))
        return false;
      return true;
    }

    public static class Builder {
      private byte[] startRow;
      private byte[] endRow;
      private double numRows;
      private Map<HFileStat, Double> hfileStats;

      public Builder() {
        this.hfileStats = new HashMap<HFileStat, Double>();
      }

      /**
       * Create a builder from an existing Bucket structure.
       * @param b : Given Bucket.
       */
      public Builder(Bucket b) {
        this.hfileStats = new HashMap<HFileStat, Double>();
        for (Entry<HFileStat, Double> entry : b.hfileStats.entrySet()) {
          this.hfileStats.put(entry.getKey(), entry.getValue());
        }
        setStartRow(b.startRow);
        setEndRow(b.endRow);
        setNumRows(b.numRows);
      }

      public Builder setStartRow(byte[] startRow) {
        this.startRow = Bytes.copyOfByteArray(startRow);
        return this;
      }

      public Builder setEndRow(byte[] endRow) {
        this.endRow = Bytes.copyOfByteArray(endRow);
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
  }

  /**
   * Adds a row to the Histogram.
   *
   * @param kv
   */
  public void add(KeyValue kv);

  /**
   * Gets the set of Buckets from the Histogram. The buckets will be a
   * representation of the Equi-Depth histogram stored inside the Region.
   *
   * @return
   */
  public List<Bucket> getUniformBuckets();

  /**
   * Serializes the Histogram to he written onto the HFile and stored in the
   * meta block.
   *
   * @return
   */
  public Writable serialize();

  /**
   * Composes a list of HFileHistograms and returns a HFileHistogram which is a
   * merge of all the given Histograms. Assumes that the HFileHistogram objects
   * in the list are of the same type as this object.
   *
   * @param histograms
   * @return
   */
  public HFileHistogram compose(List<HFileHistogram> histograms);

  /**
   * Method to deserialize the histogram from the HFile. This is the inverse of
   * the serialize function.
   *
   * @param buf
   * @return
   * @throws IOException
   */
  public HFileHistogram deserialize(ByteBuffer buf) throws IOException;

  public HFileHistogram merge(HFileHistogram h2);

  public int getBinCount();
}

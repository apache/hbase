/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.Collections;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of
 * instantiation.  Rather than specifying a single row, an optional startRow
 * and stopRow may be defined.  If rows are not specified, the Scanner will
 * iterate over all rows.
 * <p>
 * To scan everything for each row, instantiate a Scan object.
 * <p>
 * To modify scanner caching for just this scan, use {@link #setCaching(int) setCaching}.
 * <p>
 * To further define the scope of what to get when scanning, perform additional 
 * methods as outlined below.
 * <p>
 * To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
 * for each family to retrieve.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
 * for each column to retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps,
 * execute {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, execute
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To add a filter, execute {@link #setFilter(org.apache.hadoop.hbase.filter.Filter) setFilter}.
 * <p>
 * Expert: To explicitly disable server-side block caching for this scan, 
 * execute {@link #setCacheBlocks(boolean)}.
 */
public class Scan implements Writable {
  // An empty navigable set to be used when adding a whole family
  private static final NavigableSet<byte[]> EMPTY_NAVIGABLE_SET
    = new TreeSet<byte[]>();
  // Version -1 first scan version. negative number used to distnguish
  // from previous version which would use this value to envode the start-key
  // length
  private static final byte SCAN_VERSION = (byte) -1;


  private byte [] startRow = HConstants.EMPTY_START_ROW;
  private byte [] stopRow  = HConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private int caching = -1;
  private boolean cacheBlocks = true;
  private Filter filter = null;
  private RowFilterInterface oldFilter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], NavigableSet<byte []>> familyMap =
    new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
  // additional data for the scan
  protected Map<ImmutableBytesWritable, ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();
  
  /**
   * Create a Scan operation across all rows.
   */
  public Scan() {}

  public Scan(byte [] startRow, Filter filter) {
    this(startRow);
    this.filter = filter;
  }
  
  /**
   * Create a Scan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the
   * next closest row after the specified row.
   * @param startRow row to start scanner at or after
   */
  public Scan(byte [] startRow) {
    this.startRow = startRow;
  }
  
  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public Scan(byte [] startRow, byte [] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }
  
  /**
   * Creates a new instance of this class while copying all values.
   * 
   * @param scan  The scan instance to copy from.
   * @throws IOException When copying the values fails.
   */
  public Scan(Scan scan) throws IOException {
    startRow = scan.getStartRow();
    stopRow  = scan.getStopRow();
    maxVersions = scan.getMaxVersions();
    caching = scan.getCaching();
    cacheBlocks = scan.getCacheBlocks();
    filter = scan.getFilter(); // clone?
    oldFilter = scan.getOldFilter(); // clone?
    TimeRange ctr = scan.getTimeRange();
    tr = new TimeRange(ctr.getMin(), ctr.getMax());
    Map<byte[], NavigableSet<byte[]>> fams = scan.getFamilyMap();
    for (byte[] fam : fams.keySet()) {
      NavigableSet<byte[]> cols = fams.get(fam);
      if (cols != null && cols.size() > 0) {
        for (byte[] col : cols) {
          addColumn(fam, col);
        }
      } else {
        addFamily(fam);
      }
    }
  }

  /**
   * Builds a scan object with the same specs as get.
   * @param get get to model scan after
   */
  public Scan(Get get) {
    this.startRow = get.getRow();
    this.stopRow = get.getRow();
    this.filter = get.getFilter();
    this.maxVersions = get.getMaxVersions();
    this.tr = get.getTimeRange();
    this.familyMap = get.getFamilyMap();
  }

  public boolean isGetScan() {
    return this.startRow != null && this.startRow.length > 0 &&
      Bytes.equals(this.startRow, this.stopRow);
  }

  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   * @return this
   */
  public Scan addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, EMPTY_NAVIGABLE_SET);
    return this;
  }
  
  /**
   * Get the column from the specified family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   * @return this
   */
  public Scan addColumn(byte [] family, byte [] qualifier) {
    NavigableSet<byte []> set = familyMap.get(family);
    if(set == null || set == EMPTY_NAVIGABLE_SET) {
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    }
    set.add(qualifier);
    familyMap.put(family, set);

    return this;
  }

  /**
   * Parses a combined family and qualifier and adds either both or just the 
   * family in case there is not qualifier. This assumes the older colon 
   * divided notation, e.g. "data:contents" or "meta:".
   * <p>
   * Note: It will through an error when the colon is missing.
   * 
   * @param familyAndQualifier
   * @return A reference to this instance.
   * @throws IllegalArgumentException When the colon is missing.
   */
  public Scan addColumn(byte[] familyAndQualifier) {
    byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
    if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
      addColumn(fq[0], fq[1]);  
    } else {
      addFamily(fq[0]);
    }
    return this;
  }
  
  /**
   * Adds an array of columns specified using old format, family:qualifier.
   * <p>
   * Overrides previous calls to addFamily for any families in the input.
   * 
   * @param columns array of columns, formatted as <pre>family:qualifier</pre>
   */
  public Scan addColumns(byte [][] columns) {
    for (int i = 0; i < columns.length; i++) {
      addColumn(columns[i]);
    }
    return this;
  }

  /**
   * Convenience method to help parse old style (or rather user entry on the
   * command line) column definitions, e.g. "data:contents mime:". The columns
   * must be space delimited and always have a colon (":") to denote family
   * and qualifier.
   * 
   * @param columns  The columns to parse.
   * @return A reference to this instance.
   */
  public Scan addColumns(String columns) {
    String[] cols = columns.split(" ");
    for (String col : cols) {
      addColumn(Bytes.toBytes(col));
    }
    return this;
  }

  /**
   * Helps to convert the binary column families and qualifiers to a text 
   * representation, e.g. "data:mimetype data:contents meta:". Binary values
   * are properly encoded using {@link Bytes#toBytesBinary(String)}.
   * 
   * @return The columns in an old style string format.
   */
  public String getInputColumns() {
    StringBuilder cols = new StringBuilder();
    for (Map.Entry<byte[], NavigableSet<byte[]>> e : 
      familyMap.entrySet()) {
      byte[] fam = e.getKey();
      if (cols.length() > 0) {
        cols.append(" ");
      }
      NavigableSet<byte[]> quals = e.getValue();
      // check if this family has qualifiers
      if (quals != null && quals.size() > 0) {
        for (byte[] qual : quals) {
          if (cols.length() > 0) {
            cols.append(" ");
          }
          // encode values to make parsing easier later
          cols.append(Bytes.toStringBinary(fam));
          cols.append(":");
          cols.append(Bytes.toStringBinary(qual));
        }
      } else {
        // only add the family but with old style delimiter 
        cols.append(Bytes.toStringBinary(fam));
        cols.append(":");
      }
    }
    return cols.toString();
  }
  
  /**
   * Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).  Note, default maximum versions to return is 1.  If
   * your time range spans more than one version and you want all versions
   * returned, up the number of versions beyond the defaut.
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   */
  public Scan setTimeRange(long minStamp, long maxStamp)
  throws IOException {
    tr = new TimeRange(minStamp, maxStamp);
    return this;
  }
  
  /**
   * Get versions of columns with the specified timestamp. Note, default maximum
   * versions to return is 1.  If your time range spans more than one version
   * and you want all versions returned, up the number of versions beyond the
   * defaut.
   * @param timestamp version timestamp
   * @see #setMaxVersions()
   * @see #setMaxVersions(int)
   */
  public Scan setTimeStamp(long timestamp) {
    try {
      tr = new TimeRange(timestamp, timestamp+1);
    } catch(IOException e) {
      // Will never happen
    }
    return this;
  }

  /**
   * Set the start row.
   * @param startRow
   */
  public Scan setStartRow(byte [] startRow) {
    this.startRow = startRow;
    return this;
  }
  
  /**
   * Set the stop row.
   * @param stopRow
   */
  public Scan setStopRow(byte [] stopRow) {
    this.stopRow = stopRow;
    return this;
  }
  
  /**
   * Get all available versions.
   */
  public Scan setMaxVersions() {
    this.maxVersions = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   */
  public Scan setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  /**
   * Set the number of rows for caching that will be passed to scanners.
   * If not set, the default setting from {@link HTable#getScannerCaching()} will apply.
   * Higher caching values will enable faster scanners but will use more memory.
   * @param caching the number of rows for caching
   */
  public void setCaching(int caching) {
    this.caching = caching;
  }

  /**
   * Apply the specified server-side filter when performing the Scan.
   * @param filter filter to run on the server
   */
  public Scan setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * Set an old-style filter interface to use. Note: not all features of the
   * old style filters are supported.
   * 
   * @deprecated
   * @param filter
   * @return The scan instance. 
   */
  public Scan setOldFilter(RowFilterInterface filter) {
    oldFilter = filter;
    return this;
  }
  
  /**
   * Setting the familyMap
   * @param familyMap
   */
  public Scan setFamilyMap(Map<byte [], NavigableSet<byte []>> familyMap) {
    this.familyMap = familyMap;
    return this;
  }
  
  /**
   * Getting the familyMap
   * @return familyMap
   */
  public Map<byte [], NavigableSet<byte []>> getFamilyMap() {
    return this.familyMap;
  }
  
  /**
   * @return the number of families in familyMap
   */
  public int numFamilies() {
    if(hasFamilies()) {
      return this.familyMap.size();
    }
    return 0;
  }

  /**
   * @return true if familyMap is non empty, false otherwise
   */
  public boolean hasFamilies() {
    return !this.familyMap.isEmpty();
  }
  
  /**
   * @return the keys of the familyMap
   */
  public byte[][] getFamilies() {
    if(hasFamilies()) {
      return this.familyMap.keySet().toArray(new byte[0][0]);
    }
    return null;
  }
  
  /**
   * @return the startrow
   */
  public byte [] getStartRow() {
    return this.startRow;
  }

  /**
   * @return the stoprow
   */
  public byte [] getStopRow() {
    return this.stopRow;
  }
  
  /**
   * @return the max number of versions to fetch
   */
  public int getMaxVersions() {
    return this.maxVersions;
  } 

  /**
   * @return caching the number of rows fetched when calling next on a scanner
   */
  public int getCaching() {
    return this.caching;
  } 

  /**
   * @return TimeRange
   */
  public TimeRange getTimeRange() {
    return this.tr;
  } 
  
  /**
   * @return RowFilter
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Get the old style filter, if there is one.
   * @deprecated
   * @return null or instance
   */
  public RowFilterInterface getOldFilter() {
    return oldFilter;
  }
  
  /**
   * @return true is a filter has been specified, false if not
   */
  public boolean hasFilter() {
    return filter != null || oldFilter != null;
  }
  
  /**
   * Set whether blocks should be cached for this Scan.
   * <p>
   * This is true by default.  When true, default settings of the table and
   * family are used (this will never override caching blocks if the block
   * cache is disabled for that family or entirely).
   * 
   * @param cacheBlocks if false, default settings are overridden and blocks
   * will not be cached
   */
  public void setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }
  
  /**
   * Get whether blocks should be cached for this Scan.
   * @return true if default caching should be used, false if blocks should not
   * be cached
   */
  public boolean getCacheBlocks() {
    return cacheBlocks;
  }

  /**
   * @param key The key.
   * @return The value.
   */
  public byte[] getValue(byte[] key) {
    return getValue(new ImmutableBytesWritable(key));
  }

  private byte[] getValue(final ImmutableBytesWritable key) {
    ImmutableBytesWritable ibw = values.get(key);
    if (ibw == null)
      return null;
    return ibw.get();
  }

  /**
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * @return All values.
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
     return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(byte[] key, byte[] value) {
    setValue(new ImmutableBytesWritable(key), value);
  }

  /*
   * @param key The key.
   * @param value The value.
   */
  private void setValue(final ImmutableBytesWritable key,
      final byte[] value) {
    values.put(key, new ImmutableBytesWritable(value));
  }

  /*
   * @param key The key.
   * @param value The value.
   */
  private void setValue(final ImmutableBytesWritable key,
      final ImmutableBytesWritable value) {
    values.put(key, value);
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(String key, String value) {
    setValue(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  /**
   * @param key Key whose key and value we're to remove from HTD parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new ImmutableBytesWritable(key));
  }
  
  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("startRow=");
    sb.append(Bytes.toString(this.startRow));
    sb.append(", stopRow=");
    sb.append(Bytes.toString(this.stopRow));
    sb.append(", maxVersions=");
    sb.append("" + this.maxVersions);
    sb.append(", caching=");
    sb.append("" + this.caching);
    sb.append(", cacheBlocks=");
    sb.append("" + this.cacheBlocks);
    sb.append(", timeRange=");
    sb.append("[" + this.tr.getMin() + "," + this.tr.getMax() + ")");
    sb.append(", families=");
    if(this.familyMap.size() == 0) {
      sb.append("ALL");
      return sb.toString();
    }
    boolean moreThanOne = false;
    for(Map.Entry<byte [], NavigableSet<byte[]>> entry : this.familyMap.entrySet()) {
      if(moreThanOne) {
        sb.append("), ");
      } else {
        moreThanOne = true;
        sb.append("{");
      }
      sb.append("(family=");
      sb.append(Bytes.toString(entry.getKey()));
      sb.append(", columns=");
      if(entry.getValue() == null) {
        sb.append("ALL");
      } else {
        sb.append("{");
        boolean moreThanOneB = false;
        for(byte [] column : entry.getValue()) {
          if(moreThanOneB) {
            sb.append(", ");
          } else {
            moreThanOneB = true;
          }
          sb.append(Bytes.toString(column));
        }
        sb.append("}");
      }
    }
    sb.append("}");
    
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      String key = Bytes.toString(e.getKey().get());
      String value = Bytes.toString(e.getValue().get());
      if (key == null) {
        continue;
      }
      sb.append(", ");
      sb.append(key);
      sb.append(" => '");
      sb.append(value);
      sb.append("'");
    }

    return sb.toString();
  }
  
  @SuppressWarnings("unchecked")
  private Writable createForName(String className) {
    try {
      Class<? extends Writable> clazz =
        (Class<? extends Writable>) Class.forName(className);
      return WritableFactories.newInstance(clazz, new Configuration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Can't find class " + className);
    }    
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    byte versionOrLength = in.readByte();
    if (versionOrLength == SCAN_VERSION) {
      this.startRow = Bytes.readByteArray(in);
    } else {
      int length = (int) Writables.readVLong(in, versionOrLength);
      this.startRow = Bytes.readByteArray(in, length);
    }
    this.stopRow = Bytes.readByteArray(in);
    this.maxVersions = in.readInt();
    this.caching = in.readInt();
    this.cacheBlocks = in.readBoolean();
    if(in.readBoolean()) {
      this.filter = (Filter)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.filter.readFields(in);
    }
    if (in.readBoolean()) {
      this.oldFilter =
        (RowFilterInterface)createForName(Bytes.toString(Bytes.readByteArray(in)));
      this.oldFilter.readFields(in);
    }
    this.tr = new TimeRange();
    tr.readFields(in);
    int numFamilies = in.readInt();
    this.familyMap = 
      new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      TreeSet<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      for(int j=0; j<numColumns; j++) {
        byte [] qualifier = Bytes.readByteArray(in);
        set.add(qualifier);
      }
      this.familyMap.put(family, set);
    }
    this.values.clear();
    if (versionOrLength == SCAN_VERSION) {
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        key.readFields(in);
        value.readFields(in);
        values.put(key, value);
      }
    }
  }

  public void write(final DataOutput out)
  throws IOException {
    out.writeByte(SCAN_VERSION);
    Bytes.writeByteArray(out, this.startRow);
    Bytes.writeByteArray(out, this.stopRow);
    out.writeInt(this.maxVersions);
    out.writeInt(this.caching);
    out.writeBoolean(this.cacheBlocks);
    if(this.filter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, Bytes.toBytes(filter.getClass().getName()));
      filter.write(out);
    }
    if (this.oldFilter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, Bytes.toBytes(oldFilter.getClass().getName()));
      oldFilter.write(out);
    }
    tr.write(out);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], NavigableSet<byte []>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte []> columnSet = entry.getValue();
      if(columnSet != null){
        out.writeInt(columnSet.size());
        for(byte [] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      } else {
        out.writeInt(0);
      }
    }
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
  }
}

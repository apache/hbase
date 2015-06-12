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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;

public class SyncTable extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(SyncTable.class);

  static final String SOURCE_HASH_DIR_CONF_KEY = "sync.table.source.hash.dir";
  static final String SOURCE_TABLE_CONF_KEY = "sync.table.source.table.name";
  static final String TARGET_TABLE_CONF_KEY = "sync.table.target.table.name";
  static final String SOURCE_ZK_CLUSTER_CONF_KEY = "sync.table.source.zk.cluster";
  static final String TARGET_ZK_CLUSTER_CONF_KEY = "sync.table.target.zk.cluster";
  static final String DRY_RUN_CONF_KEY="sync.table.dry.run";
  
  Path sourceHashDir;
  String sourceTableName;
  String targetTableName;
  
  String sourceZkCluster;
  String targetZkCluster;
  boolean dryRun;
  
  Counters counters;
  
  public SyncTable(Configuration conf) {
    super(conf);
  }
  
  public Job createSubmittableJob(String[] args) throws IOException {
    FileSystem fs = sourceHashDir.getFileSystem(getConf());
    if (!fs.exists(sourceHashDir)) {
      throw new IOException("Source hash dir not found: " + sourceHashDir);
    }
    
    HashTable.TableHash tableHash = HashTable.TableHash.read(getConf(), sourceHashDir);
    LOG.info("Read source hash manifest: " + tableHash);
    LOG.info("Read " + tableHash.partitions.size() + " partition keys");
    if (!tableHash.tableName.equals(sourceTableName)) {
      LOG.warn("Table name mismatch - manifest indicates hash was taken from: "
          + tableHash.tableName + " but job is reading from: " + sourceTableName);
    }
    if (tableHash.numHashFiles != tableHash.partitions.size() + 1) {
      throw new RuntimeException("Hash data appears corrupt. The number of of hash files created"
          + " should be 1 more than the number of partition keys.  However, the manifest file "
          + " says numHashFiles=" + tableHash.numHashFiles + " but the number of partition keys"
          + " found in the partitions file is " + tableHash.partitions.size());
    }
    
    Path dataDir = new Path(sourceHashDir, HashTable.HASH_DATA_DIR);
    int dataSubdirCount = 0;
    for (FileStatus file : fs.listStatus(dataDir)) {
      if (file.getPath().getName().startsWith(HashTable.OUTPUT_DATA_FILE_PREFIX)) {
        dataSubdirCount++;
      }
    }
    
    if (dataSubdirCount != tableHash.numHashFiles) {
      throw new RuntimeException("Hash data appears corrupt. The number of of hash files created"
          + " should be 1 more than the number of partition keys.  However, the number of data dirs"
          + " found is " + dataSubdirCount + " but the number of partition keys"
          + " found in the partitions file is " + tableHash.partitions.size());
    }
    
    Job job = Job.getInstance(getConf(),getConf().get("mapreduce.job.name",
        "syncTable_" + sourceTableName + "-" + targetTableName));
    Configuration jobConf = job.getConfiguration();
    job.setJarByClass(HashTable.class);
    jobConf.set(SOURCE_HASH_DIR_CONF_KEY, sourceHashDir.toString());
    jobConf.set(SOURCE_TABLE_CONF_KEY, sourceTableName);
    jobConf.set(TARGET_TABLE_CONF_KEY, targetTableName);
    if (sourceZkCluster != null) {
      jobConf.set(SOURCE_ZK_CLUSTER_CONF_KEY, sourceZkCluster);
    }
    if (targetZkCluster != null) {
      jobConf.set(TARGET_ZK_CLUSTER_CONF_KEY, targetZkCluster);
    }
    jobConf.setBoolean(DRY_RUN_CONF_KEY, dryRun);
    
    TableMapReduceUtil.initTableMapperJob(targetTableName, tableHash.initScan(),
        SyncMapper.class, null, null, job);
    
    job.setNumReduceTasks(0);
     
    if (dryRun) {
      job.setOutputFormatClass(NullOutputFormat.class);
    } else {
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // because it sets up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(targetTableName, null, job, null,
          targetZkCluster, null, null);
      
      // would be nice to add an option for bulk load instead
    }
    
    return job;
  }
  
  public static class SyncMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
    Path sourceHashDir;
    
    Connection sourceConnection;
    Connection targetConnection;
    Table sourceTable;
    Table targetTable;
    boolean dryRun;
    
    HashTable.TableHash sourceTableHash;
    HashTable.TableHash.Reader sourceHashReader;
    ImmutableBytesWritable currentSourceHash;
    ImmutableBytesWritable nextSourceKey;
    HashTable.ResultHasher targetHasher;
    
    Throwable mapperException;
     
    public static enum Counter {BATCHES, HASHES_MATCHED, HASHES_NOT_MATCHED, SOURCEMISSINGROWS,
      SOURCEMISSINGCELLS, TARGETMISSINGROWS, TARGETMISSINGCELLS, ROWSWITHDIFFS, DIFFERENTCELLVALUES,
      MATCHINGROWS, MATCHINGCELLS, EMPTY_BATCHES, RANGESMATCHED, RANGESNOTMATCHED};
    
    @Override
    protected void setup(Context context) throws IOException {
      
      Configuration conf = context.getConfiguration();
      sourceHashDir = new Path(conf.get(SOURCE_HASH_DIR_CONF_KEY));
      sourceConnection = openConnection(conf, SOURCE_ZK_CLUSTER_CONF_KEY);
      targetConnection = openConnection(conf, TARGET_ZK_CLUSTER_CONF_KEY);
      sourceTable = openTable(sourceConnection, conf, SOURCE_TABLE_CONF_KEY);
      targetTable = openTable(targetConnection, conf, TARGET_TABLE_CONF_KEY);
      dryRun = conf.getBoolean(SOURCE_TABLE_CONF_KEY, false);
      
      sourceTableHash = HashTable.TableHash.read(conf, sourceHashDir);
      LOG.info("Read source hash manifest: " + sourceTableHash);
      LOG.info("Read " + sourceTableHash.partitions.size() + " partition keys");
      
      TableSplit split = (TableSplit) context.getInputSplit();
      ImmutableBytesWritable splitStartKey = new ImmutableBytesWritable(split.getStartRow());
      
      sourceHashReader = sourceTableHash.newReader(conf, splitStartKey);
      findNextKeyHashPair();
      
      // create a hasher, but don't start it right away
      // instead, find the first hash batch at or after the start row
      // and skip any rows that come before.  they will be caught by the previous task
      targetHasher = new HashTable.ResultHasher();
    }
  
    private static Connection openConnection(Configuration conf, String zkClusterConfKey)
      throws IOException {
        Configuration clusterConf = new Configuration(conf);
        String zkCluster = conf.get(zkClusterConfKey);
        if (zkCluster != null) {
          ZKUtil.applyClusterKeyToConf(clusterConf, zkCluster);
        }
        return ConnectionFactory.createConnection(clusterConf);
    }
    
    private static Table openTable(Connection connection, Configuration conf,
        String tableNameConfKey) throws IOException {
      return connection.getTable(TableName.valueOf(conf.get(tableNameConfKey)));
    }
    
    /**
     * Attempt to read the next source key/hash pair.
     * If there are no more, set nextSourceKey to null
     */
    private void findNextKeyHashPair() throws IOException {
      boolean hasNext = sourceHashReader.next();
      if (hasNext) {
        nextSourceKey = sourceHashReader.getCurrentKey();
      } else {
        // no more keys - last hash goes to the end
        nextSourceKey = null;
      }
    }
    
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      try {
        // first, finish any hash batches that end before the scanned row
        while (nextSourceKey != null && key.compareTo(nextSourceKey) >= 0) {
          moveToNextBatch(context);
        }
        
        // next, add the scanned row (as long as we've reached the first batch)
        if (targetHasher.isBatchStarted()) {
          targetHasher.hashResult(value);
        }
      } catch (Throwable t) {
        mapperException = t;
        Throwables.propagateIfInstanceOf(t, IOException.class);
        Throwables.propagateIfInstanceOf(t, InterruptedException.class);
        Throwables.propagate(t);
      }
    }

    /**
     * If there is an open hash batch, complete it and sync if there are diffs.
     * Start a new batch, and seek to read the 
     */
    private void moveToNextBatch(Context context) throws IOException, InterruptedException {
      if (targetHasher.isBatchStarted()) {
        finishBatchAndCompareHashes(context);
      }
      targetHasher.startBatch(nextSourceKey);
      currentSourceHash = sourceHashReader.getCurrentHash();
      
      findNextKeyHashPair();
    }

    /**
     * Finish the currently open hash batch.
     * Compare the target hash to the given source hash.
     * If they do not match, then sync the covered key range.
     */
    private void finishBatchAndCompareHashes(Context context)
        throws IOException, InterruptedException {
      targetHasher.finishBatch();
      context.getCounter(Counter.BATCHES).increment(1);
      if (targetHasher.getBatchSize() == 0) {
        context.getCounter(Counter.EMPTY_BATCHES).increment(1);
      }
      ImmutableBytesWritable targetHash = targetHasher.getBatchHash();
      if (targetHash.equals(currentSourceHash)) {
        context.getCounter(Counter.HASHES_MATCHED).increment(1);
      } else {
        context.getCounter(Counter.HASHES_NOT_MATCHED).increment(1);
        
        ImmutableBytesWritable stopRow = nextSourceKey == null
                                          ? new ImmutableBytesWritable(sourceTableHash.stopRow)
                                          : nextSourceKey;
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("Hash mismatch.  Key range: " + toHex(targetHasher.getBatchStartKey())
              + " to " + toHex(stopRow)
              + " sourceHash: " + toHex(currentSourceHash)
              + " targetHash: " + toHex(targetHash));
        }
        
        syncRange(context, targetHasher.getBatchStartKey(), stopRow);
      }
    }
    private static String toHex(ImmutableBytesWritable bytes) {
      return Bytes.toHex(bytes.get(), bytes.getOffset(), bytes.getLength());
    }
    
    private static final CellScanner EMPTY_CELL_SCANNER
      = new CellScanner(Iterators.<Result>emptyIterator());
    
    /**
     * Rescan the given range directly from the source and target tables.
     * Count and log differences, and if this is not a dry run, output Puts and Deletes
     * to make the target table match the source table for this range
     */
    private void syncRange(Context context, ImmutableBytesWritable startRow,
        ImmutableBytesWritable stopRow) throws IOException, InterruptedException {
      
      Scan scan = sourceTableHash.initScan();
      scan.setStartRow(startRow.copyBytes());
      scan.setStopRow(stopRow.copyBytes());
      
      ResultScanner sourceScanner = sourceTable.getScanner(scan);
      CellScanner sourceCells = new CellScanner(sourceScanner.iterator());

      ResultScanner targetScanner = targetTable.getScanner(scan);
      CellScanner targetCells = new CellScanner(targetScanner.iterator());
      
      boolean rangeMatched = true;
      byte[] nextSourceRow = sourceCells.nextRow();
      byte[] nextTargetRow = targetCells.nextRow();
      while(nextSourceRow != null || nextTargetRow != null) {
        boolean rowMatched;
        int rowComparison = compareRowKeys(nextSourceRow, nextTargetRow);
        if (rowComparison < 0) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Target missing row: " + Bytes.toHex(nextSourceRow));
          }
          context.getCounter(Counter.TARGETMISSINGROWS).increment(1);
          
          rowMatched = syncRowCells(context, nextSourceRow, sourceCells, EMPTY_CELL_SCANNER);
          nextSourceRow = sourceCells.nextRow();  // advance only source to next row
        } else if (rowComparison > 0) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Source missing row: " + Bytes.toHex(nextTargetRow));
          }
          context.getCounter(Counter.SOURCEMISSINGROWS).increment(1);
          
          rowMatched = syncRowCells(context, nextTargetRow, EMPTY_CELL_SCANNER, targetCells);
          nextTargetRow = targetCells.nextRow();  // advance only target to next row
        } else {
          // current row is the same on both sides, compare cell by cell
          rowMatched = syncRowCells(context, nextSourceRow, sourceCells, targetCells);
          nextSourceRow = sourceCells.nextRow();  
          nextTargetRow = targetCells.nextRow();
        }
        
        if (!rowMatched) {
          rangeMatched = false;
        }
      }
      
      sourceScanner.close();
      targetScanner.close();
      
      context.getCounter(rangeMatched ? Counter.RANGESMATCHED : Counter.RANGESNOTMATCHED)
        .increment(1);
    }
    
    private static class CellScanner {
      private final Iterator<Result> results;
      
      private byte[] currentRow;
      private Result currentRowResult;
      private int nextCellInRow;
      
      private Result nextRowResult;
      
      public CellScanner(Iterator<Result> results) {
        this.results = results;
      }
      
      /**
       * Advance to the next row and return its row key.
       * Returns null iff there are no more rows.
       */
      public byte[] nextRow() {
        if (nextRowResult == null) {
          // no cached row - check scanner for more
          while (results.hasNext()) {
            nextRowResult = results.next();
            Cell nextCell = nextRowResult.rawCells()[0];
            if (currentRow == null
                || !Bytes.equals(currentRow, 0, currentRow.length, nextCell.getRowArray(),
                nextCell.getRowOffset(), nextCell.getRowLength())) {
              // found next row
              break;
            } else {
              // found another result from current row, keep scanning
              nextRowResult = null;
            }
          }
          
          if (nextRowResult == null) {
            // end of data, no more rows
            currentRowResult = null;
            currentRow = null;
            return null;
          }
        }
        
        // advance to cached result for next row
        currentRowResult = nextRowResult;
        nextCellInRow = 0;
        currentRow = currentRowResult.getRow();
        nextRowResult = null;
        return currentRow;
      }
      
      /**
       * Returns the next Cell in the current row or null iff none remain.
       */
      public Cell nextCellInRow() {
        if (currentRowResult == null) {
          // nothing left in current row
          return null;
        }
        
        Cell nextCell = currentRowResult.rawCells()[nextCellInRow];
        nextCellInRow++;
        if (nextCellInRow == currentRowResult.size()) {
          if (results.hasNext()) {
            Result result = results.next();
            Cell cell = result.rawCells()[0];
            if (Bytes.equals(currentRow, 0, currentRow.length, cell.getRowArray(),
                cell.getRowOffset(), cell.getRowLength())) {
              // result is part of current row
              currentRowResult = result;
              nextCellInRow = 0;
            } else {
              // result is part of next row, cache it
              nextRowResult = result;
              // current row is complete
              currentRowResult = null;
            }
          } else {
            // end of data
            currentRowResult = null;
          }
        }
        return nextCell;
      }
    }
       
    /**
     * Compare the cells for the given row from the source and target tables.
     * Count and log any differences.
     * If not a dry run, output a Put and/or Delete needed to sync the target table
     * to match the source table.
     */
    private boolean syncRowCells(Context context, byte[] rowKey, CellScanner sourceCells,
        CellScanner targetCells) throws IOException, InterruptedException {
      Put put = null;
      Delete delete = null;
      long matchingCells = 0;
      boolean matchingRow = true;
      Cell sourceCell = sourceCells.nextCellInRow();
      Cell targetCell = targetCells.nextCellInRow();
      while (sourceCell != null || targetCell != null) {

        int cellKeyComparison = compareCellKeysWithinRow(sourceCell, targetCell);
        if (cellKeyComparison < 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Target missing cell: " + sourceCell);
          }
          context.getCounter(Counter.TARGETMISSINGCELLS).increment(1);
          matchingRow = false;
          
          if (!dryRun) {
            if (put == null) {
              put = new Put(rowKey);
            }
            put.add(sourceCell);
          }
          
          sourceCell = sourceCells.nextCellInRow();
        } else if (cellKeyComparison > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source missing cell: " + targetCell);
          }
          context.getCounter(Counter.SOURCEMISSINGCELLS).increment(1);
          matchingRow = false;
          
          if (!dryRun) {
            if (delete == null) {
              delete = new Delete(rowKey);
            }
            // add a tombstone to exactly match the target cell that is missing on the source
            delete.addColumn(CellUtil.cloneFamily(targetCell),
                CellUtil.cloneQualifier(targetCell), targetCell.getTimestamp());
          }
          
          targetCell = targetCells.nextCellInRow();
        } else {
          // the cell keys are equal, now check values
          if (CellUtil.matchingValue(sourceCell, targetCell)) {
            matchingCells++;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Different values: ");
              LOG.debug("  source cell: " + sourceCell
                  + " value: " + Bytes.toHex(sourceCell.getValueArray(),
                      sourceCell.getValueOffset(), sourceCell.getValueLength()));
              LOG.debug("  target cell: " + targetCell
                  + " value: " + Bytes.toHex(targetCell.getValueArray(),
                      targetCell.getValueOffset(), targetCell.getValueLength()));
            }
            context.getCounter(Counter.DIFFERENTCELLVALUES).increment(1);
            matchingRow = false;
            
            if (!dryRun) {
              // overwrite target cell
              if (put == null) {
                put = new Put(rowKey);
              }
              put.add(sourceCell);
            }
          }
          sourceCell = sourceCells.nextCellInRow();
          targetCell = targetCells.nextCellInRow();
        }
        
        if (!dryRun && sourceTableHash.scanBatch > 0) {
          if (put != null && put.size() >= sourceTableHash.scanBatch) {
            context.write(new ImmutableBytesWritable(rowKey), put);
            put = null;
          }
          if (delete != null && delete.size() >= sourceTableHash.scanBatch) {
            context.write(new ImmutableBytesWritable(rowKey), delete);
            delete = null;
          }
        }
      }
      
      if (!dryRun) {
        if (put != null) {
          context.write(new ImmutableBytesWritable(rowKey), put);
        }
        if (delete != null) {
          context.write(new ImmutableBytesWritable(rowKey), delete);
        }
      }
      
      if (matchingCells > 0) {
        context.getCounter(Counter.MATCHINGCELLS).increment(matchingCells);
      }
      if (matchingRow) {
        context.getCounter(Counter.MATCHINGROWS).increment(1);
        return true;
      } else {
        context.getCounter(Counter.ROWSWITHDIFFS).increment(1);
        return false;
      }
    }

    private static final CellComparator cellComparator = new CellComparator();
    /**
     * Compare row keys of the given Result objects.
     * Nulls are after non-nulls
     */
    private static int compareRowKeys(byte[] r1, byte[] r2) {
      if (r1 == null) {
        return 1;  // source missing row
      } else if (r2 == null) {
        return -1; // target missing row
      } else {
        return cellComparator.compareRows(r1, 0, r1.length, r2, 0, r2.length);
      }
    }

    /**
     * Compare families, qualifiers, and timestamps of the given Cells.
     * They are assumed to be of the same row.
     * Nulls are after non-nulls.
     */
     private static int compareCellKeysWithinRow(Cell c1, Cell c2) {
      if (c1 == null) {
        return 1; // source missing cell
      }
      if (c2 == null) {
        return -1; // target missing cell
      }
      
      int result = CellComparator.compareFamilies(c1, c2);
      if (result != 0) {
        return result;
      }
      
      result = CellComparator.compareQualifiers(c1, c2);
      if (result != 0) {
        return result;
      }
      
      // note timestamp comparison is inverted - more recent cells first
      return CellComparator.compareTimestamps(c1, c2);
    }
     
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      if (mapperException == null) {
        try {
          finishRemainingHashRanges(context);
        } catch (Throwable t) {
          mapperException = t;
        }
      }
      
      try {
        sourceTable.close();
        targetTable.close();
        sourceConnection.close();
        targetConnection.close();
      } catch (Throwable t) {
        if (mapperException == null) {
          mapperException = t;
        } else {
          LOG.error("Suppressing exception from closing tables", t);
        }
      }
      
      // propagate first exception
      if (mapperException != null) {
        Throwables.propagateIfInstanceOf(mapperException, IOException.class);
        Throwables.propagateIfInstanceOf(mapperException, InterruptedException.class);
        Throwables.propagate(mapperException);
      }
    }

    private void finishRemainingHashRanges(Context context) throws IOException,
        InterruptedException {
      TableSplit split = (TableSplit) context.getInputSplit();
      byte[] splitEndRow = split.getEndRow();
      boolean reachedEndOfTable = HashTable.isTableEndRow(splitEndRow);

      // if there are more hash batches that begin before the end of this split move to them
      while (nextSourceKey != null
          && (nextSourceKey.compareTo(splitEndRow) < 0 || reachedEndOfTable)) {
        moveToNextBatch(context);
      }
      
      if (targetHasher.isBatchStarted()) {
        // need to complete the final open hash batch

        if ((nextSourceKey != null && nextSourceKey.compareTo(splitEndRow) > 0)
              || (nextSourceKey == null && !Bytes.equals(splitEndRow, sourceTableHash.stopRow))) {
          // the open hash range continues past the end of this region
          // add a scan to complete the current hash range
          Scan scan = sourceTableHash.initScan();
          scan.setStartRow(splitEndRow);
          if (nextSourceKey == null) {
            scan.setStopRow(sourceTableHash.stopRow);
          } else {
            scan.setStopRow(nextSourceKey.copyBytes());
          }
          
          ResultScanner targetScanner = targetTable.getScanner(scan);
          for (Result row : targetScanner) {
            targetHasher.hashResult(row);          
          }
        } // else current batch ends exactly at split end row

        finishBatchAndCompareHashes(context);
      }
    }
  }
  
  private static final int NUM_ARGS = 3;
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
      System.err.println();
    }
    System.err.println("Usage: SyncTable [options] <sourcehashdir> <sourcetable> <targettable>");
    System.err.println();
    System.err.println("Options:");
    
    System.err.println(" sourcezkcluster  ZK cluster key of the source table");
    System.err.println("                  (defaults to cluster in classpath's config)");
    System.err.println(" targetzkcluster  ZK cluster key of the target table");
    System.err.println("                  (defaults to cluster in classpath's config)");
    System.err.println(" dryrun           if true, output counters but no writes");
    System.err.println("                  (defaults to false)");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" sourcehashdir    path to HashTable output dir for source table");
    System.err.println("                  if not specified, then all data will be scanned");
    System.err.println(" sourcetable      Name of the source table to sync from");
    System.err.println(" targettable      Name of the target table to sync to");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" For a dry run SyncTable of tableA from a remote source cluster");
    System.err.println(" to a local target cluster:");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.SyncTable --dryrun=true"
        + " --sourcezkcluster=zk1.example.com,zk2.example.com,zk3.example.com:2181:/hbase"
        + " hdfs://nn:9000/hashes/tableA tableA tableA");
  }
  
  private boolean doCommandLine(final String[] args) {
    if (args.length < NUM_ARGS) {
      printUsage(null);
      return false;
    }
    try {
      sourceHashDir = new Path(args[args.length - 3]);
      sourceTableName = args[args.length - 2];
      targetTableName = args[args.length - 1];
            
      for (int i = 0; i < args.length - NUM_ARGS; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }
        
        final String sourceZkClusterKey = "--sourcezkcluster=";
        if (cmd.startsWith(sourceZkClusterKey)) {
          sourceZkCluster = cmd.substring(sourceZkClusterKey.length());
          continue;
        }
        
        final String targetZkClusterKey = "--targetzkcluster=";
        if (cmd.startsWith(targetZkClusterKey)) {
          targetZkCluster = cmd.substring(targetZkClusterKey.length());
          continue;
        }
        
        final String dryRunKey = "--dryrun=";
        if (cmd.startsWith(dryRunKey)) {
          dryRun = Boolean.parseBoolean(cmd.substring(dryRunKey.length()));
          continue;
        }
        
        printUsage("Invalid argument '" + cmd + "'");
        return false;
      }

      
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }
  
  /**
   * Main entry point.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new SyncTable(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (!doCommandLine(otherArgs)) {
      return 1;
    }

    Job job = createSubmittableJob(otherArgs);
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      return 1;
    }
    counters = job.getCounters();
    return 0;
  }

}

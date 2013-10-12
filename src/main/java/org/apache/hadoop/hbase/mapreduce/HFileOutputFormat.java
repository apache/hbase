/**
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.master.RegionPlacement;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Writes HFiles. Passed KeyValues must arrive in order.
 * Currently, can only write files to a single column family at a
 * time.  Multiple column families requires coordinating keys cross family.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created hfiles.
 * @see KeyValueSortReducer
 */
public class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  static Log LOG = LogFactory.getLog(HFileOutputFormat.class);
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";
  //This stores a string in the format family1=bloomType1&family2=bloomType2&...&familyN=bloomTypeN
  static final String BLOOMFILTER_TYPE_PER_CF_KEY =
    "hbase.hfileoutputformat.families.bloomfilter.typePerCF";
  
  static final String ENCODING_TYPE_PER_CF_KEY =
    "hbase.hfileoutputformat.families.encoding.typePerCF";

  static final String TABLE_NAME = "hbase.hfileoutputformat.tablename";
  static final String UTF8 = "UTF-8";

  private static RecordWriter<ImmutableBytesWritable, KeyValue> latestWriter = null;

  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(final TaskAttemptContext context)
  throws IOException, InterruptedException {
    // Get the path of the temporary output file
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
    final Configuration conf = context.getConfiguration();
    final FileSystem fs = outputdir.getFileSystem(conf);

    // These configs. are from hbase-*.xml
    final long maxsize = conf.getLong("hbase.hregion.max.filesize",
        HConstants.DEFAULT_MAX_FILE_SIZE);
    final int blocksize = conf.getInt("hfile.min.blocksize.size",
        HFile.DEFAULT_BLOCKSIZE);
    // Invented config.  Add to hbase-*.xml if other than default compression.
    final String defaultCompression = conf.get("hfile.compression",
        Compression.Algorithm.NONE.getName());
    HTable tempTable = null;
    if (conf.get(TABLE_NAME) != null) {
      tempTable = new HTable(conf, conf.get(TABLE_NAME));
    }
    final HTable table = tempTable;

    // create a map from column family to the compression algorithm
    final Map<byte[], String> compressionMap = createFamilyCompressionMap(conf);

    final Map<byte[], BloomType> bloomTypeMap = createFamilyBloomTypeMap(conf);
    
    final Map<byte[], HFileDataBlockEncoder> encoderTypeMap = 
      createFamilyDeltaEncodingMap(conf);
    
    final Pair<byte[][], byte[][]> startKeysAndFavoredNodes = 
      (table == null ? null : table.getStartKeysAndFavoredNodes());

    RecordWriter<ImmutableBytesWritable, KeyValue> writer =
        new RecordWriter<ImmutableBytesWritable, KeyValue>() {
      // Map of families to writers and how much has been output on the writer.
      private final Map<byte [], WriterLength> writers =
        new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
      private byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
      private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
      private boolean rollRequested = false;
      private byte[] favoredNodes = null;

      public void write(ImmutableBytesWritable row, KeyValue kv)
      throws IOException {

        // null input == user explicitly wants to flush
        if (row == null && kv == null) {
          rollWriters();
          return;
        }

        byte [] rowKey = kv.getRow();
        long length = kv.getLength();
        byte [] family = kv.getFamily();
        WriterLength wl = this.writers.get(family);
        if (favoredNodes == null) favoredNodes = getFavoredNodesForKey(rowKey);

        // If this is a new column family, verify that the directory exists
        if (wl == null) {
          fs.mkdirs(new Path(outputdir, Bytes.toString(family)));
        }

        // If any of the HFiles for the column families has reached
        // maxsize, we need to roll all the writers
        if (wl != null && wl.written + length >= maxsize) {
          this.rollRequested = true;
        }

        // This can only happen once a row is finished though
        if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
          rollWriters();
        }

        // create a new HLog writer, if necessary
        if (wl == null || wl.writer == null) {
          wl = getNewWriter(family, conf);
        }

        // we now have the proper HLog writer. full steam ahead
        kv.updateLatestStamp(this.now);
        wl.writer.append(kv);
        wl.written += length;

        // Copy the row so we know when a row transition.
        this.previousRow = rowKey;
      }

      private byte[] getFavoredNodesForKey(byte[] rowKey) {
        if (startKeysAndFavoredNodes == null) {
          return HConstants.EMPTY_BYTE_ARRAY;
        }
        byte[][] startKeys = startKeysAndFavoredNodes.getFirst();
        byte[][] favoredNodes = startKeysAndFavoredNodes.getSecond();
        if (startKeys == null || favoredNodes == null)
          return HConstants.EMPTY_BYTE_ARRAY;
        ConcurrentSkipListMap<byte [], byte[]> startKeysToFavoredNodes =
          new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i=0; i<startKeys.length; i++) {
          if (startKeys[i] == null || favoredNodes[i] == null)
            return HConstants.EMPTY_BYTE_ARRAY;
          startKeysToFavoredNodes.put(startKeys[i], favoredNodes[i]);
        }
        return startKeysToFavoredNodes.floorEntry(rowKey).getValue();
      }

      private void rollWriters() throws IOException {
        for (WriterLength wl : this.writers.values()) {
          if (wl.writer != null) {
            LOG.info("Writer=" + wl.writer.getPath() +
                ((wl.written == 0)? "": ", wrote=" + wl.written));
            close(wl.writer);
          }
          wl.writer = null;
          wl.written = 0;
        }
        this.rollRequested = false;
      }

      /* Create a new HFile.Writer.
       * @param family
       * @return A WriterLength, containing a new HFile.Writer.
       * @throws IOException
       */
      private WriterLength getNewWriter(byte[] family, Configuration conf)
          throws IOException {
        WriterLength wl = new WriterLength();
        Path familydir = new Path(outputdir, Bytes.toString(family));
        String compression = compressionMap.get(family);
        compression = compression == null ? defaultCompression : compression;
        BloomType bloomType = bloomTypeMap.get(family);
        HFileDataBlockEncoder encoder = encoderTypeMap.get(family);
        String favNodes = "";
        if (favoredNodes != null) favNodes = Bytes.toString(favoredNodes);
        if (bloomType == null) {
          bloomType = BloomType.NONE;
        }
        if (encoder == null) {
          encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.NONE,
            DataBlockEncoding.NONE);
        }

        LOG.info("Using " + encoder.getEncodingInCache() + " in cache and " +
          encoder.getEncodingOnDisk() + " on disk for the column family " +
          Bytes.toString(family));

        /* new bloom filter does not require maxKeys. */
        int maxKeys = 0;
        wl.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(conf),
            fs, blocksize)
                .withOutputDir(familydir)
                .withCompression(compression)
                .withBloomType(bloomType)
                .withMaxKeyCount(maxKeys)
                .withDataBlockEncoder(encoder)
                .withFavoredNodes(RegionPlacement.getFavoredInetSocketAddress(favNodes))
                .build();
        this.writers.put(family, wl);
        return wl;
      }

      private void close(final StoreFile.Writer w) throws IOException {
        if (w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
              Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
              Bytes.toBytes(context.getTaskAttemptID().toString()));

          /* Set maxSequenceId to be 0 for bulk imported files since
           * these files do not correspond to any edit log items.
           * Set majorCompaction flag to be false for bulk import file.
           * For now, bulk load files don't have minFlushTime.
           */
          w.appendMetadata(HConstants.NO_MIN_FLUSH_TIME, 0, false);
          w.close();

        }
      }

      public void close(TaskAttemptContext c)
      throws IOException, InterruptedException {
        for (WriterLength wl: this.writers.values()) {
          close(wl.writer);
        }
      }
    };

    latestWriter = writer;
    return writer;
  }

  /*
   * Data structure to hold a Writer and amount of data written on it.
   */
  static class WriterLength {
    long written = 0;
    StoreFile.Writer writer = null;
  }

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   */
  private static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
  throws IOException {
    byte[][] byteKeys = table.getStartKeys();
    ArrayList<ImmutableBytesWritable> ret =
      new ArrayList<ImmutableBytesWritable>(byteKeys.length);
    for (byte[] byteKey : byteKeys) {
      ret.add(new ImmutableBytesWritable(byteKey));
    }
    return ret;
  }

  /**
   * Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.
   * @param partitionsPath output path for SequenceFile
   * @param startKeys the region start keys
   */
  private static void writePartitions(Configuration conf, Path partitionsPath,
      List<ImmutableBytesWritable> startKeys) throws IOException {
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0
    TreeSet<ImmutableBytesWritable> sorted =
      new TreeSet<ImmutableBytesWritable>(startKeys);

    ImmutableBytesWritable first = sorted.first();
    if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IllegalArgumentException(
          "First region of table should have empty start key. Instead has: "
          + Bytes.toStringBinary(first.get()));
    }
    sorted.remove(first);

    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs,
        conf, partitionsPath, ImmutableBytesWritable.class, NullWritable.class);

    try {
      for (ImmutableBytesWritable startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * It is safe to set up  this job only if each mapper emits values for one region only 
   * and in a sorted order.
   * @param job
   * @param table
   * @throws IOException
   */
  public static void configAsMapOutputFormat(Job job, HTable table) throws IOException {
    LOG.warn("Set up the HFileOutputFormat as MapperOutputFormat." +
        "It is the mapper task's responsibility to make sure that each mapper emits values " +
        "for one region only and in a sorted order. !");
    Configuration conf = job.getConfiguration();
    if (!KeyValue.class.equals(job.getMapOutputValueClass())) {
      LOG.error("Only support the KeyValue.class as MapOutputValueClass so far!");
      System.exit (-1);
    }

    // Set compression algorithms based on column families
    configureCompression(table, conf);
    // Set BloomFilter type based on column families and
    // relevant parameters.
    configureBloomFilter(table, conf);

    // Configure the DeltaEncoding per family
    configureDeltaEncoding(table, conf);

    LOG.info("Configured the HFileOutputFormat as MapperOutputFormat for table: " +
        table.getTableDescriptor().getNameAsString());
  }

  /**
   * Configure a MapReduce Job to perform an incremental load into the given
   * table. This
   * <ul>
   *   <li>Inspects the table to configure a total order partitioner</li>
   *   <li>Uploads the partitions file to the cluster and adds it to the DistributedCache</li>
   *   <li>Sets the number of reduce tasks to match the current number of regions</li>
   *   <li>Sets the output key/value class to match HFileOutputFormat's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   * </ul>
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, HTable table) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setPartitionerClass(TotalOrderPartitioner.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);
    job.setOutputFormatClass(HFileOutputFormat.class);

    // Based on the configured map output class, set the correct reducer to properly
    // sort the incoming values.
    // TODO it would be nice to pick one or the other of these formats.
    if (KeyValue.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(KeyValueSortReducer.class);
    } else if (Put.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(PutSortReducer.class);
    } else if (RowMutation.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(RowMutationSortReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }

    LOG.info("Looking up current regions for table " + table);
    List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
        "to match current region count");
    job.setNumReduceTasks(startKeys.size());

    Path partitionsPath = new Path(job.getWorkingDirectory(),
        "partitions_" + System.currentTimeMillis());
    LOG.info("Writing partition information to " + partitionsPath);

    FileSystem fs = partitionsPath.getFileSystem(conf);
    writePartitions(conf, partitionsPath, startKeys);
    partitionsPath.makeQualified(fs);
    URI cacheUri;
    try {
      cacheUri = new URI(partitionsPath.toString() + "#" +
          TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, conf);
    DistributedCache.createSymlink(conf);

    // Set compression algorithms based on column families
    configureCompression(table, conf);

    // Set BloomFilter type based on column families and
    // relevant parameters.
    configureBloomFilter(table, conf);

    // Configure the DeltaEncoding per family
    configureDeltaEncoding(table, conf);

    // Configuring the favoredNodes
    configureFavoredNodes(table, conf);

    LOG.info("Incremental table output configured.");
  }

  /**
   * Run inside the task to deserialize column family to compression algorithm
   * map from the
   * configuration.
   *
   * Package-private for unit tests only.
   *
   * @return a map from column family to the name of the configured compression
   *         algorithm
   */
  static Map<byte[], String> createFamilyCompressionMap(Configuration conf) {
    Map<byte[], String> compressionMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
    String compressionConf = conf.get(COMPRESSION_CONF_KEY, "");
    for (String familyConf : compressionConf.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        continue;
      }

      try {
        compressionMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
            URLDecoder.decode(familySplit[1], "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return compressionMap;
  }

  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * Package-private for unit tests only.
   *
   * @throws IOException
   *           on failure to read column family descriptors
   */
  static void configureCompression(HTable table, Configuration conf) throws IOException {
    StringBuilder compressionConfigValue = new StringBuilder();
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if(tableDescriptor == null){
      // could happen with mock table instance
      return;
    }
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        compressionConfigValue.append('&');
      }
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      compressionConfigValue.append('=');
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
    }
    // Get rid of the last ampersand
    conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
  }

  protected static void configureBloomFilter(HTable table, Configuration conf)
  throws IOException {
    // get conf information needed by BloomFilter
    Configuration tableConf = table.getConfiguration();

    // Now go through the column family and save the BloomFilter setting for
    // each column family
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if (tableDescriptor == null){
      return;
    }

    if (tableConf != null) {
      // copying Bloom filter related configuration to conf.
      BloomFilterFactory.copyBloomFilterConf(tableConf, conf);
    }

    StringBuilder bloomfilterTypePerCFConfigValue = new StringBuilder();
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        bloomfilterTypePerCFConfigValue.append('&');
      }

      bloomfilterTypePerCFConfigValue.append(
          URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      bloomfilterTypePerCFConfigValue.append('=');
      bloomfilterTypePerCFConfigValue.append(
          URLEncoder.encode(familyDescriptor.getBloomFilterType().toString(),
          "UTF-8"));
    }

    conf.set(BLOOMFILTER_TYPE_PER_CF_KEY, bloomfilterTypePerCFConfigValue.toString());
  }

  static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
    Map<byte[], BloomType> bloomTypeMap =
      new TreeMap<byte[], BloomType >(Bytes.BYTES_COMPARATOR);
    String bloomFilterTypeConf = conf.get(BLOOMFILTER_TYPE_PER_CF_KEY, "");

    if (bloomFilterTypeConf.isEmpty()) {
      return bloomTypeMap;
    }

    for (String familyConf : bloomFilterTypeConf.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        throw new AssertionError("invalid bloomfilter type configuration");
      }

      try {
        bloomTypeMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
            BloomType.valueOf(URLDecoder.decode(familySplit[1], "UTF-8")));
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return bloomTypeMap;
  }

  protected static void configureDeltaEncoding(HTable table, Configuration conf)
  throws IOException {
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if (tableDescriptor == null){
      return;
    }

    StringBuilder encodingTypePerCFConfigValue = new StringBuilder();
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        encodingTypePerCFConfigValue.append('&');
      }

      encodingTypePerCFConfigValue.append(
          URLEncoder.encode(familyDescriptor.getNameAsString(), UTF8));
      encodingTypePerCFConfigValue.append('=');
      encodingTypePerCFConfigValue.append(
          URLEncoder.encode(familyDescriptor.getDataBlockEncodingOnDisk().toString(),
            UTF8));
      encodingTypePerCFConfigValue.append(':');
      encodingTypePerCFConfigValue.append(
          URLEncoder.encode(familyDescriptor.getDataBlockEncoding().toString(),
            UTF8));
    }

    conf.set(ENCODING_TYPE_PER_CF_KEY, encodingTypePerCFConfigValue.toString());
  }

  static Map<byte[], HFileDataBlockEncoder> createFamilyDeltaEncodingMap(Configuration conf) {
    Map<byte[], HFileDataBlockEncoder> encodingTypeMap =
      new TreeMap<byte[], HFileDataBlockEncoder >(Bytes.BYTES_COMPARATOR);
    String encodingTypeConf = conf.get(ENCODING_TYPE_PER_CF_KEY, "");

    if (encodingTypeConf.isEmpty()) {
      return encodingTypeMap;
    }

    for (String familyConf : encodingTypeConf.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        throw new AssertionError("Invalid Encoding type per family configuration");
      }

      try {
        String[] encodingsForFamily =
          URLDecoder.decode(familySplit[1], UTF8).split(":");
        HFileDataBlockEncoder encoder = new HFileDataBlockEncoderImpl(
          DataBlockEncoding.valueOf(URLDecoder.decode(encodingsForFamily[0], UTF8)),
          DataBlockEncoding.valueOf(URLDecoder.decode(encodingsForFamily[1], UTF8)));
        encodingTypeMap.put(
          URLDecoder.decode(familySplit[0], UTF8).getBytes(),
          encoder);
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return encodingTypeMap;
  }

  protected static void configureFavoredNodes(HTable table, Configuration conf) {
    if (table.getTableName() != null) {
      conf.set(TABLE_NAME, Bytes.toString(table.getTableName()));
    }
    // The rest of the stuff will be taken care of by the RecordWriter
  }

  public static void closeWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    if (HFileOutputFormat.latestWriter != null) {
      latestWriter.close(context);
    }
  }

  /**
   * Create a fake TaskAttemptContext for getting a RecordWriter.
   * By using this, client can write HFiles and bulk upload them to HTable
   * without integration of MapReduce.
   * Important: KeyValues written in HFiles must have the same column family and
   * be sorted by {@Link KeyValue.KVComparator} in advance.
   *
   * @param conf HBase configuration
   * @param outputDir The root directory for writing HFiles
   * @param identifier Unique identifier for each task if there are more than one
   * @return Fake TaskAttemptContext to get a KeyValue writer
   */
  public static TaskAttemptContext createFakeTaskAttemptContext(
      Configuration conf, Path outputDir, int identifier) {
    JobID jobId = new JobID();
    TaskID taskId = new TaskID(jobId, false, identifier);
    conf.set("mapred.output.dir", outputDir.toString());
    TaskAttemptContext context = new TaskAttemptContext(conf, new TaskAttemptID(taskId, 0));

    return context;
  }
}

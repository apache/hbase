/**
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
import java.util.UUID;

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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.annotations.VisibleForTesting;

/**
 * Writes HFiles. Passed KeyValues must arrive in order.
 * Currently, can only write files to a single column family at a
 * time.  Multiple column families requires coordinating keys cross family.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created hfiles. Calling write(null,null) will forcibly roll
 * all HFiles being written.
 * @see KeyValueSortReducer
 */
public class HFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {
  static Log LOG = LogFactory.getLog(HFileOutputFormat.class);
  
  // The following constants are private since these are used by
  // HFileOutputFormat to internally transfer data between job setup and
  // reducer run using conf.
  // These should not be changed by the client.
  private static final String COMPRESSION_FAMILIES_CONF_KEY =
      "hbase.hfileoutputformat.families.compression";
  private static final String BLOOM_TYPE_FAMILIES_CONF_KEY =
      "hbase.hfileoutputformat.families.bloomtype";
  private static final String BLOCK_SIZE_FAMILIES_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.blocksize";
  private static final String DATABLOCK_ENCODING_FAMILIES_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.families.datablock.encoding";

  // This constant is public since the client can modify this when setting
  // up their conf object and thus refer to this symbol.
  // It is present for backwards compatibility reasons. Use it only to
  // override the auto-detection of datablock encoding.
  public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.datablock.encoding";

  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(final TaskAttemptContext context)
  throws IOException, InterruptedException {
    // Get the path of the temporary output file
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();
    final Configuration conf = context.getConfiguration();
    final FileSystem fs = outputdir.getFileSystem(conf);
    // These configs. are from hbase-*.xml
    final long maxsize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
        HConstants.DEFAULT_MAX_FILE_SIZE);
    // Invented config.  Add to hbase-*.xml if other than default compression.
    final String defaultCompressionStr = conf.get("hfile.compression",
        Compression.Algorithm.NONE.getName());
    final Algorithm defaultCompression =
        AbstractHFileWriter.compressionByName(defaultCompressionStr);

    final boolean compactionExclude = conf.getBoolean(
        "hbase.mapreduce.hfileoutputformat.compaction.exclude", false);

    // create a map from column family to the compression algorithm
    final Map<byte[], Algorithm> compressionMap = createFamilyCompressionMap(conf);
    final Map<byte[], BloomType> bloomTypeMap = createFamilyBloomTypeMap(conf);
    final Map<byte[], Integer> blockSizeMap = createFamilyBlockSizeMap(conf);
    
    String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_OVERRIDE_CONF_KEY);
    final Map<byte[], HFileDataBlockEncoder> datablockEncodingMap =
        createFamilyDataBlockEncodingMap(conf);
    final HFileDataBlockEncoder overriddenEncoder;
    if (dataBlockEncodingStr != null) {
      overriddenEncoder = getDataBlockEncoderFromString(dataBlockEncodingStr);
    } else {
      overriddenEncoder = null;
    }

    return new RecordWriter<ImmutableBytesWritable, KeyValue>() {
      // Map of families to writers and how much has been output on the writer.
      private final Map<byte [], WriterLength> writers =
        new TreeMap<byte [], WriterLength>(Bytes.BYTES_COMPARATOR);
      private byte [] previousRow = HConstants.EMPTY_BYTE_ARRAY;
      private final byte [] now = Bytes.toBytes(System.currentTimeMillis());
      private boolean rollRequested = false;

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

      /* Create a new StoreFile.Writer.
       * @param family
       * @return A WriterLength, containing a new StoreFile.Writer.
       * @throws IOException
       */
      private WriterLength getNewWriter(byte[] family, Configuration conf)
          throws IOException {
        WriterLength wl = new WriterLength();
        Path familydir = new Path(outputdir, Bytes.toString(family));
        Algorithm compression = compressionMap.get(family);
        compression = compression == null ? defaultCompression : compression;
        BloomType bloomType = bloomTypeMap.get(family);
        bloomType = bloomType == null ? BloomType.NONE : bloomType;
        Integer blockSize = blockSizeMap.get(family);
        blockSize = blockSize == null ? HFile.DEFAULT_BLOCKSIZE : blockSize;
        HFileDataBlockEncoder encoder = overriddenEncoder;
        encoder = encoder == null ? datablockEncodingMap.get(family) : encoder;
        encoder = encoder == null ? NoOpDataBlockEncoder.INSTANCE : encoder;

        Configuration tempConf = new Configuration(conf);
        tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
        wl.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs, blockSize)
            .withOutputDir(familydir)
            .withCompression(compression)
            .withBloomType(bloomType)
            .withComparator(KeyValue.COMPARATOR)
            .withDataBlockEncoder(encoder)
            .withChecksumType(Store.getChecksumType(conf))
            .withBytesPerChecksum(Store.getBytesPerChecksum(conf))
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
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
              Bytes.toBytes(true));
          w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
              Bytes.toBytes(compactionExclude));
          w.appendTrackedTimestampsToMetadata();
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
  public static void configureIncrementalLoad(Job job, HTable table)
  throws IOException {
    Configuration conf = job.getConfiguration();
    Class<? extends Partitioner> topClass;
    try {
      topClass = getTotalOrderPartitionerClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed getting TotalOrderPartitioner", e);
    }
    job.setPartitionerClass(topClass);
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
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }

    LOG.info("Looking up current regions for table " + table);
    List<ImmutableBytesWritable> startKeys = getRegionStartKeys(table);
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
        "to match current region count");
    job.setNumReduceTasks(startKeys.size());

    Path partitionsPath = new Path(job.getWorkingDirectory(),
                                   "partitions_" + UUID.randomUUID());
    LOG.info("Writing partition information to " + partitionsPath);

    FileSystem fs = partitionsPath.getFileSystem(conf);
    writePartitions(conf, partitionsPath, startKeys);
    partitionsPath.makeQualified(fs);

    URI cacheUri;
    try {
      // Below we make explicit reference to the bundled TOP.  Its cheating.
      // We are assume the define in the hbase bundled TOP is as it is in
      // hadoop (whether 0.20 or 0.22, etc.)
      cacheUri = new URI(partitionsPath.toString() + "#" +
        org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, conf);
    DistributedCache.createSymlink(conf);

    // Set compression algorithms based on column families
    configureCompression(table, conf);
    configureBloomType(table, conf);
    configureBlockSize(table, conf);
    configureDataBlockEncoding(table, conf);
    
    TableMapReduceUtil.addDependencyJars(job);
    LOG.info("Incremental table output configured.");
  }

  /**
   * If > hadoop 0.20, then we want to use the hadoop TotalOrderPartitioner.
   * If 0.20, then we want to use the TOP that we have under hadoopbackport.
   * This method is about hbase being able to run on different versions of
   * hadoop.  In 0.20.x hadoops, we have to use the TOP that is bundled with
   * hbase.  Otherwise, we use the one in Hadoop.
   * @return Instance of the TotalOrderPartitioner class
   * @throws ClassNotFoundException If can't find a TotalOrderPartitioner.
   */
  private static Class<? extends Partitioner> getTotalOrderPartitionerClass()
  throws ClassNotFoundException {
    Class<? extends Partitioner> clazz = null;
    try {
      clazz = (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner");
    } catch (ClassNotFoundException e) {
      clazz =
        (Class<? extends Partitioner>) Class.forName("org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner");
    }
    return clazz;
  }

  /**
   * Runs inside the task to deserialize column family to compression algorithm
   * map from the configuration.
   *
   * @return a map from column family to the name of the configured compression
   *         algorithm
   */
  @VisibleForTesting
  static Map<byte[], Algorithm> createFamilyCompressionMap(Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf, COMPRESSION_FAMILIES_CONF_KEY);
    Map<byte[], Algorithm> compressionMap = new TreeMap<byte[], Algorithm>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      Algorithm algorithm = AbstractHFileWriter.compressionByName(e.getValue());
      compressionMap.put(e.getKey(), algorithm);
    }
    return compressionMap;
  }

  /**
   * Runs inside the task to deserialize column family to bloom type
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured bloom type
   */
  @VisibleForTesting
  static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf, BLOOM_TYPE_FAMILIES_CONF_KEY);
    Map<byte[], BloomType> bloomTypeMap = new TreeMap<byte[], BloomType>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      BloomType bloomType = BloomType.valueOf(e.getValue());
      bloomTypeMap.put(e.getKey(), bloomType);
    }
    return bloomTypeMap;
  }

  /**
   * Runs inside the task to deserialize column family to block size
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured block size
   */
  @VisibleForTesting
  static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf, BLOCK_SIZE_FAMILIES_CONF_KEY);
    Map<byte[], Integer> blockSizeMap = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      Integer blockSize = Integer.parseInt(e.getValue());
      blockSizeMap.put(e.getKey(), blockSize);
    }
    return blockSizeMap;
  }
  
  /**
   * Runs inside the task to deserialize column family to data block encoding type map from the
   * configuration.
   * 
   * @param conf to read the serialized values from
   * @return a map from column family to HFileDataBlockEncoder for the configured data block type
   *         for the family
   */
  @VisibleForTesting
  static Map<byte[], HFileDataBlockEncoder> createFamilyDataBlockEncodingMap(Configuration conf) {
    Map<byte[], String> stringMap =
        createFamilyConfValueMap(conf, DATABLOCK_ENCODING_FAMILIES_CONF_KEY);
    Map<byte[], HFileDataBlockEncoder> encoderMap =
        new TreeMap<byte[], HFileDataBlockEncoder>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      encoderMap.put(e.getKey(), getDataBlockEncoderFromString(e.getValue()));
    }
    return encoderMap;
  }

  private static HFileDataBlockEncoder getDataBlockEncoderFromString(String dataBlockEncodingStr) {
    HFileDataBlockEncoder encoder;
    try {
      encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.valueOf(dataBlockEncodingStr));
    } catch (IllegalArgumentException ex) {
      throw new RuntimeException("Invalid data block encoding type configured for the param "
          + DATABLOCK_ENCODING_FAMILIES_CONF_KEY + " : " + dataBlockEncodingStr);
    }
    return encoder;
  }

  /**
   * Run inside the task to deserialize column family to given conf value map.
   * 
   * @param conf to read the serialized values from
   * @param confName conf key to read from the configuration
   * @return a map of column family to the given configuration value
   */
  private static Map<byte[], String> createFamilyConfValueMap(Configuration conf, String confName) {
    Map<byte[], String> confValMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
    String confVal = conf.get(confName, "");
    for (String familyConf : confVal.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        continue;
      }
      try {
        confValMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
            URLDecoder.decode(familySplit[1], "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return confValMap;
  }
  
  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   * 
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException 
   *            on failure to read column family descriptors
   */
  @VisibleForTesting
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
    conf.set(COMPRESSION_FAMILIES_CONF_KEY, compressionConfigValue.toString());
  }
  
  /**
   * Serialize column family to block size map to configuration.
   * Invoked while configuring the MR job for incremental load.
   * 
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException 
   *            on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureBlockSize(HTable table, Configuration conf) throws IOException {
    StringBuilder blockSizeConfigValue = new StringBuilder();
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if (tableDescriptor == null) {
      // could happen with mock table instance
      return;
    }
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        blockSizeConfigValue.append('&');
      }
      blockSizeConfigValue.append(URLEncoder.encode(
          familyDescriptor.getNameAsString(), "UTF-8"));
      blockSizeConfigValue.append('=');
      blockSizeConfigValue.append(URLEncoder.encode(
          String.valueOf(familyDescriptor.getBlocksize()), "UTF-8"));
    }
    // Get rid of the last ampersand
    conf.set(BLOCK_SIZE_FAMILIES_CONF_KEY, blockSizeConfigValue.toString());
  }

  /**
   * Serialize column family to bloom type map to configuration.
   * Invoked while configuring the MR job for incremental load.
   * 
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException 
   *            on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureBloomType(HTable table, Configuration conf) throws IOException {
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if (tableDescriptor == null) {
      // could happen with mock table instance
      return;
    }
    StringBuilder bloomTypeConfigValue = new StringBuilder();
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        bloomTypeConfigValue.append('&');
      }
      bloomTypeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      bloomTypeConfigValue.append('=');
      String bloomType = familyDescriptor.getBloomFilterType().toString();
      if (bloomType == null) {
        bloomType = HColumnDescriptor.DEFAULT_BLOOMFILTER;
      }
      bloomTypeConfigValue.append(URLEncoder.encode(bloomType, "UTF-8"));
    }
    conf.set(BLOOM_TYPE_FAMILIES_CONF_KEY, bloomTypeConfigValue.toString());
  }

  /**
   * Serialize column family to data block encoding map to configuration.
   * Invoked while configuring the MR job for incremental load.
   * 
   * @param table to read the properties from
   * @param conf to persist serialized values into
   * @throws IOException 
   *            on failure to read column family descriptors
   */
  @VisibleForTesting
  static void configureDataBlockEncoding(HTable table, Configuration conf) throws IOException {
    HTableDescriptor tableDescriptor = table.getTableDescriptor();
    if (tableDescriptor == null) {
      // could happen with mock table instance
      return;
    }
    StringBuilder dataBlockEncodingConfigValue = new StringBuilder();
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        dataBlockEncodingConfigValue.append('&');
      }
      dataBlockEncodingConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(),
        "UTF-8"));
      dataBlockEncodingConfigValue.append('=');
      DataBlockEncoding encoding = familyDescriptor.getDataBlockEncoding();
      if (encoding == null) {
        encoding = DataBlockEncoding.NONE;
      }
      dataBlockEncodingConfigValue.append(URLEncoder.encode(encoding.toString(), "UTF-8"));
    }
    conf.set(DATABLOCK_ENCODING_FAMILIES_CONF_KEY, dataBlockEncodingConfigValue.toString());
  }

}

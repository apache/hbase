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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TASK_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAJOR_COMPACTION_KEY;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes HFiles. Passed Cells must arrive in order.
 * Writes current time as the sequence id for the file. Sets the major compacted
 * attribute on created {@link HFile}s. Calling write(null,null) will forcibly roll
 * all HFiles being written.
 * <p>
 * Using this class as part of a MapReduce job is best done
 * using {@link #configureIncrementalLoad(Job, TableDescriptor, RegionLocator)}.
 */
@InterfaceAudience.Public
public class HFileOutputFormat2
    extends FileOutputFormat<ImmutableBytesWritable, Cell> {
  private static final Logger LOG = LoggerFactory.getLogger(HFileOutputFormat2.class);
  static class TableInfo {
    private TableDescriptor tableDesctiptor;
    private RegionLocator regionLocator;

    public TableInfo(TableDescriptor tableDesctiptor, RegionLocator regionLocator) {
      this.tableDesctiptor = tableDesctiptor;
      this.regionLocator = regionLocator;
    }

    /**
     * The modification for the returned HTD doesn't affect the inner TD.
     * @return A clone of inner table descriptor
     * @deprecated since 2.0.0 and will be removed in 3.0.0. Use {@link #getTableDescriptor()}
     *   instead.
     * @see #getTableDescriptor()
     * @see <a href="https://issues.apache.org/jira/browse/HBASE-18241">HBASE-18241</a>
     */
    @Deprecated
    public HTableDescriptor getHTableDescriptor() {
      return new HTableDescriptor(tableDesctiptor);
    }

    public TableDescriptor getTableDescriptor() {
      return tableDesctiptor;
    }

    public RegionLocator getRegionLocator() {
      return regionLocator;
    }
  }

  protected static final byte[] tableSeparator = Bytes.toBytes(";");

  protected static byte[] combineTableNameSuffix(byte[] tableName, byte[] suffix) {
    return Bytes.add(tableName, tableSeparator, suffix);
  }

  // The following constants are private since these are used by
  // HFileOutputFormat2 to internally transfer data between job setup and
  // reducer run using conf.
  // These should not be changed by the client.
  static final String COMPRESSION_FAMILIES_CONF_KEY =
      "hbase.hfileoutputformat.families.compression";
  static final String BLOOM_TYPE_FAMILIES_CONF_KEY =
      "hbase.hfileoutputformat.families.bloomtype";
  static final String BLOOM_PARAM_FAMILIES_CONF_KEY =
      "hbase.hfileoutputformat.families.bloomparam";
  static final String BLOCK_SIZE_FAMILIES_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.blocksize";
  static final String DATABLOCK_ENCODING_FAMILIES_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.families.datablock.encoding";

  // This constant is public since the client can modify this when setting
  // up their conf object and thus refer to this symbol.
  // It is present for backwards compatibility reasons. Use it only to
  // override the auto-detection of datablock encoding and compression.
  public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.datablock.encoding";
  public static final String COMPRESSION_OVERRIDE_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.compression";

  /**
   * Keep locality while generating HFiles for bulkload. See HBASE-12596
   */
  public static final String LOCALITY_SENSITIVE_CONF_KEY =
      "hbase.bulkload.locality.sensitive.enabled";
  private static final boolean DEFAULT_LOCALITY_SENSITIVE = true;
  static final String OUTPUT_TABLE_NAME_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.table.name";
  static final String MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY =
          "hbase.mapreduce.use.multi.table.hfileoutputformat";

  public static final String REMOTE_CLUSTER_CONF_PREFIX =
    "hbase.hfileoutputformat.remote.cluster.";
  public static final String REMOTE_CLUSTER_ZOOKEEPER_QUORUM_CONF_KEY =
    REMOTE_CLUSTER_CONF_PREFIX + "zookeeper.quorum";
  public static final String REMOTE_CLUSTER_ZOOKEEPER_CLIENT_PORT_CONF_KEY =
    REMOTE_CLUSTER_CONF_PREFIX + "zookeeper." + HConstants.CLIENT_PORT_STR;
  public static final String REMOTE_CLUSTER_ZOOKEEPER_ZNODE_PARENT_CONF_KEY =
    REMOTE_CLUSTER_CONF_PREFIX + HConstants.ZOOKEEPER_ZNODE_PARENT;

  public static final String STORAGE_POLICY_PROPERTY = HStore.BLOCK_STORAGE_POLICY_KEY;
  public static final String STORAGE_POLICY_PROPERTY_CF_PREFIX = STORAGE_POLICY_PROPERTY + ".";

  @Override
  public RecordWriter<ImmutableBytesWritable, Cell> getRecordWriter(
      final TaskAttemptContext context) throws IOException, InterruptedException {
    return createRecordWriter(context, this.getOutputCommitter(context));
  }

  protected static byte[] getTableNameSuffixedWithFamily(byte[] tableName, byte[] family) {
    return combineTableNameSuffix(tableName, family);
  }

  static <V extends Cell> RecordWriter<ImmutableBytesWritable, V> createRecordWriter(
    final TaskAttemptContext context, final OutputCommitter committer) throws IOException {

    // Get the path of the temporary output file
    final Path outputDir = ((FileOutputCommitter)committer).getWorkPath();
    final Configuration conf = context.getConfiguration();
    final boolean writeMultipleTables =
      conf.getBoolean(MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY, false);
    final String writeTableNames = conf.get(OUTPUT_TABLE_NAME_CONF_KEY);
    if (writeTableNames == null || writeTableNames.isEmpty()) {
      throw new IllegalArgumentException("" + OUTPUT_TABLE_NAME_CONF_KEY + " cannot be empty");
    }
    final FileSystem fs = outputDir.getFileSystem(conf);
    // These configs. are from hbase-*.xml
    final long maxsize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
        HConstants.DEFAULT_MAX_FILE_SIZE);
    // Invented config.  Add to hbase-*.xml if other than default compression.
    final String defaultCompressionStr = conf.get("hfile.compression",
        Compression.Algorithm.NONE.getName());
    final Algorithm defaultCompression = HFileWriterImpl.compressionByName(defaultCompressionStr);
    String compressionStr = conf.get(COMPRESSION_OVERRIDE_CONF_KEY);
    final Algorithm overriddenCompression = compressionStr != null ?
      Compression.getCompressionAlgorithmByName(compressionStr): null;
    final boolean compactionExclude = conf.getBoolean(
        "hbase.mapreduce.hfileoutputformat.compaction.exclude", false);
    final Set<String> allTableNames = Arrays.stream(writeTableNames.split(
            Bytes.toString(tableSeparator))).collect(Collectors.toSet());

    // create a map from column family to the compression algorithm
    final Map<byte[], Algorithm> compressionMap = createFamilyCompressionMap(conf);
    final Map<byte[], BloomType> bloomTypeMap = createFamilyBloomTypeMap(conf);
    final Map<byte[], String> bloomParamMap = createFamilyBloomParamMap(conf);
    final Map<byte[], Integer> blockSizeMap = createFamilyBlockSizeMap(conf);

    String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_OVERRIDE_CONF_KEY);
    final Map<byte[], DataBlockEncoding> datablockEncodingMap
        = createFamilyDataBlockEncodingMap(conf);
    final DataBlockEncoding overriddenEncoding = dataBlockEncodingStr != null ?
      DataBlockEncoding.valueOf(dataBlockEncodingStr) : null;

    return new RecordWriter<ImmutableBytesWritable, V>() {
      // Map of families to writers and how much has been output on the writer.
      private final Map<byte[], WriterLength> writers = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      private final Map<byte[], byte[]> previousRows = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      private final long now = EnvironmentEdgeManager.currentTime();

      @Override
      public void write(ImmutableBytesWritable row, V cell) throws IOException {
        Cell kv = cell;
        // null input == user explicitly wants to flush
        if (row == null && kv == null) {
          rollWriters(null);
          return;
        }

        byte[] rowKey = CellUtil.cloneRow(kv);
        int length = (PrivateCellUtil.estimatedSerializedSizeOf(kv)) - Bytes.SIZEOF_INT;
        byte[] family = CellUtil.cloneFamily(kv);
        byte[] tableNameBytes = null;
        if (writeMultipleTables) {
          tableNameBytes = MultiTableHFileOutputFormat.getTableName(row.get());
          tableNameBytes = TableName.valueOf(tableNameBytes).toBytes();
          if (!allTableNames.contains(Bytes.toString(tableNameBytes))) {
            throw new IllegalArgumentException("TableName " + Bytes.toString(tableNameBytes) +
              " not expected");
          }
        } else {
          tableNameBytes = Bytes.toBytes(writeTableNames);
        }
        Path tableRelPath = getTableRelativePath(tableNameBytes);
        byte[] tableAndFamily = getTableNameSuffixedWithFamily(tableNameBytes, family);
        WriterLength wl = this.writers.get(tableAndFamily);

        // If this is a new column family, verify that the directory exists
        if (wl == null) {
          Path writerPath = null;
          if (writeMultipleTables) {
            writerPath = new Path(outputDir,new Path(tableRelPath, Bytes.toString(family)));
          }
          else {
            writerPath = new Path(outputDir, Bytes.toString(family));
          }
          fs.mkdirs(writerPath);
          configureStoragePolicy(conf, fs, tableAndFamily, writerPath);
        }

        // This can only happen once a row is finished though
        if (wl != null && wl.written + length >= maxsize
                && Bytes.compareTo(this.previousRows.get(family), rowKey) != 0) {
          rollWriters(wl);
        }

        // create a new WAL writer, if necessary
        if (wl == null || wl.writer == null) {
          if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
            HRegionLocation loc = null;

            String tableName = Bytes.toString(tableNameBytes);
            if (tableName != null) {
              try (Connection connection = ConnectionFactory.createConnection(
                createRemoteClusterConf(conf));
                     RegionLocator locator =
                       connection.getRegionLocator(TableName.valueOf(tableName))) {
                loc = locator.getRegionLocation(rowKey);
              } catch (Throwable e) {
                LOG.warn("Something wrong locating rowkey {} in {}",
                  Bytes.toString(rowKey), tableName, e);
                loc = null;
              } }

            if (null == loc) {
              LOG.trace("Failed get of location, use default writer {}", Bytes.toString(rowKey));
              wl = getNewWriter(tableNameBytes, family, conf, null);
            } else {
              LOG.debug("First rowkey: [{}]", Bytes.toString(rowKey));
              InetSocketAddress initialIsa =
                  new InetSocketAddress(loc.getHostname(), loc.getPort());
              if (initialIsa.isUnresolved()) {
                LOG.trace("Failed resolve address {}, use default writer", loc.getHostnamePort());
                wl = getNewWriter(tableNameBytes, family, conf, null);
              } else {
                LOG.debug("Use favored nodes writer: {}", initialIsa.getHostString());
                wl = getNewWriter(tableNameBytes, family, conf, new InetSocketAddress[] { initialIsa
                });
              }
            }
          } else {
            wl = getNewWriter(tableNameBytes, family, conf, null);
          }
        }

        // we now have the proper WAL writer. full steam ahead
        PrivateCellUtil.updateLatestStamp(cell, this.now);
        wl.writer.append(kv);
        wl.written += length;

        // Copy the row so we know when a row transition.
        this.previousRows.put(family, rowKey);
      }

      private Path getTableRelativePath(byte[] tableNameBytes) {
        String tableName = Bytes.toString(tableNameBytes);
        String[] tableNameParts = tableName.split(":");
        Path tableRelPath = new Path(tableName.split(":")[0]);
        if (tableNameParts.length > 1) {
          tableRelPath = new Path(tableRelPath, tableName.split(":")[1]);
        }
        return tableRelPath;
      }

      private void rollWriters(WriterLength writerLength) throws IOException {
        if (writerLength != null) {
          closeWriter(writerLength);
        } else {
          for (WriterLength wl : this.writers.values()) {
            closeWriter(wl);
          }
        }
      }

      private void closeWriter(WriterLength wl) throws IOException {
        if (wl.writer != null) {
          LOG.info("Writer=" + wl.writer.getPath() +
            ((wl.written == 0)? "": ", wrote=" + wl.written));
          close(wl.writer);
          wl.writer = null;
        }
        wl.written = 0;
      }

      private Configuration createRemoteClusterConf(Configuration conf) {
        final Configuration newConf = new Configuration(conf);

        final String quorum = conf.get(REMOTE_CLUSTER_ZOOKEEPER_QUORUM_CONF_KEY);
        final String clientPort = conf.get(REMOTE_CLUSTER_ZOOKEEPER_CLIENT_PORT_CONF_KEY);
        final String parent = conf.get(REMOTE_CLUSTER_ZOOKEEPER_ZNODE_PARENT_CONF_KEY);

        if (quorum != null && clientPort != null && parent != null) {
          newConf.set(HConstants.ZOOKEEPER_QUORUM, quorum);
          newConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.parseInt(clientPort));
          newConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parent);
        }

        for (Entry<String, String> entry : conf) {
          String key = entry.getKey();
          if (REMOTE_CLUSTER_ZOOKEEPER_QUORUM_CONF_KEY.equals(key) ||
              REMOTE_CLUSTER_ZOOKEEPER_CLIENT_PORT_CONF_KEY.equals(key) ||
              REMOTE_CLUSTER_ZOOKEEPER_ZNODE_PARENT_CONF_KEY.equals(key)) {
            // Handled them above
            continue;
          }

          if (entry.getKey().startsWith(REMOTE_CLUSTER_CONF_PREFIX)) {
            String originalKey = entry.getKey().substring(REMOTE_CLUSTER_CONF_PREFIX.length());
            if (!originalKey.isEmpty()) {
              newConf.set(originalKey, entry.getValue());
            }
          }
        }

        return newConf;
      }

      /*
       * Create a new StoreFile.Writer.
       * @return A WriterLength, containing a new StoreFile.Writer.
       */
      @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="BX_UNBOXING_IMMEDIATELY_REBOXED",
          justification="Not important")
      private WriterLength getNewWriter(byte[] tableName, byte[] family, Configuration conf,
          InetSocketAddress[] favoredNodes) throws IOException {
        byte[] tableAndFamily = getTableNameSuffixedWithFamily(tableName, family);
        Path familydir = new Path(outputDir, Bytes.toString(family));
        if (writeMultipleTables) {
          familydir = new Path(outputDir,
            new Path(getTableRelativePath(tableName), Bytes.toString(family)));
        }
        WriterLength wl = new WriterLength();
        Algorithm compression = overriddenCompression;
        compression = compression == null ? compressionMap.get(tableAndFamily) : compression;
        compression = compression == null ? defaultCompression : compression;
        BloomType bloomType = bloomTypeMap.get(tableAndFamily);
        bloomType = bloomType == null ? BloomType.NONE : bloomType;
        String bloomParam = bloomParamMap.get(tableAndFamily);
        if (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH) {
          conf.set(BloomFilterUtil.PREFIX_LENGTH_KEY, bloomParam);
        }
        Integer blockSize = blockSizeMap.get(tableAndFamily);
        blockSize = blockSize == null ? HConstants.DEFAULT_BLOCKSIZE : blockSize;
        DataBlockEncoding encoding = overriddenEncoding;
        encoding = encoding == null ? datablockEncodingMap.get(tableAndFamily) : encoding;
        encoding = encoding == null ? DataBlockEncoding.NONE : encoding;
        HFileContextBuilder contextBuilder = new HFileContextBuilder().withCompression(compression)
          .withDataBlockEncoding(encoding).withChecksumType(StoreUtils.getChecksumType(conf))
          .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf)).withBlockSize(blockSize)
          .withColumnFamily(family).withTableName(tableName);

        if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
          contextBuilder.withIncludesTags(true);
        }

        HFileContext hFileContext = contextBuilder.build();
        if (null == favoredNodes) {
          wl.writer = new StoreFileWriter.Builder(conf, CacheConfig.DISABLED, fs)
            .withOutputDir(familydir).withBloomType(bloomType)
            .withFileContext(hFileContext).build();
        } else {
          wl.writer = new StoreFileWriter.Builder(conf, CacheConfig.DISABLED, new HFileSystem(fs))
            .withOutputDir(familydir).withBloomType(bloomType)
            .withFileContext(hFileContext).withFavoredNodes(favoredNodes).build();
        }

        this.writers.put(tableAndFamily, wl);
        return wl;
      }

      private void close(final StoreFileWriter w) throws IOException {
        if (w != null) {
          w.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(BULKLOAD_TASK_KEY, Bytes.toBytes(context.getTaskAttemptID().toString()));
          w.appendFileInfo(MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
          w.appendFileInfo(EXCLUDE_FROM_MINOR_COMPACTION_KEY, Bytes.toBytes(compactionExclude));
          w.appendTrackedTimestampsToMetadata();
          w.close();
        }
      }

      @Override
      public void close(TaskAttemptContext c) throws IOException, InterruptedException {
        for (WriterLength wl: this.writers.values()) {
          close(wl.writer);
        }
      }
    };
  }

  /**
   * Configure block storage policy for CF after the directory is created.
   */
  static void configureStoragePolicy(final Configuration conf, final FileSystem fs,
      byte[] tableAndFamily, Path cfPath) {
    if (null == conf || null == fs || null == tableAndFamily || null == cfPath) {
      return;
    }

    String policy =
        conf.get(STORAGE_POLICY_PROPERTY_CF_PREFIX + Bytes.toString(tableAndFamily),
          conf.get(STORAGE_POLICY_PROPERTY));
    CommonFSUtils.setStoragePolicy(fs, cfPath, policy);
  }

  /*
   * Data structure to hold a Writer and amount of data written on it.
   */
  static class WriterLength {
    long written = 0;
    StoreFileWriter writer = null;
  }

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   */
  private static List<ImmutableBytesWritable> getRegionStartKeys(List<RegionLocator> regionLocators,
                                                                 boolean writeMultipleTables)
          throws IOException {

    ArrayList<ImmutableBytesWritable> ret = new ArrayList<>();
    for(RegionLocator regionLocator : regionLocators) {
      TableName tableName = regionLocator.getName();
      LOG.info("Looking up current regions for table " + tableName);
      byte[][] byteKeys = regionLocator.getStartKeys();
      for (byte[] byteKey : byteKeys) {
        byte[] fullKey = byteKey; //HFileOutputFormat2 use case
        if (writeMultipleTables) {
          //MultiTableHFileOutputFormat use case
          fullKey = combineTableNameSuffix(tableName.getName(), byteKey);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("SplitPoint startkey for " + tableName + ": " + Bytes.toStringBinary(fullKey));
        }
        ret.add(new ImmutableBytesWritable(fullKey));
      }
    }
    return ret;
  }

  /**
   * Write out a {@link SequenceFile} that can be read by
   * {@link TotalOrderPartitioner} that contains the split points in startKeys.
   */
  @SuppressWarnings("deprecation")
  private static void writePartitions(Configuration conf, Path partitionsPath,
      List<ImmutableBytesWritable> startKeys, boolean writeMultipleTables) throws IOException {
    LOG.info("Writing partition information to " + partitionsPath);
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0
    TreeSet<ImmutableBytesWritable> sorted = new TreeSet<>(startKeys);
    ImmutableBytesWritable first = sorted.first();
    if (writeMultipleTables) {
      first =
        new ImmutableBytesWritable(MultiTableHFileOutputFormat.getSuffix(sorted.first().get()));
    }
    if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IllegalArgumentException(
          "First region of table should have empty start key. Instead has: "
          + Bytes.toStringBinary(first.get()));
    }
    sorted.remove(sorted.first());

    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(
      fs, conf, partitionsPath, ImmutableBytesWritable.class,
      NullWritable.class);

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
   *   <li>Sets the output key/value class to match HFileOutputFormat2's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   *   <li>Sets the HBase cluster key to load region locations for locality-sensitive</li>
   * </ul>
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, Table table, RegionLocator regionLocator)
      throws IOException {
    configureIncrementalLoad(job, table.getDescriptor(), regionLocator);
    configureRemoteCluster(job, table.getConfiguration());
  }

  /**
   * Configure a MapReduce Job to perform an incremental load into the given
   * table. This
   * <ul>
   *   <li>Inspects the table to configure a total order partitioner</li>
   *   <li>Uploads the partitions file to the cluster and adds it to the DistributedCache</li>
   *   <li>Sets the number of reduce tasks to match the current number of regions</li>
   *   <li>Sets the output key/value class to match HFileOutputFormat2's requirements</li>
   *   <li>Sets the reducer up to perform the appropriate sorting (either KeyValueSortReducer or
   *     PutSortReducer)</li>
   * </ul>
   * The user should be sure to set the map output value class to either KeyValue or Put before
   * running this function.
   */
  public static void configureIncrementalLoad(Job job, TableDescriptor tableDescriptor,
      RegionLocator regionLocator) throws IOException {
    ArrayList<TableInfo> singleTableInfo = new ArrayList<>();
    singleTableInfo.add(new TableInfo(tableDescriptor, regionLocator));
    configureIncrementalLoad(job, singleTableInfo, HFileOutputFormat2.class);
  }

  static void configureIncrementalLoad(Job job, List<TableInfo> multiTableInfo,
      Class<? extends OutputFormat<?, ?>> cls) throws IOException {
    Configuration conf = job.getConfiguration();
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(MapReduceExtendedCell.class);
    job.setOutputFormatClass(cls);

    if (multiTableInfo.stream().distinct().count() != multiTableInfo.size()) {
      throw new IllegalArgumentException("Duplicate entries found in TableInfo argument");
    }
    boolean writeMultipleTables = false;
    if (MultiTableHFileOutputFormat.class.equals(cls)) {
      writeMultipleTables = true;
      conf.setBoolean(MULTI_TABLE_HFILEOUTPUTFORMAT_CONF_KEY, true);
    }
    // Based on the configured map output class, set the correct reducer to properly
    // sort the incoming values.
    // TODO it would be nice to pick one or the other of these formats.
    if (KeyValue.class.equals(job.getMapOutputValueClass())
        || MapReduceExtendedCell.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(CellSortReducer.class);
    } else if (Put.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(PutSortReducer.class);
    } else if (Text.class.equals(job.getMapOutputValueClass())) {
      job.setReducerClass(TextSortReducer.class);
    } else {
      LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
    }

    conf.setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        CellSerialization.class.getName());

    if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
      LOG.info("bulkload locality sensitive enabled");
    }

    /* Now get the region start keys for every table required */
    List<String> allTableNames = new ArrayList<>(multiTableInfo.size());
    List<RegionLocator> regionLocators = new ArrayList<>(multiTableInfo.size());
    List<TableDescriptor> tableDescriptors = new ArrayList<>(multiTableInfo.size());

    for(TableInfo tableInfo : multiTableInfo) {
      regionLocators.add(tableInfo.getRegionLocator());
      allTableNames.add(tableInfo.getRegionLocator().getName().getNameAsString());
      tableDescriptors.add(tableInfo.getTableDescriptor());
    }
    // Record tablenames for creating writer by favored nodes, and decoding compression,
    // block size and other attributes of columnfamily per table
    conf.set(OUTPUT_TABLE_NAME_CONF_KEY, StringUtils.join(allTableNames, Bytes
            .toString(tableSeparator)));
    List<ImmutableBytesWritable> startKeys =
      getRegionStartKeys(regionLocators, writeMultipleTables);
    // Use table's region boundaries for TOP split points.
    LOG.info("Configuring " + startKeys.size() + " reduce partitions " +
        "to match current region count for all tables");
    job.setNumReduceTasks(startKeys.size());

    configurePartitioner(job, startKeys, writeMultipleTables);
    // Set compression algorithms based on column families

    conf.set(COMPRESSION_FAMILIES_CONF_KEY, serializeColumnFamilyAttribute(compressionDetails,
            tableDescriptors));
    conf.set(BLOCK_SIZE_FAMILIES_CONF_KEY, serializeColumnFamilyAttribute(blockSizeDetails,
            tableDescriptors));
    conf.set(BLOOM_TYPE_FAMILIES_CONF_KEY, serializeColumnFamilyAttribute(bloomTypeDetails,
            tableDescriptors));
    conf.set(BLOOM_PARAM_FAMILIES_CONF_KEY, serializeColumnFamilyAttribute(bloomParamDetails,
        tableDescriptors));
    conf.set(DATABLOCK_ENCODING_FAMILIES_CONF_KEY,
            serializeColumnFamilyAttribute(dataBlockEncodingDetails, tableDescriptors));

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
    LOG.info("Incremental output configured for tables: " + StringUtils.join(allTableNames, ","));
  }

  public static void configureIncrementalLoadMap(Job job, TableDescriptor tableDescriptor) throws
      IOException {
    Configuration conf = job.getConfiguration();

    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(MapReduceExtendedCell.class);
    job.setOutputFormatClass(HFileOutputFormat2.class);

    ArrayList<TableDescriptor> singleTableDescriptor = new ArrayList<>(1);
    singleTableDescriptor.add(tableDescriptor);

    conf.set(OUTPUT_TABLE_NAME_CONF_KEY, tableDescriptor.getTableName().getNameAsString());
    // Set compression algorithms based on column families
    conf.set(COMPRESSION_FAMILIES_CONF_KEY,
        serializeColumnFamilyAttribute(compressionDetails, singleTableDescriptor));
    conf.set(BLOCK_SIZE_FAMILIES_CONF_KEY,
        serializeColumnFamilyAttribute(blockSizeDetails, singleTableDescriptor));
    conf.set(BLOOM_TYPE_FAMILIES_CONF_KEY,
        serializeColumnFamilyAttribute(bloomTypeDetails, singleTableDescriptor));
    conf.set(BLOOM_PARAM_FAMILIES_CONF_KEY,
        serializeColumnFamilyAttribute(bloomParamDetails, singleTableDescriptor));
    conf.set(DATABLOCK_ENCODING_FAMILIES_CONF_KEY,
        serializeColumnFamilyAttribute(dataBlockEncodingDetails, singleTableDescriptor));

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
    LOG.info("Incremental table " + tableDescriptor.getTableName() + " output configured.");
  }

  /**
   * Configure HBase cluster key for remote cluster to load region location for locality-sensitive
   * if it's enabled.
   * It's not necessary to call this method explicitly when the cluster key for HBase cluster to be
   * used to load region location is configured in the job configuration.
   * Call this method when another HBase cluster key is configured in the job configuration.
   * For example, you should call when you load data from HBase cluster A using
   * {@link TableInputFormat} and generate hfiles for HBase cluster B.
   * Otherwise, HFileOutputFormat2 fetch location from cluster A and locality-sensitive won't
   * working correctly.
   * {@link #configureIncrementalLoad(Job, Table, RegionLocator)} calls this method using
   * {@link Table#getConfiguration} as clusterConf.
   * See HBASE-25608.
   *
   * @param job which has configuration to be updated
   * @param clusterConf which contains cluster key of the HBase cluster to be locality-sensitive
   *
   * @see #configureIncrementalLoad(Job, Table, RegionLocator)
   * @see #LOCALITY_SENSITIVE_CONF_KEY
   * @see #REMOTE_CLUSTER_ZOOKEEPER_QUORUM_CONF_KEY
   * @see #REMOTE_CLUSTER_ZOOKEEPER_CLIENT_PORT_CONF_KEY
   * @see #REMOTE_CLUSTER_ZOOKEEPER_ZNODE_PARENT_CONF_KEY
   */
  public static void configureRemoteCluster(Job job, Configuration clusterConf) {
    Configuration conf = job.getConfiguration();

    if (!conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
      return;
    }

    final String quorum = clusterConf.get(HConstants.ZOOKEEPER_QUORUM);
    final int clientPort = clusterConf.getInt(
      HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT);
    final String parent = clusterConf.get(
      HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);

    conf.set(REMOTE_CLUSTER_ZOOKEEPER_QUORUM_CONF_KEY, quorum);
    conf.setInt(REMOTE_CLUSTER_ZOOKEEPER_CLIENT_PORT_CONF_KEY, clientPort);
    conf.set(REMOTE_CLUSTER_ZOOKEEPER_ZNODE_PARENT_CONF_KEY, parent);

    LOG.info("ZK configs for remote cluster of bulkload is configured: " +
      quorum + ":" + clientPort + "/" + parent);
  }

  /**
   * Runs inside the task to deserialize column family to compression algorithm
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured compression algorithm
   */
  @InterfaceAudience.Private
  static Map<byte[], Algorithm> createFamilyCompressionMap(Configuration
      conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf,
        COMPRESSION_FAMILIES_CONF_KEY);
    Map<byte[], Algorithm> compressionMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      Algorithm algorithm = HFileWriterImpl.compressionByName(e.getValue());
      compressionMap.put(e.getKey(), algorithm);
    }
    return compressionMap;
  }

  /**
   * Runs inside the task to deserialize column family to bloom filter type
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the the configured bloom filter type
   */
  @InterfaceAudience.Private
  static Map<byte[], BloomType> createFamilyBloomTypeMap(Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf,
        BLOOM_TYPE_FAMILIES_CONF_KEY);
    Map<byte[], BloomType> bloomTypeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      BloomType bloomType = BloomType.valueOf(e.getValue());
      bloomTypeMap.put(e.getKey(), bloomType);
    }
    return bloomTypeMap;
  }

  /**
   * Runs inside the task to deserialize column family to bloom filter param
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the the configured bloom filter param
   */
  @InterfaceAudience.Private
  static Map<byte[], String> createFamilyBloomParamMap(Configuration conf) {
    return createFamilyConfValueMap(conf, BLOOM_PARAM_FAMILIES_CONF_KEY);
  }


  /**
   * Runs inside the task to deserialize column family to block size
   * map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to the configured block size
   */
  @InterfaceAudience.Private
  static Map<byte[], Integer> createFamilyBlockSizeMap(Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf,
        BLOCK_SIZE_FAMILIES_CONF_KEY);
    Map<byte[], Integer> blockSizeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      Integer blockSize = Integer.parseInt(e.getValue());
      blockSizeMap.put(e.getKey(), blockSize);
    }
    return blockSizeMap;
  }

  /**
   * Runs inside the task to deserialize column family to data block encoding
   * type map from the configuration.
   *
   * @param conf to read the serialized values from
   * @return a map from column family to HFileDataBlockEncoder for the
   *         configured data block type for the family
   */
  @InterfaceAudience.Private
  static Map<byte[], DataBlockEncoding> createFamilyDataBlockEncodingMap(
      Configuration conf) {
    Map<byte[], String> stringMap = createFamilyConfValueMap(conf,
        DATABLOCK_ENCODING_FAMILIES_CONF_KEY);
    Map<byte[], DataBlockEncoding> encoderMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], String> e : stringMap.entrySet()) {
      encoderMap.put(e.getKey(), DataBlockEncoding.valueOf((e.getValue())));
    }
    return encoderMap;
  }


  /**
   * Run inside the task to deserialize column family to given conf value map.
   *
   * @param conf to read the serialized values from
   * @param confName conf key to read from the configuration
   * @return a map of column family to the given configuration value
   */
  private static Map<byte[], String> createFamilyConfValueMap(
      Configuration conf, String confName) {
    Map<byte[], String> confValMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    String confVal = conf.get(confName, "");
    for (String familyConf : confVal.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        continue;
      }
      try {
        confValMap.put(Bytes.toBytes(URLDecoder.decode(familySplit[0], "UTF-8")),
            URLDecoder.decode(familySplit[1], "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return confValMap;
  }

  /**
   * Configure <code>job</code> with a TotalOrderPartitioner, partitioning against
   * <code>splitPoints</code>. Cleans up the partitions file after job exists.
   */
  static void configurePartitioner(Job job, List<ImmutableBytesWritable> splitPoints, boolean
          writeMultipleTables)
      throws IOException {
    Configuration conf = job.getConfiguration();
    // create the partitions file
    FileSystem fs = FileSystem.get(conf);
    String hbaseTmpFsDir =
        conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
          HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    Path partitionsPath = new Path(hbaseTmpFsDir, "partitions_" + UUID.randomUUID());
    fs.makeQualified(partitionsPath);
    writePartitions(conf, partitionsPath, splitPoints, writeMultipleTables);
    fs.deleteOnExit(partitionsPath);

    // configure job to use it
    job.setPartitionerClass(TotalOrderPartitioner.class);
    TotalOrderPartitioner.setPartitionFile(conf, partitionsPath);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value =
    "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @InterfaceAudience.Private
  static String serializeColumnFamilyAttribute(Function<ColumnFamilyDescriptor, String> fn,
        List<TableDescriptor> allTables)
      throws UnsupportedEncodingException {
    StringBuilder attributeValue = new StringBuilder();
    int i = 0;
    for (TableDescriptor tableDescriptor : allTables) {
      if (tableDescriptor == null) {
        // could happen with mock table instance
        // CODEREVIEW: Can I set an empty string in conf if mock table instance?
        return "";
      }
      for (ColumnFamilyDescriptor familyDescriptor : tableDescriptor.getColumnFamilies()) {
        if (i++ > 0) {
          attributeValue.append('&');
        }
        attributeValue.append(URLEncoder.encode(
          Bytes.toString(combineTableNameSuffix(tableDescriptor.getTableName().getName(),
            familyDescriptor.getName())), "UTF-8"));
        attributeValue.append('=');
        attributeValue.append(URLEncoder.encode(fn.apply(familyDescriptor), "UTF-8"));
      }
    }
    // Get rid of the last ampersand
    return attributeValue.toString();
  }

  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   */
  @InterfaceAudience.Private
  static Function<ColumnFamilyDescriptor, String> compressionDetails = familyDescriptor ->
          familyDescriptor.getCompressionType().getName();

  /**
   * Serialize column family to block size map to configuration. Invoked while
   * configuring the MR job for incremental load.
   */
  @InterfaceAudience.Private
  static Function<ColumnFamilyDescriptor, String> blockSizeDetails = familyDescriptor -> String
          .valueOf(familyDescriptor.getBlocksize());

  /**
   * Serialize column family to bloom type map to configuration. Invoked while
   * configuring the MR job for incremental load.
   */
  @InterfaceAudience.Private
  static Function<ColumnFamilyDescriptor, String> bloomTypeDetails = familyDescriptor -> {
    String bloomType = familyDescriptor.getBloomFilterType().toString();
    if (bloomType == null) {
      bloomType = ColumnFamilyDescriptorBuilder.DEFAULT_BLOOMFILTER.name();
    }
    return bloomType;
  };

  /**
   * Serialize column family to bloom param map to configuration. Invoked while
   * configuring the MR job for incremental load.
   */
  @InterfaceAudience.Private
  static Function<ColumnFamilyDescriptor, String> bloomParamDetails = familyDescriptor -> {
    BloomType bloomType = familyDescriptor.getBloomFilterType();
    String bloomParam = "";
    if (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH) {
      bloomParam = familyDescriptor.getConfigurationValue(BloomFilterUtil.PREFIX_LENGTH_KEY);
    }
    return bloomParam;
  };

  /**
   * Serialize column family to data block encoding map to configuration.
   * Invoked while configuring the MR job for incremental load.
   */
  @InterfaceAudience.Private
  static Function<ColumnFamilyDescriptor, String> dataBlockEncodingDetails = familyDescriptor -> {
    DataBlockEncoding encoding = familyDescriptor.getDataBlockEncoding();
    if (encoding == null) {
      encoding = DataBlockEncoding.NONE;
    }
    return encoding.toString();
  };

}

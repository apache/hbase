/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class LoadIncrementalHFilesJob extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(LoadIncrementalHFilesJob.class);

  public static final String ROOT_DIR = "loadincrementalhfilesjob.root.dir";
  public static final String TABLE_NAME = "loadincrementalhfilesjob.table.name";
  public static final String MAX_MAP_TASK = "loadincrementalhfilesjob.max.map.tasks";
  public static final String DEPTH = "loadincrementalhfilesjob.depth";

  @Override public int run(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create(getConf());

    return run(args[0], args[1], configuration);
  }

  @VisibleForTesting public int run(String rootPath, String table, Configuration configuration)
      throws Exception {
    Path basePath = new Path(rootPath);
    configuration.set(TABLE_NAME, table);
    configuration.set(ROOT_DIR, basePath.toString());

    if (!bulkload(basePath, table, configuration)) {
      return 1;
    }

    return 0;
  }

  private boolean bulkload(Path rootDir, String table, Configuration configuration)
      throws Exception {
    Configuration jobConfiguration = new Configuration(configuration);

    applyNonOverridableOptions(jobConfiguration);

    Job job = Job.getInstance(jobConfiguration, "Bulkload-" + table + "-" + rootDir.toString());

    TableMapReduceUtil.addDependencyJars(job);

    job.setJarByClass(LoadIncrementalHFilesJob.class);

    job.setInputFormatClass(BulkLoadInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapperClass(BulkoadMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

    return job.waitForCompletion(true);
  }

  private static void applyNonOverridableOptions(Configuration configuration) {
    configuration.setInt("mapred.map.max.attempts", 1);
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapreduce.map.speculative", false);
    configuration.setBoolean("mapreduce.task.classpath.user.precedence", true);
    configuration.setInt("mapreduce.task.timeout", 0);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new LoadIncrementalHFilesJob(), args);
    System.exit(ret);
  }

  public static class BulkoadMapper extends Mapper<Text, Text, NullWritable, NullWritable> {
    LoadIncrementalHFiles loadIncrementalHFiles;
    private final Deque<LoadIncrementalHFiles.LoadQueueItem> queue = new LinkedList<>();

    @Override protected void setup(Context context) throws IOException, InterruptedException {
      try {
        loadIncrementalHFiles = new LoadIncrementalHFiles(context.getConfiguration());
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate LoadIncrementalHFiles", e);
      }
    }

    @Override protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      byte[] family = Bytes.toBytes(key.toString());
      Path hfilePath = new Path(value.toString());
      LoadIncrementalHFiles.LoadQueueItem loadQueueItem =
          new LoadIncrementalHFiles.LoadQueueItem(family, hfilePath);
      queue.add(loadQueueItem);
    }

    @Override protected void cleanup(Context context) throws IOException, InterruptedException {
      int queueSize = queue.size();
      String tableStr = context.getConfiguration().get(TABLE_NAME);
      try (Connection connection = ConnectionFactory.createConnection(context.getConfiguration());
          Table table = connection.getTable(TableName.valueOf(tableStr));
          RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableStr))) {
        loadIncrementalHFiles.doBulkloadFromQueue(queue, table, regionLocator, connection);
      }
      context.getCounter("Bulkload", "Files Loaded").increment(queueSize);
    }
  }

  public static class BulkLoadInputFormat extends InputFormat<Text, Text> {

    private static final Log LOG = LogFactory.getLog(BulkLoadInputFormat.class);

    @Override public List<InputSplit> getSplits(JobContext jobContext)
        throws IOException, InterruptedException {
      Configuration configuration = jobContext.getConfiguration();

      List<InputSplit> result = new ArrayList<>();
      List<BulkoadInputWritable> bulkoadInputWritables = new ArrayList<>();

      int depth = configuration.getInt(DEPTH, 2);
      if (depth != 2 && depth != 3) {
        throw new IllegalStateException("Invalid depth " + depth);
      }
      Set<String> validFamilies = getValidFamiliesSet(configuration);


      Path rootPath = new Path(configuration.get(ROOT_DIR));
      FileSystem fs = rootPath.getFileSystem(configuration);

      for (FileStatus child : fs.listStatus(rootPath)) {
        if (!child.isDirectory()) {
          LOG.info("Skipping non dir " + child.getPath());
          continue;
        }
        if (child.getPath().getName().startsWith("_")) {
          LOG.info("Skipping " + child.getPath());
          continue;
        }
        if (depth == 2) {
          bulkoadInputWritables
                  .addAll(createSplitsForFamilyDirectory(fs, child.getPath(), validFamilies));
        } else {
          for (FileStatus family : fs.listStatus(child.getPath())) {
            bulkoadInputWritables
                    .addAll(createSplitsForFamilyDirectory(fs, family.getPath(), validFamilies));
          }
        }
      }

      int maxMapTasks = configuration.getInt(MAX_MAP_TASK, 1);

      int filesPerMap = bulkoadInputWritables.size() / maxMapTasks;
      for (List<BulkoadInputWritable> partition : Lists
              .partition(bulkoadInputWritables, filesPerMap)) {
        BulkoadInputWritableArray bulkoadInputWritableArray = new BulkoadInputWritableArray();
        bulkoadInputWritableArray
                .set(partition.toArray(new BulkoadInputWritable[partition.size()]));
        result.add(new BulkloadInputSplit(bulkoadInputWritableArray));
      }

      return result;
    }

    private Set<String> getValidFamiliesSet(Configuration configuration) throws IOException {
      TableName tableName = TableName.valueOf(configuration.get(TABLE_NAME));
      try (Connection connection = ConnectionFactory.createConnection(configuration)) {
        try (Table table = connection.getTable(tableName)) {
          Set<String> families = new HashSet<>();
          for (HColumnDescriptor columnDescriptor : table.getTableDescriptor()
              .getColumnFamilies()) {
            families.add(columnDescriptor.getNameAsString());
          }
          return families;
        }
      }
    }

    private List<BulkoadInputWritable> createSplitsForFamilyDirectory(FileSystem fs,
        Path familyPath, Set<String> validFamilies) throws IOException {
      if (!validFamilies.contains(familyPath.getName())) {
        throw new IOException("Unmatched family names found " + familyPath.getName());
      }
      LOG.info("Listing family dir " + familyPath);
      List<BulkoadInputWritable> bulkloadInputSplits = new ArrayList<>();
      Text family = new Text(familyPath.getName());
      for (FileStatus file : fs.listStatus(familyPath)) {
        if (file.isDirectory()) {
          LOG.warn("Skipping non-file " + file.getPath());
          continue;
        }
        if (StoreFileInfo.isReference(file.getPath()) || HFileLink.isHFileLink(file.getPath())) {
          continue;
        }
        bulkloadInputSplits
            .add(new BulkoadInputWritable(new Text(family), new Text(file.getPath().toString())));
      }

      return bulkloadInputSplits;
    }

    @Override public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit,
        TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      return new RecordReader<Text, Text>() {

        Iterator<Writable> iterator;
        BulkoadInputWritable current;

        @Override

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
          iterator = Iterators.forArray(((BulkloadInputSplit) inputSplit).bulkloadInputs.get());
        }

        @Override public boolean nextKeyValue() throws IOException, InterruptedException {
          if (!iterator.hasNext()) {
            return false;
          }
          current = (BulkoadInputWritable) iterator.next();
          return true;
        }

        @Override public Text getCurrentKey() throws IOException, InterruptedException {
          return current.family;
        }

        @Override public Text getCurrentValue() throws IOException, InterruptedException {
          return current.hfile;
        }

        @Override public float getProgress() throws IOException, InterruptedException {
          return 0;
        }

        @Override public void close() throws IOException {

        }
      };
    }
  }

  public static class BulkoadInputWritable implements Writable {
    private Text family = new Text();
    private Text hfile = new Text();

    public BulkoadInputWritable() {
    }

    public BulkoadInputWritable(Text family, Text hfile) {
      this.family = family;
      this.hfile = hfile;
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
      family.readFields(dataInput);
      hfile.readFields(dataInput);
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
      family.write(dataOutput);
      hfile.write(dataOutput);
    }
  }

  public static class BulkoadInputWritableArray extends ArrayWritable {
    public BulkoadInputWritableArray() {
      super(BulkoadInputWritable.class);
    }

    public BulkoadInputWritable[] asArray() {
      return (BulkoadInputWritable[]) get();
    }
  }

  public static class BulkloadInputSplit extends InputSplit implements Writable {

    private BulkoadInputWritableArray bulkloadInputs = new BulkoadInputWritableArray();

    public BulkloadInputSplit() {
    }

    public BulkloadInputSplit(BulkoadInputWritableArray bulkloadInputs) {
      this.bulkloadInputs = bulkloadInputs;
    }

    @Override public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
      bulkloadInputs.write(dataOutput);
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
      bulkloadInputs.readFields(dataInput);
    }
  }
}

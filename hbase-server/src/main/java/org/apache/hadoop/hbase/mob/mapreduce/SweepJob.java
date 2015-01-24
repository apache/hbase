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
package org.apache.hadoop.hbase.mob.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;

/**
 * The sweep job.
 * Run map reduce to merge the smaller mob files into bigger ones and cleans the unused ones.
 */
@InterfaceAudience.Private
public class SweepJob {

  private final FileSystem fs;
  private final Configuration conf;
  private static final Log LOG = LogFactory.getLog(SweepJob.class);
  static final String SWEEP_JOB_ID = "mob.sweep.job.id";
  static final String SWEEP_JOB_SERVERNAME = "mob.sweep.job.servername";
  static final String SWEEP_JOB_TABLE_NODE = "mob.sweep.job.table.node";
  static final String WORKING_DIR_KEY = "mob.sweep.job.dir";
  static final String WORKING_ALLNAMES_FILE_KEY = "mob.sweep.job.all.file";
  static final String WORKING_VISITED_DIR_KEY = "mob.sweep.job.visited.dir";
  static final String WORKING_ALLNAMES_DIR = "all";
  static final String WORKING_VISITED_DIR = "visited";
  public static final String WORKING_FILES_DIR_KEY = "mob.sweep.job.files.dir";
  //the MOB_SWEEP_JOB_DELAY is ONE_DAY by default. Its value is only changed when testing.
  public static final String MOB_SWEEP_JOB_DELAY = "hbase.mob.sweep.job.delay";
  protected static long ONE_DAY = 24 * 60 * 60 * 1000;
  private long compactionStartTime = EnvironmentEdgeManager.currentTime();
  public final static String CREDENTIALS_LOCATION = "credentials_location";
  private CacheConfig cacheConfig;
  static final int SCAN_CACHING = 10000;
  private TableLockManager tableLockManager;

  public SweepJob(Configuration conf, FileSystem fs) {
    this.conf = conf;
    this.fs = fs;
    // disable the block cache.
    Configuration copyOfConf = new Configuration(conf);
    copyOfConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0f);
    cacheConfig = new CacheConfig(copyOfConf);
  }

  static ServerName getCurrentServerName(Configuration conf) throws IOException {
    String hostname = conf.get(
        "hbase.regionserver.ipc.address",
        Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            conf.get("hbase.regionserver.dns.interface", "default"),
            conf.get("hbase.regionserver.dns.nameserver", "default"))));
    int port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    return ServerName.valueOf(initialIsa.getHostName(), initialIsa.getPort(),
        EnvironmentEdgeManager.currentTime());
  }

  /**
   * Runs MapReduce to do the sweeping on the mob files.
   * There's a MobReferenceOnlyFilter so that the mappers only get the cells that have mob
   * references from 'normal' regions' rows.
   * The running of the sweep tool on the same column family are mutually exclusive.
   * The HBase major compaction and running of the sweep tool on the same column family
   * are mutually exclusive.
   * The synchronization is done by the Zookeeper.
   * So in the beginning of the running, we need to make sure only this sweep tool is the only one
   * that is currently running in this column family, and in this column family there're no major
   * compaction in progress.
   * @param tn The current table name.
   * @param family The descriptor of the current column family.
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws KeeperException
   */
  public int sweep(TableName tn, HColumnDescriptor family) throws IOException,
      ClassNotFoundException, InterruptedException, KeeperException {
    Configuration conf = new Configuration(this.conf);
    // check whether the current user is the same one with the owner of hbase root
    String currentUserName = UserGroupInformation.getCurrentUser().getShortUserName();
    FileStatus[] hbaseRootFileStat = fs.listStatus(new Path(conf.get(HConstants.HBASE_DIR)));
    if (hbaseRootFileStat.length > 0) {
      String owner = hbaseRootFileStat[0].getOwner();
      if (!owner.equals(currentUserName)) {
        String errorMsg = "The current user[" + currentUserName
            + "] doesn't have hbase root credentials."
            + " Please make sure the user is the root of the target HBase";
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
    } else {
      LOG.error("The target HBase doesn't exist");
      throw new IOException("The target HBase doesn't exist");
    }
    String familyName = family.getNameAsString();
    String id = "SweepJob" + UUID.randomUUID().toString().replace("-", "");
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, id, new DummyMobAbortable());
    try {
      ServerName serverName = getCurrentServerName(conf);
      tableLockManager = TableLockManager.createTableLockManager(conf, zkw, serverName);
      TableName lockName = MobUtils.getTableLockName(tn);
      TableLock lock = tableLockManager.writeLock(lockName, "Run sweep tool");
      String tableName = tn.getNameAsString();
      // Try to obtain the lock. Use this lock to synchronize all the query
      try {
        lock.acquire();
      } catch (Exception e) {
        LOG.warn("Can not lock the table " + tableName
            + ". The major compaction in HBase may be in-progress or another sweep job is running."
            + " Please re-run the job.");
        return 3;
      }
      Job job = null;
      try {
        Scan scan = new Scan();
        scan.addFamily(family.getName());
        // Do not retrieve the mob data when scanning
        scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
        scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
        scan.setCaching(SCAN_CACHING);
        scan.setCacheBlocks(false);
        scan.setMaxVersions(family.getMaxVersions());
        conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
            JavaSerialization.class.getName() + "," + WritableSerialization.class.getName());
        conf.set(SWEEP_JOB_ID, id);
        conf.set(SWEEP_JOB_SERVERNAME, serverName.toString());
        String tableLockNode = ZKUtil.joinZNode(zkw.tableLockZNode, lockName.getNameAsString());
        conf.set(SWEEP_JOB_TABLE_NODE, tableLockNode);
        job = prepareJob(tn, familyName, scan, conf);
        job.getConfiguration().set(TableInputFormat.SCAN_COLUMN_FAMILY, familyName);
        // Record the compaction start time.
        // In the sweep tool, only the mob file whose modification time is older than
        // (startTime - delay) could be handled by this tool.
        // The delay is one day. It could be configured as well, but this is only used
        // in the test.
        job.getConfiguration().setLong(MobConstants.MOB_SWEEP_TOOL_COMPACTION_START_DATE,
            compactionStartTime);

        job.setPartitionerClass(MobFilePathHashPartitioner.class);
        submit(job, tn, familyName);
        if (job.waitForCompletion(true)) {
          // Archive the unused mob files.
          removeUnusedFiles(job, tn, family);
        } else {
          System.err.println("Job Failed");
          return 4;
        }
      } finally {
        try {
          cleanup(job, tn, familyName);
        } finally {
          try {
            lock.release();
          } catch (IOException e) {
            LOG.error("Fail to release the table lock " + tableName, e);
          }
        }
      }
    } finally {
      zkw.close();
    }
    return 0;
  }

  /**
   * Prepares a map reduce job.
   * @param tn The current table name.
   * @param familyName The current family name.
   * @param scan The current scan.
   * @param conf The current configuration.
   * @return A map reduce job.
   * @throws IOException
   */
  private Job prepareJob(TableName tn, String familyName, Scan scan, Configuration conf)
      throws IOException {
    Job job = Job.getInstance(conf);
    job.setJarByClass(SweepMapper.class);
    TableMapReduceUtil.initTableMapperJob(tn.getNameAsString(), scan,
        SweepMapper.class, Text.class, Writable.class, job);

    job.setInputFormatClass(TableInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(KeyValue.class);
    job.setReducerClass(SweepReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    String jobName = getCustomJobName(this.getClass().getSimpleName(), tn.getNameAsString(),
        familyName);
    job.setJobName(jobName);
    if (StringUtils.isNotEmpty(conf.get(CREDENTIALS_LOCATION))) {
      String fileLoc = conf.get(CREDENTIALS_LOCATION);
      Credentials cred = Credentials.readTokenStorageFile(new File(fileLoc), conf);
      job.getCredentials().addAll(cred);
    }
    return job;
  }

  /**
   * Gets a customized job name.
   * It's className-mapperClassName-reducerClassName-tableName-familyName.
   * @param className The current class name.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The customized job name.
   */
  private static String getCustomJobName(String className, String tableName, String familyName) {
    StringBuilder name = new StringBuilder();
    name.append(className);
    name.append('-').append(SweepMapper.class.getSimpleName());
    name.append('-').append(SweepReducer.class.getSimpleName());
    name.append('-').append(tableName);
    name.append('-').append(familyName);
    return name.toString();
  }

  /**
   * Submits a job.
   * @param job The current job.
   * @param tn The current table name.
   * @param familyName The current family name.
   * @throws IOException
   */
  private void submit(Job job, TableName tn, String familyName) throws IOException {
    // delete the temp directory of the mob files in case the failure in the previous
    // execution.
    Path tempDir =
        new Path(MobUtils.getMobHome(job.getConfiguration()), MobConstants.TEMP_DIR_NAME);
    Path mobCompactionTempDir =
        new Path(tempDir, MobConstants.MOB_SWEEP_TOOL_COMPACTION_TEMP_DIR_NAME);
    Path workingPath = MobUtils.getCompactionWorkingPath(mobCompactionTempDir, job.getJobName());
    job.getConfiguration().set(WORKING_DIR_KEY, workingPath.toString());
    // delete the working directory in case it'not deleted by the last running.
    fs.delete(workingPath, true);
    // create the working directory.
    fs.mkdirs(workingPath);
    // create a sequence file which contains the names of all the existing files.
    Path workingPathOfFiles = new Path(workingPath, "files");
    Path workingPathOfNames = new Path(workingPath, "names");
    job.getConfiguration().set(WORKING_FILES_DIR_KEY, workingPathOfFiles.toString());
    Path allFileNamesPath = new Path(workingPathOfNames, WORKING_ALLNAMES_DIR);
    job.getConfiguration().set(WORKING_ALLNAMES_FILE_KEY, allFileNamesPath.toString());
    Path vistiedFileNamesPath = new Path(workingPathOfNames, WORKING_VISITED_DIR);
    job.getConfiguration().set(WORKING_VISITED_DIR_KEY, vistiedFileNamesPath.toString());
    // create a directory where the files contain names of visited mob files are saved.
    fs.mkdirs(vistiedFileNamesPath);
    Path mobStorePath = MobUtils.getMobFamilyPath(job.getConfiguration(), tn, familyName);
    // Find all the files whose creation time are older than one day.
    // Write those file names to a file.
    // In each reducer there's a writer, it write the visited file names to a file which is saved
    // in WORKING_VISITED_DIR.
    // After the job is finished, compare those files, then find out the unused mob files and
    // archive them.
    FileStatus[] files = fs.listStatus(mobStorePath);
    Set<String> fileNames = new TreeSet<String>();
    long mobCompactionDelay = job.getConfiguration().getLong(MOB_SWEEP_JOB_DELAY, ONE_DAY);
    for (FileStatus fileStatus : files) {
      if (fileStatus.isFile() && !HFileLink.isHFileLink(fileStatus.getPath())) {
        if (compactionStartTime - fileStatus.getModificationTime() > mobCompactionDelay) {
          // only record the potentially unused files older than one day.
          fileNames.add(fileStatus.getPath().getName());
        }
      }
    }
    FSDataOutputStream fout = null;
    SequenceFile.Writer writer = null;
    try {
      // create a file includes all the existing mob files whose creation time is older than
      // (now - oneDay)
      fout = fs.create(allFileNamesPath, true);
      // write the names to a sequence file
      writer = SequenceFile.createWriter(job.getConfiguration(), fout, String.class, String.class,
          CompressionType.NONE, null);
      for (String fileName : fileNames) {
        writer.append(fileName, MobConstants.EMPTY_STRING);
      }
      writer.hflush();
    } finally {
      if (writer != null) {
        IOUtils.closeStream(writer);
      }
      if (fout != null) {
        IOUtils.closeStream(fout);
      }
    }
  }

  /**
   * Gets the unused mob files.
   * Compare the file which contains all the existing mob files and the visited files,
   * find out the unused mob file and archive them.
   * @param conf The current configuration.
   * @return The unused mob files.
   * @throws IOException
   */
  List<String> getUnusedFiles(Configuration conf) throws IOException {
    // find out the unused files and archive them
    Path allFileNamesPath = new Path(conf.get(WORKING_ALLNAMES_FILE_KEY));
    SequenceFile.Reader allNamesReader = null;
    MergeSortReader visitedNamesReader = null;
    List<String> toBeArchived = new ArrayList<String>();
    try {
      allNamesReader = new SequenceFile.Reader(fs, allFileNamesPath, conf);
      visitedNamesReader = new MergeSortReader(fs, conf,
          new Path(conf.get(WORKING_VISITED_DIR_KEY)));
      String nextAll = (String) allNamesReader.next((String) null);
      String nextVisited = visitedNamesReader.next();
      do {
        if (nextAll != null) {
          if (nextVisited != null) {
            int compare = nextAll.compareTo(nextVisited);
            if (compare < 0) {
              toBeArchived.add(nextAll);
              nextAll = (String) allNamesReader.next((String) null);
            } else if (compare > 0) {
              nextVisited = visitedNamesReader.next();
            } else {
              nextAll = (String) allNamesReader.next((String) null);
              nextVisited = visitedNamesReader.next();
            }
          } else {
            toBeArchived.add(nextAll);
            nextAll = (String) allNamesReader.next((String) null);
          }
        } else {
          break;
        }
      } while (nextAll != null || nextVisited != null);
    } finally {
      if (allNamesReader != null) {
        IOUtils.closeStream(allNamesReader);
      }
      if (visitedNamesReader != null) {
        visitedNamesReader.close();
      }
    }
    return toBeArchived;
  }

  /**
   * Archives unused mob files.
   * @param job The current job.
   * @param tn The current table name.
   * @param hcd The descriptor of the current column family.
   * @throws IOException
   */
  private void removeUnusedFiles(Job job, TableName tn, HColumnDescriptor hcd) throws IOException {
    // find out the unused files and archive them
    List<StoreFile> storeFiles = new ArrayList<StoreFile>();
    List<String> toBeArchived = getUnusedFiles(job.getConfiguration());
    // archive them
    Path mobStorePath = MobUtils
        .getMobFamilyPath(job.getConfiguration(), tn, hcd.getNameAsString());
    for (String archiveFileName : toBeArchived) {
      Path path = new Path(mobStorePath, archiveFileName);
      storeFiles.add(new StoreFile(fs, path, job.getConfiguration(), cacheConfig, BloomType.NONE));
    }
    if (!storeFiles.isEmpty()) {
      try {
        MobUtils.removeMobFiles(job.getConfiguration(), fs, tn,
            FSUtils.getTableDir(MobUtils.getMobHome(conf), tn), hcd.getName(), storeFiles);
        LOG.info(storeFiles.size() + " unused MOB files are removed");
      } catch (Exception e) {
        LOG.error("Fail to archive the store files " + storeFiles, e);
      }
    }
  }

  /**
   * Deletes the working directory.
   * @param job The current job.
   * @param familyName The family to cleanup
   */
  private void cleanup(Job job, TableName tn, String familyName) {
    if (job != null) {
      // delete the working directory
      Path workingPath = new Path(job.getConfiguration().get(WORKING_DIR_KEY));
      try {
        fs.delete(workingPath, true);
      } catch (IOException e) {
        LOG.warn("Fail to delete the working directory after sweeping store " + familyName
            + " in the table " + tn.getNameAsString(), e);
      }
    }
  }

  /**
   * A result with index.
   */
  private class IndexedResult implements Comparable<IndexedResult> {
    private int index;
    private String value;

    public IndexedResult(int index, String value) {
      this.index = index;
      this.value = value;
    }

    public int getIndex() {
      return this.index;
    }

    public String getValue() {
      return this.value;
    }

    @Override
    public int compareTo(IndexedResult o) {
      if (this.value == null) {
        return 0;
      } else if (o.value == null) {
        return 1;
      } else {
        return this.value.compareTo(o.value);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof IndexedResult)) {
        return false;
      }
      return compareTo((IndexedResult) obj) == 0;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  /**
   * Merge sort reader.
   * It merges and sort the readers in different sequence files as one where
   * the results are read in order.
   */
  private class MergeSortReader {

    private List<SequenceFile.Reader> readers = new ArrayList<SequenceFile.Reader>();
    private PriorityQueue<IndexedResult> results = new PriorityQueue<IndexedResult>();

    public MergeSortReader(FileSystem fs, Configuration conf, Path path) throws IOException {
      if (fs.exists(path)) {
        FileStatus[] files = fs.listStatus(path);
        int index = 0;
        for (FileStatus file : files) {
          if (file.isFile()) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf);
            String key = (String) reader.next((String) null);
            if (key != null) {
              results.add(new IndexedResult(index, key));
              readers.add(reader);
              index++;
            }
          }
        }
      }
    }

    public String next() throws IOException {
      IndexedResult result = results.poll();
      if (result != null) {
        SequenceFile.Reader reader = readers.get(result.getIndex());
        String key = (String) reader.next((String) null);
        if (key != null) {
          results.add(new IndexedResult(result.getIndex(), key));
        }
        return result.getValue();
      }
      return null;
    }

    public void close() {
      for (SequenceFile.Reader reader : readers) {
        if (reader != null) {
          IOUtils.closeStream(reader);
        }
      }
    }
  }

  /**
   * The counter used in sweep job.
   */
  public enum SweepCounter {

    /**
     * How many files are read.
     */
    INPUT_FILE_COUNT,

    /**
     * How many files need to be merged or cleaned.
     */
    FILE_TO_BE_MERGE_OR_CLEAN,

    /**
     * How many files are left after merging.
     */
    FILE_AFTER_MERGE_OR_CLEAN,

    /**
     * How many records are updated.
     */
    RECORDS_UPDATED,
  }

  public static class DummyMobAbortable implements Abortable {

    private boolean abort = false;

    public void abort(String why, Throwable e) {
      abort = true;
    }

    public boolean isAborted() {
      return abort;
    }

  }
}
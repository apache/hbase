/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles.LoadQueueItem;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

/**
 * It is used for replicating HFile entries. It will first copy parallely all the hfiles to a local
 * staging directory and then it will use ({@link LoadIncrementalHFiles} to prepare a collection of
 * {@link LoadQueueItem} which will finally be loaded(replicated) into the table of this cluster.
 */
@InterfaceAudience.Private
public class HFileReplicator {
  /** Maximum number of threads to allow in pool to copy hfiles during replication */
  public static final String REPLICATION_BULKLOAD_COPY_MAXTHREADS_KEY =
      "hbase.replication.bulkload.copy.maxthreads";
  public static final int REPLICATION_BULKLOAD_COPY_MAXTHREADS_DEFAULT = 10;
  /** Number of hfiles to copy per thread during replication */
  public static final String REPLICATION_BULKLOAD_COPY_HFILES_PERTHREAD_KEY =
      "hbase.replication.bulkload.copy.hfiles.perthread";
  public static final int REPLICATION_BULKLOAD_COPY_HFILES_PERTHREAD_DEFAULT = 10;

  private static final Log LOG = LogFactory.getLog(HFileReplicator.class);
  private static final String UNDERSCORE = "_";
  private final static FsPermission PERM_ALL_ACCESS = FsPermission.valueOf("-rwxrwxrwx");

  private Configuration sourceClusterConf;
  private String sourceBaseNamespaceDirPath;
  private String sourceHFileArchiveDirPath;
  private Map<String, List<Pair<byte[], List<String>>>> bulkLoadHFileMap;
  private FileSystem sinkFs;
  private FsDelegationToken fsDelegationToken;
  private UserProvider userProvider;
  private Configuration conf;
  private Connection connection;
  private Path hbaseStagingDir;
  private ThreadPoolExecutor exec;
  private int maxCopyThreads;
  private int copiesPerThread;

  public HFileReplicator(Configuration sourceClusterConf,
      String sourceBaseNamespaceDirPath, String sourceHFileArchiveDirPath,
      Map<String, List<Pair<byte[], List<String>>>> tableQueueMap, Configuration conf,
      Connection connection) throws IOException {
    this.sourceClusterConf = sourceClusterConf;
    this.sourceBaseNamespaceDirPath = sourceBaseNamespaceDirPath;
    this.sourceHFileArchiveDirPath = sourceHFileArchiveDirPath;
    this.bulkLoadHFileMap = tableQueueMap;
    this.conf = conf;
    this.connection = connection;

    userProvider = UserProvider.instantiate(conf);
    fsDelegationToken = new FsDelegationToken(userProvider, "renewer");
    this.hbaseStagingDir = new Path(FSUtils.getRootDir(conf), HConstants.BULKLOAD_STAGING_DIR_NAME);
    this.maxCopyThreads =
        this.conf.getInt(REPLICATION_BULKLOAD_COPY_MAXTHREADS_KEY,
          REPLICATION_BULKLOAD_COPY_MAXTHREADS_DEFAULT);
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("HFileReplicationCallable-%1$d");
    this.exec =
        new ThreadPoolExecutor(maxCopyThreads, maxCopyThreads, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), builder.build());
    this.exec.allowCoreThreadTimeOut(true);
    this.copiesPerThread =
        conf.getInt(REPLICATION_BULKLOAD_COPY_HFILES_PERTHREAD_KEY,
          REPLICATION_BULKLOAD_COPY_HFILES_PERTHREAD_DEFAULT);

    sinkFs = FileSystem.get(conf);
  }

  public Void replicate() throws IOException {
    // Copy all the hfiles to the local file system
    Map<String, Path> tableStagingDirsMap = copyHFilesToStagingDir();

    int maxRetries = conf.getInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 10);

    for (Entry<String, Path> tableStagingDir : tableStagingDirsMap.entrySet()) {
      String tableNameString = tableStagingDir.getKey();
      Path stagingDir = tableStagingDir.getValue();

      LoadIncrementalHFiles loadHFiles = null;
      try {
        loadHFiles = new LoadIncrementalHFiles(conf);
      } catch (Exception e) {
        LOG.error("Failed to initialize LoadIncrementalHFiles for replicating bulk loaded"
            + " data.", e);
        throw new IOException(e);
      }
      Configuration newConf = HBaseConfiguration.create(conf);
      newConf.set(LoadIncrementalHFiles.CREATE_TABLE_CONF_KEY, "no");
      loadHFiles.setConf(newConf);

      TableName tableName = TableName.valueOf(tableNameString);
      Table table = this.connection.getTable(tableName);

      // Prepare collection of queue of hfiles to be loaded(replicated)
      Deque<LoadQueueItem> queue = new LinkedList<LoadQueueItem>();
      loadHFiles.prepareHFileQueue(stagingDir, table, queue, false);

      if (queue.isEmpty()) {
        LOG.warn("Replication process did not find any files to replicate in directory "
            + stagingDir.toUri());
        return null;
      }

      try (RegionLocator locator = connection.getRegionLocator(tableName)) {

        fsDelegationToken.acquireDelegationToken(sinkFs);

        // Set the staging directory which will be used by LoadIncrementalHFiles for loading the
        // data
        loadHFiles.setBulkToken(stagingDir.toString());

        doBulkLoad(loadHFiles, table, queue, locator, maxRetries);
      } finally {
        cleanup(stagingDir.toString(), table);
      }
    }
    return null;
  }

  private void doBulkLoad(LoadIncrementalHFiles loadHFiles, Table table,
      Deque<LoadQueueItem> queue, RegionLocator locator, int maxRetries) throws IOException {
    int count = 0;
    Pair<byte[][], byte[][]> startEndKeys;
    while (!queue.isEmpty()) {
      // need to reload split keys each iteration.
      startEndKeys = locator.getStartEndKeys();
      if (count != 0) {
        LOG.warn("Error occured while replicating HFiles, retry attempt " + count + " with "
            + queue.size() + " files still remaining to replicate.");
      }

      if (maxRetries != 0 && count >= maxRetries) {
        throw new IOException("Retry attempted " + count
            + " times without completing, bailing out.");
      }
      count++;

      // Try bulk load
      loadHFiles.loadHFileQueue(table, connection, queue, startEndKeys);
    }
  }

  private void cleanup(String stagingDir, Table table) {
    // Release the file system delegation token
    fsDelegationToken.releaseDelegationToken();
    // Delete the staging directory
    if (stagingDir != null) {
      try {
        sinkFs.delete(new Path(stagingDir), true);
      } catch (IOException e) {
        LOG.warn("Failed to delete the staging directory " + stagingDir, e);
      }
    }
    // Do not close the file system

    /*
     * if (sinkFs != null) { try { sinkFs.close(); } catch (IOException e) { LOG.warn(
     * "Failed to close the file system"); } }
     */

    // Close the table
    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the table.", e);
      }
    }
  }

  private Map<String, Path> copyHFilesToStagingDir() throws IOException {
    Map<String, Path> mapOfCopiedHFiles = new HashMap<String, Path>();
    Pair<byte[], List<String>> familyHFilePathsPair;
    List<String> hfilePaths;
    byte[] family;
    Path familyStagingDir;
    int familyHFilePathsPairsListSize;
    int totalNoOfHFiles;
    List<Pair<byte[], List<String>>> familyHFilePathsPairsList;
    FileSystem sourceFs = null;

    try {
      Path sourceClusterPath = new Path(sourceBaseNamespaceDirPath);
      /*
       * Path#getFileSystem will by default get the FS from cache. If both source and sink cluster
       * has same FS name service then it will return peer cluster FS. To avoid this we explicitly
       * disable the loading of FS from cache, so that a new FS is created with source cluster
       * configuration.
       */
      String sourceScheme = sourceClusterPath.toUri().getScheme();
      String disableCacheName =
          String.format("fs.%s.impl.disable.cache", new Object[] { sourceScheme });
      sourceClusterConf.setBoolean(disableCacheName, true);

      sourceFs = sourceClusterPath.getFileSystem(sourceClusterConf);

      User user = userProvider.getCurrent();
      // For each table name in the map
      for (Entry<String, List<Pair<byte[], List<String>>>> tableEntry : bulkLoadHFileMap
          .entrySet()) {
        String tableName = tableEntry.getKey();

        // Create staging directory for each table
        Path stagingDir =
            createStagingDir(hbaseStagingDir, user, TableName.valueOf(tableName));

        familyHFilePathsPairsList = tableEntry.getValue();
        familyHFilePathsPairsListSize = familyHFilePathsPairsList.size();

        // For each list of family hfile paths pair in the table
        for (int i = 0; i < familyHFilePathsPairsListSize; i++) {
          familyHFilePathsPair = familyHFilePathsPairsList.get(i);

          family = familyHFilePathsPair.getFirst();
          hfilePaths = familyHFilePathsPair.getSecond();

          familyStagingDir = new Path(stagingDir, Bytes.toString(family));
          totalNoOfHFiles = hfilePaths.size();

          // For each list of hfile paths for the family
          List<Future<Void>> futures = new ArrayList<Future<Void>>();
          Callable<Void> c;
          Future<Void> future;
          int currentCopied = 0;
          // Copy the hfiles parallely
          while (totalNoOfHFiles > currentCopied + this.copiesPerThread) {
            c =
                new Copier(sourceFs, familyStagingDir, hfilePaths.subList(currentCopied,
                  currentCopied + this.copiesPerThread));
            future = exec.submit(c);
            futures.add(future);
            currentCopied += this.copiesPerThread;
          }

          int remaining = totalNoOfHFiles - currentCopied;
          if (remaining > 0) {
            c =
                new Copier(sourceFs, familyStagingDir, hfilePaths.subList(currentCopied,
                  currentCopied + remaining));
            future = exec.submit(c);
            futures.add(future);
          }

          for (Future<Void> f : futures) {
            try {
              f.get();
            } catch (InterruptedException e) {
              InterruptedIOException iioe =
                  new InterruptedIOException(
                      "Failed to copy HFiles to local file system. This will be retried again "
                          + "by the source cluster.");
              iioe.initCause(e);
              throw iioe;
            } catch (ExecutionException e) {
              throw new IOException("Failed to copy HFiles to local file system. This will "
                  + "be retried again by the source cluster.", e);
            }
          }
        }
        // Add the staging directory to this table. Staging directory contains all the hfiles
        // belonging to this table
        mapOfCopiedHFiles.put(tableName, stagingDir);
      }
      return mapOfCopiedHFiles;
    } finally {
      if (sourceFs != null) {
        sourceFs.close();
      }
      if(exec != null) {
        exec.shutdown();
      }
    }
  }

  private Path createStagingDir(Path baseDir, User user, TableName tableName) throws IOException {
    String tblName = tableName.getNameAsString().replace(":", UNDERSCORE);
    int RANDOM_WIDTH = 320;
    int RANDOM_RADIX = 32;
    String doubleUnderScore = UNDERSCORE + UNDERSCORE;
    String randomDir = user.getShortName() + doubleUnderScore + tblName + doubleUnderScore
        + (new BigInteger(RANDOM_WIDTH, new SecureRandom()).toString(RANDOM_RADIX));
    return createStagingDir(baseDir, user, randomDir);
  }

  private Path createStagingDir(Path baseDir, User user, String randomDir) throws IOException {
    Path p = new Path(baseDir, randomDir);
    sinkFs.mkdirs(p, PERM_ALL_ACCESS);
    sinkFs.setPermission(p, PERM_ALL_ACCESS);
    return p;
  }

  /**
   * This class will copy the given hfiles from the given source file system to the given local file
   * system staging directory.
   */
  private class Copier implements Callable<Void> {
    private FileSystem sourceFs;
    private Path stagingDir;
    private List<String> hfiles;

    public Copier(FileSystem sourceFs, final Path stagingDir, final List<String> hfiles)
        throws IOException {
      this.sourceFs = sourceFs;
      this.stagingDir = stagingDir;
      this.hfiles = hfiles;
    }

    @Override
    public Void call() throws IOException {
      Path sourceHFilePath;
      Path localHFilePath;
      int totalHFiles = hfiles.size();
      for (int i = 0; i < totalHFiles; i++) {
        sourceHFilePath = new Path(sourceBaseNamespaceDirPath, hfiles.get(i));
        localHFilePath = new Path(stagingDir, sourceHFilePath.getName());
        try {
          FileUtil.copy(sourceFs, sourceHFilePath, sinkFs, localHFilePath, false, conf);
          // If any other exception other than FNFE then we will fail the replication requests and
          // source will retry to replicate these data.
        } catch (FileNotFoundException e) {
          LOG.info("Failed to copy hfile from " + sourceHFilePath + " to " + localHFilePath
              + ". Trying to copy from hfile archive directory.",
            e);
          sourceHFilePath = new Path(sourceHFileArchiveDirPath, hfiles.get(i));

          try {
            FileUtil.copy(sourceFs, sourceHFilePath, sinkFs, localHFilePath, false, conf);
          } catch (FileNotFoundException e1) {
            // This will mean that the hfile does not exists any where in source cluster FS. So we
            // cannot do anything here just log and continue.
            LOG.error("Failed to copy hfile from " + sourceHFilePath + " to " + localHFilePath
                + ". Hence ignoring this hfile from replication..",
              e1);
            continue;
          }
        }
        sinkFs.setPermission(localHFilePath, PERM_ALL_ACCESS);
      }
      return null;
    }
  }
}

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
package org.apache.hadoop.hbase.backup.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.mapreduce.BulkLoadCollectorJob;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to run BulkLoadCollectorJob over a comma-separated list of WAL directories and return a
 * deduplicated list of discovered bulk-load file paths.
 */
@InterfaceAudience.Private
public final class BulkFilesCollector {

  private static final Logger LOG = LoggerFactory.getLogger(BulkFilesCollector.class);

  private BulkFilesCollector() {
    /* static only */ }

  /**
   * Convenience overload: collector will create and configure BulkLoadCollectorJob internally.
   * @param conf           cluster/configuration used to initialize job and access FS
   * @param walDirsCsv     comma-separated WAL directories
   * @param restoreRootDir parent path under which temporary output dir will be created
   * @param sourceTable    source table name (for args/logging)
   * @param targetTable    target table name (for args/logging)
   * @param startTime      start time (ms) to set in the job config (WALInputFormat.START_TIME_KEY)
   * @param endTime        end time (ms) to set in the job config (WALInputFormat.END_TIME_KEY)
   * @return deduplicated list of Paths discovered by the collector
   * @throws IOException on IO or job failure
   */
  public static List<Path> collectFromWalDirs(Configuration conf, String walDirsCsv,
    Path restoreRootDir, TableName sourceTable, TableName targetTable, long startTime, long endTime)
    throws IOException {

    // prepare job Tool
    Configuration jobConf = new Configuration(conf);
    if (startTime > 0) jobConf.setLong(WALInputFormat.START_TIME_KEY, startTime);
    if (endTime > 0) jobConf.setLong(WALInputFormat.END_TIME_KEY, endTime);

    // ignore empty WAL files by default to make collection robust
    jobConf.setBoolean(WALPlayer.IGNORE_EMPTY_FILES, true);

    BulkLoadCollectorJob bulkCollector = new BulkLoadCollectorJob();
    bulkCollector.setConf(jobConf);

    return collectFromWalDirs(conf, walDirsCsv, restoreRootDir, sourceTable, targetTable,
      bulkCollector);
  }

  /**
   * Primary implementation: runs the provided Tool (BulkLoadCollectorJob) with args "<walDirsCsv>
   * <bulkFilesOut> <sourceTable> <targetTable>" and returns deduped list of Paths.
   */
  public static List<Path> collectFromWalDirs(Configuration conf, String walDirsCsv,
    Path restoreRootDir, TableName sourceTable, TableName targetTable, Tool bulkCollector)
    throws IOException {

    if (walDirsCsv == null || walDirsCsv.trim().isEmpty()) {
      throw new IOException(
        "walDirsCsv must be a non-empty comma-separated list of WAL directories");
    }

    List<String> walDirs =
      Arrays.stream(walDirsCsv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();

    if (walDirs.isEmpty()) {
      throw new IOException("walDirsCsv did not contain any entries: '" + walDirsCsv + "'");
    }

    List<String> existing = new ArrayList<>();
    for (String d : walDirs) {
      Path p = new Path(d);
      try {
        FileSystem fsForPath = p.getFileSystem(conf);
        if (fsForPath.exists(p)) {
          existing.add(d);
        } else {
          LOG.debug("WAL dir does not exist: {}", d);
        }
      } catch (IOException e) {
        // If getting FS or checking existence fails, treat as missing but log the cause.
        LOG.warn("Error checking WAL dir {}: {}", d, e.toString());
      }
    }

    // If any of the provided walDirs are missing, fail with an informative message.
    List<String> missing = new ArrayList<>(walDirs);
    missing.removeAll(existing);

    if (!missing.isEmpty()) {
      throw new IOException(
        "Some of the provided WAL paths do not exist: " + String.join(", ", missing));
    }

    // Create unique temporary output dir under restoreRootDir, e.g.
    // <restoreRootDir>/_wal_collect_<table_name><ts>
    final String unique = String.format("_wal_collect_%s%d", sourceTable.getQualifierAsString(),
      EnvironmentEdgeManager.currentTime());
    final Path bulkFilesOut = new Path(restoreRootDir, unique);

    FileSystem fs = bulkFilesOut.getFileSystem(conf);

    try {
      // If bulkFilesOut exists for some reason, delete it.
      if (fs.exists(bulkFilesOut)) {
        LOG.info("Temporary bulkload file collect output directory {} already exists - deleting.",
          bulkFilesOut);
        fs.delete(bulkFilesOut, true);
      }

      final String[] args = new String[] { walDirsCsv, bulkFilesOut.toString(),
        sourceTable.getNameAsString(), targetTable.getNameAsString() };

      LOG.info("Running bulk collector Tool with args: {}", (Object) args);

      int exitCode;
      try {
        exitCode = bulkCollector.run(args);
      } catch (Exception e) {
        LOG.error("Error during BulkLoadCollectorJob for {}: {}", sourceTable, e.getMessage(), e);
        throw new IOException("Exception during BulkLoadCollectorJob collect", e);
      }

      if (exitCode != 0) {
        throw new IOException("Bulk collector Tool returned non-zero exit code: " + exitCode);
      }

      LOG.info("BulkLoadCollectorJob collect completed successfully for {}", sourceTable);

      // read and dedupe
      List<Path> results = readBulkFilesListFromOutput(fs, bulkFilesOut);
      LOG.info("BulkFilesCollector: discovered {} unique bulk-load files", results.size());
      return results;
    } finally {
      // best-effort cleanup
      try {
        if (fs.exists(bulkFilesOut)) {
          boolean deleted = fs.delete(bulkFilesOut, true);
          if (!deleted) {
            LOG.warn("Could not delete temporary bulkFilesOut directory {}", bulkFilesOut);
          } else {
            LOG.debug("Deleted temporary bulkFilesOut directory {}", bulkFilesOut);
          }
        }
      } catch (IOException ioe) {
        LOG.warn("Exception while deleting temporary bulkload file collect output dir {}: {}",
          bulkFilesOut, ioe.getMessage(), ioe);
      }
    }
  }

  // reads all non-hidden files under bulkFilesOut, collects lines in insertion order, returns Paths
  private static List<Path> readBulkFilesListFromOutput(FileSystem fs, Path bulkFilesOut)
    throws IOException {
    if (!fs.exists(bulkFilesOut)) {
      LOG.warn("BulkFilesCollector: bulkFilesOut directory does not exist: {}", bulkFilesOut);
      return new ArrayList<>();
    }

    RemoteIterator<LocatedFileStatus> it = fs.listFiles(bulkFilesOut, true);
    Set<String> dedupe = new LinkedHashSet<>();

    while (it.hasNext()) {
      LocatedFileStatus status = it.next();
      Path p = status.getPath();
      String name = p.getName();
      // skip hidden/system files like _SUCCESS or _logs
      if (name.startsWith("_") || name.startsWith(".")) continue;

      try (FSDataInputStream in = fs.open(p);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          line = line.trim();
          if (line.isEmpty()) continue;
          dedupe.add(line);
        }
      }
    }

    List<Path> result = new ArrayList<>(dedupe.size());
    for (String s : dedupe)
      result.add(new Path(s));

    LOG.info("Collected {} unique bulk-load store files.", result.size());
    return result;
  }
}

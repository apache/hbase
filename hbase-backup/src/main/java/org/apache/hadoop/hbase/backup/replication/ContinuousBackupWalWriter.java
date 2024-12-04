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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;

/**
 * Handles writing of Write-Ahead Log (WAL) entries and tracking bulk-load files for continuous
 * backup.
 * <p>
 * This class is responsible for managing the WAL files and their associated context files. It
 * provides functionality to write WAL entries, persist backup-related metadata, and retrieve
 * bulk-load files from context files.
 * </p>
 */
@InterfaceAudience.Private
public class ContinuousBackupWalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousBackupWalWriter.class);
  public static final String WAL_FILE_PREFIX = "wal_file.";
  public static final String WAL_WRITER_CONTEXT_FILE_SUFFIX = ".context";
  private final WALProvider.Writer writer;
  private final FileSystem fileSystem;
  private final Path rootDir;
  private final Path walPath;
  private final Path walWriterContextFilePath;
  private final long initialWalFileSize;
  private final List<Path> bulkLoadFiles = new ArrayList<>();

  /**
   * Constructs a ContinuousBackupWalWriter for a specific WAL directory.
   * @param fs      the file system instance to use for file operations
   * @param rootDir the root directory for WAL files
   * @param walDir  the WAL directory path
   * @param conf    the HBase configuration object
   * @throws IOException if an error occurs during initialization
   */
  public ContinuousBackupWalWriter(FileSystem fs, Path rootDir, Path walDir, Configuration conf)
    throws IOException {
    LOG.info("Initializing ContinuousBackupWalWriter for WAL directory: {}", walDir);
    this.fileSystem = fs;
    this.rootDir = rootDir;

    // Create WAL file
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    Path walDirFullPath = new Path(rootDir, walDir);
    if (!fileSystem.exists(walDirFullPath)) {
      LOG.info("WAL directory {} does not exist. Creating it.", walDirFullPath);
      fileSystem.mkdirs(walDirFullPath);
    }
    String walFileName = WAL_FILE_PREFIX + currentTime;
    this.walPath = new Path(walDir, walFileName);
    Path walFileFullPath = new Path(walDirFullPath, walFileName);
    LOG.debug("Creating WAL file at path: {}", walFileFullPath);
    this.writer = WALFactory.createWALWriter(fileSystem, walFileFullPath, conf);
    this.initialWalFileSize = writer.getLength();

    // Create WAL Writer Context file
    String walWriterContextFileName = walPath.getName() + WAL_WRITER_CONTEXT_FILE_SUFFIX;
    this.walWriterContextFilePath = new Path(walPath.getParent(), walWriterContextFileName);
    persistWalWriterContext();

    LOG.info("ContinuousBackupWalWriter initialized successfully with WAL file: {}", walPath);
  }

  /**
   * Writes WAL entries to the WAL file and tracks associated bulk-load files.
   * @param walEntries    the list of WAL entries to write
   * @param bulkLoadFiles the list of bulk-load files to track
   * @throws IOException if an error occurs during writing
   */
  public void write(List<WAL.Entry> walEntries, List<Path> bulkLoadFiles) throws IOException {
    LOG.debug("Writing {} WAL entries to WAL file: {}", walEntries.size(), walPath);
    for (WAL.Entry entry : walEntries) {
      writer.append(entry);
    }

    writer.sync(true); // Ensure data is flushed to disk
    this.bulkLoadFiles.addAll(bulkLoadFiles);
    persistWalWriterContext();
  }

  /**
   * Returns the current size of the WAL file.
   */
  public long getLength() {
    return writer.getLength();
  }

  /**
   * Closes the WAL writer, ensuring all resources are released.
   * @throws IOException if an error occurs during closure
   */
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  /**
   * Checks if the WAL file has any entries written to it.
   * @return {@code true} if the WAL file contains entries; {@code false} otherwise
   */
  public boolean hasAnyEntry() {
    return writer.getLength() > initialWalFileSize;
  }

  private void persistWalWriterContext() throws IOException {
    LOG.debug("Persisting WAL writer context for file: {}", walWriterContextFilePath);
    BackupProtos.ContinuousBackupWalWriterContext.Builder protoBuilder =
      BackupProtos.ContinuousBackupWalWriterContext.newBuilder().setWalPath(walPath.toString())
        .setInitialWalFileSize(this.initialWalFileSize);

    for (Path bulkLoadFile : bulkLoadFiles) {
      protoBuilder.addBulkLoadFiles(bulkLoadFile.toString());
    }

    Path walWriterContextFileFullPath = new Path(rootDir, walWriterContextFilePath);
    try (FSDataOutputStream outputStream = fileSystem.create(walWriterContextFileFullPath, true)) {
      if (!fileSystem.exists(walWriterContextFileFullPath)) {
        LOG.error("Failed to create context file: {}", walWriterContextFileFullPath);
        throw new IOException("Context file creation failed.");
      }
      protoBuilder.build().writeTo(outputStream);
      outputStream.flush();
      LOG.info("Successfully persisted WAL writer context for file: {}",
        walWriterContextFileFullPath);
    }
  }

  /**
   * Checks if the specified file is the WAL file being written by this writer.
   * @param filePath the path of the file to check
   * @return {@code true} if the specified file is being written by this writer; {@code false}
   *         otherwise
   */
  public boolean isWritingToFile(Path filePath) {
    return filePath.equals(new Path(rootDir, walPath));
  }

  /**
   * Determines if a file name corresponds to a WAL writer context file.
   * @param fileName the name of the file to check
   * @return {@code true} if the file is a WAL writer context file; {@code false} otherwise
   */
  public static boolean isWalWriterContextFile(String fileName) {
    return fileName.contains(WAL_WRITER_CONTEXT_FILE_SUFFIX);
  }

  /**
   * Retrieves bulk-load files from a WAL writer context proto file.
   * @param fs            the file system instance
   * @param protoFilePath the path to the proto file
   * @return a list of paths for the bulk-load files
   * @throws IOException if an error occurs during retrieval
   */
  public static List<Path> getBulkloadFilesFromProto(FileSystem fs, Path protoFilePath)
    throws IOException {
    LOG.debug("Retrieving bulk load files from proto file: {}", protoFilePath);
    List<Path> bulkloadFiles = new ArrayList<>();
    try (FSDataInputStream inputStream = fs.open(protoFilePath)) {
      BackupProtos.ContinuousBackupWalWriterContext proto =
        BackupProtos.ContinuousBackupWalWriterContext.parseFrom(inputStream);
      for (String bulkLoadFile : proto.getBulkLoadFilesList()) {
        bulkloadFiles.add(new Path(bulkLoadFile));
      }
      LOG.info("Retrieved {} bulk load files from proto file: {}", bulkloadFiles.size(),
        protoFilePath);
    }
    return bulkloadFiles;
  }
}

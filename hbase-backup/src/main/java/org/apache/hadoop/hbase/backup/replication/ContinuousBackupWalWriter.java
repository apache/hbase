package org.apache.hadoop.hbase.backup.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  public ContinuousBackupWalWriter(FileSystem fs, Path rootDir, Path walDir, Configuration conf) throws
    IOException {
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

  public void writer(List<WAL.Entry> walEntries, List<Path> bulkLoadFiles) throws IOException {
    LOG.debug("Writing {} WAL entries to WAL file: {}", walEntries.size(), walPath);
    for (WAL.Entry entry : walEntries) {
      writer.append(entry);
    }

    writer.sync(true); // Ensure data is flushed to disk
    this.bulkLoadFiles.addAll(bulkLoadFiles);
    persistWalWriterContext();
  }

  public long getLength() {
    return writer.getLength();
  }

  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  public boolean hasAnyEntry() {
    return writer.getLength() > initialWalFileSize;
  }

  private void persistWalWriterContext() throws IOException {
    LOG.debug("Persisting WAL writer context for file: {}", walWriterContextFilePath);
    BackupProtos.ContinuousBackupWalWriterContext.Builder protoBuilder =
      BackupProtos.ContinuousBackupWalWriterContext.newBuilder()
      .setWalPath(walPath.toString())
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
      LOG.info("Successfully persisted WAL writer context for file: {}", walWriterContextFileFullPath);
    }
  }

  public boolean isWritingToFile(Path filePath) {
    return filePath.equals(new Path(rootDir, walPath));
  }

  public static boolean isWalWriterContextFile(String fileName) {
    return fileName.contains(WAL_WRITER_CONTEXT_FILE_SUFFIX);
  }

  public static List<Path> getBulkloadFilesFromProto(FileSystem fs, Path protoFilePath) throws IOException {
    LOG.debug("Retrieving bulk load files from proto file: {}", protoFilePath);
    List<Path> bulkloadFiles = new ArrayList<>();
    try (FSDataInputStream inputStream = fs.open(protoFilePath)) {
      BackupProtos.ContinuousBackupWalWriterContext proto =
        BackupProtos.ContinuousBackupWalWriterContext.parseFrom(inputStream);
      for (String bulkLoadFile : proto.getBulkLoadFilesList()) {
        bulkloadFiles.add(new Path(bulkLoadFile));
      }
      LOG.info("Retrieved {} bulk load files from proto file: {}", bulkloadFiles.size(), protoFilePath);
    }
    return bulkloadFiles;
  }
}

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
package org.apache.hadoop.hbase.io.hfile.cache.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CachePersistenceStorage} implementation backed by a Hadoop {@link FileSystem}.
 * <p>
 * This implementation works with filesystems available through Hadoop's filesystem abstraction,
 * including HDFS and the local filesystem.
 * </p>
 * <p>
 * Every persistence storage key is represented as a file below a configured root directory. Keys
 * may contain relative path components separated by {@code /}, allowing higher-level persistence
 * infrastructure to organize state by component role.
 * </p>
 * <p>
 * Persistence writes are first written to a temporary file. The temporary file is promoted to the
 * committed state path only after the caller explicitly commits the write. An abandoned write
 * deletes its temporary file and leaves previously committed state unchanged.
 * </p>
 * <p>
 * Publication uses filesystem rename operations. Rename atomicity and implementation details are
 * determined by the underlying Hadoop filesystem. For filesystems where rename is implemented as
 * copy-and-delete, publication has the corresponding filesystem semantics.
 * </p>
 * <p>
 * This class does not own the supplied {@link FileSystem} and does not close it.
 * </p>
 */
@InterfaceAudience.Private
public class HadoopFsCachePersistenceStorage implements CachePersistenceStorage {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFsCachePersistenceStorage.class);
  private static final String STATE_FILE_SUFFIX = ".state";
  private static final String TEMP_FILE_MARKER = ".tmp-";
  private static final String BACKUP_FILE_SUFFIX = ".backup";

  private final FileSystem fileSystem;
  private final Path rootPath;

  /**
   * Creates persistence storage using the filesystem associated with the supplied root path.
   * @param conf     Hadoop configuration used to resolve the filesystem
   * @param rootPath root directory under which persisted cache state is stored
   * @throws IOException if the filesystem for the root path cannot be resolved
   */
  public HadoopFsCachePersistenceStorage(Configuration conf, Path rootPath) throws IOException {
    this(resolveFileSystem(conf, rootPath), rootPath);
  }

  /**
   * Creates persistence storage using an explicitly supplied Hadoop filesystem.
   * <p>
   * The supplied filesystem remains owned by the caller and is not closed by this storage instance.
   * </p>
   * @param fileSystem Hadoop filesystem used to store cache state
   * @param rootPath   root directory under which persisted cache state is stored
   */
  public HadoopFsCachePersistenceStorage(FileSystem fileSystem, Path rootPath) {
    this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null");
    this.rootPath = Objects.requireNonNull(rootPath, "rootPath must not be null");
  }

  /**
   * Opens committed persisted state for the specified storage key.
   * @param key stable storage key assigned to a persistent cache component
   * @return input stream containing committed state, or an empty optional if no state exists
   * @throws IOException if committed state exists but cannot be opened
   */
  @Override
  public Optional<InputStream> open(String key) throws IOException {
    Path path = getStatePath(key);
    recoverCommittedState(path);

    if (!fileSystem.exists(path)) {
      return Optional.empty();
    }
    return Optional.of(fileSystem.open(path));
  }

  /**
   * Starts a new persistence write for the specified storage key.
   * <p>
   * Data is written to a temporary file located next to the final state file. The temporary file
   * becomes committed state only when the returned handle is explicitly committed.
   * </p>
   * @param key stable storage key assigned to a persistent cache component
   * @return handle for writing and committing persisted state
   * @throws IOException if the temporary persistence file cannot be created
   */
  @Override
  public CachePersistenceOutput create(String key) throws IOException {
    Path targetPath = getStatePath(key);
    Path parent = targetPath.getParent();
    ensureDirectory(parent);
    recoverCommittedState(targetPath);

    Path temporaryPath = createTemporaryPath(targetPath);
    FSDataOutputStream output = fileSystem.create(temporaryPath, false);

    return new HadoopFsCachePersistenceOutput(fileSystem, output, temporaryPath, targetPath);
  }

  /**
   * Creates the parent directory for a persistence state file when necessary.
   * @param directory directory to create
   * @throws IOException if the directory cannot be created
   */
  private void ensureDirectory(Path directory) throws IOException {
    if (directory == null || fileSystem.exists(directory)) {
      return;
    }

    if (!fileSystem.mkdirs(directory) && !fileSystem.exists(directory)) {
      throw new IOException("Failed to create cache persistence directory " + directory);
    }
  }

  /**
   * Creates a unique temporary path adjacent to the specified committed state path.
   * @param targetPath committed state path
   * @return unique temporary path
   */
  private Path createTemporaryPath(Path targetPath) {
    return new Path(targetPath.toString() + TEMP_FILE_MARKER + UUID.randomUUID());
  }

  /**
   * Resolves the Hadoop filesystem associated with the supplied persistence root path.
   * @param conf     Hadoop configuration used to resolve the filesystem
   * @param rootPath persistence root path
   * @return filesystem associated with the root path
   * @throws IOException if the filesystem cannot be resolved
   */
  private static FileSystem resolveFileSystem(Configuration conf, Path rootPath)
    throws IOException {
    Objects.requireNonNull(conf, "conf must not be null");
    Objects.requireNonNull(rootPath, "rootPath must not be null");
    return rootPath.getFileSystem(conf);
  }

  /**
   * Returns the filesystem path containing committed state for the supplied persistence key.
   * @param key persistence storage key
   * @return filesystem path containing committed persisted state
   */
  private Path getStatePath(String key) {
    validateKey(key);
    return new Path(rootPath, key + STATE_FILE_SUFFIX);
  }

  /**
   * Validates a persistence storage key before using it as a relative filesystem path.
   * <p>
   * Storage keys must be relative and may contain multiple path segments. Empty segments and the
   * special {@code .} and {@code ..} path segments are not allowed.
   * </p>
   * @param key persistence storage key to validate
   * @throws NullPointerException     if the key is {@code null}
   * @throws IllegalArgumentException if the key is empty or contains an invalid path component
   */
  private void validateKey(String key) {
    Objects.requireNonNull(key, "key must not be null");

    if (key.isEmpty()) {
      throw new IllegalArgumentException("key must not be empty");
    }
    if (
      key.startsWith("/") || key.endsWith("/") || key.indexOf('\\') >= 0 || key.contains("://")
        || new Path(key + STATE_FILE_SUFFIX).toUri().getScheme() != null
    ) {
      throw new IllegalArgumentException("Invalid persistence key: " + key);
    }

    String[] components = key.split("/");
    for (String component : components) {
      if (component.isEmpty() || ".".equals(component) || "..".equals(component)) {
        throw new IllegalArgumentException("Invalid persistence key: " + key);
      }
    }
  }

  /**
   * Recovers previously committed state when publication was interrupted after the committed state
   * was moved to its backup location.
   * <p>
   * If the normal state file exists, no recovery is required. If it is absent and a backup exists,
   * the backup represents the last successfully committed state and is restored to the normal state
   * path.
   * </p>
   * @param targetPath committed persistence state path
   * @throws IOException if previously committed state cannot be recovered
   */
  private void recoverCommittedState(Path targetPath) throws IOException {
    if (fileSystem.exists(targetPath)) {
      return;
    }

    Path backupPath = getBackupPath(targetPath);
    if (!fileSystem.exists(backupPath)) {
      return;
    }

    if (!fileSystem.rename(backupPath, targetPath)) {
      throw new IOException(
        "Failed to recover committed cache persistence state " + backupPath + " to " + targetPath);
    }
  }

  /**
   * Returns the backup path used while replacing committed persistence state.
   * @param targetPath committed persistence state path
   * @return backup path for the committed state
   */
  private static Path getBackupPath(Path targetPath) {
    return new Path(targetPath.toString() + BACKUP_FILE_SUFFIX);
  }

  /**
   * Persistence output backed by a temporary Hadoop filesystem file.
   * <p>
   * A successful commit closes the temporary output stream and publishes the temporary file as the
   * committed state file. Closing an uncommitted instance aborts the write.
   * </p>
   */
  private static final class HadoopFsCachePersistenceOutput implements CachePersistenceOutput {

    private final FileSystem fileSystem;
    private final FSDataOutputStream output;
    private final Path temporaryPath;
    private final Path targetPath;

    private boolean streamClosed;
    private boolean committed;
    private boolean aborted;

    /**
     * Creates a persistence output backed by a temporary filesystem file.
     * @param fileSystem    filesystem containing the temporary and target files
     * @param output        output stream writing the temporary file
     * @param temporaryPath temporary persistence file
     * @param targetPath    committed persistence file
     */
    private HadoopFsCachePersistenceOutput(FileSystem fileSystem, FSDataOutputStream output,
      Path temporaryPath, Path targetPath) {
      this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null");
      this.output = Objects.requireNonNull(output, "output must not be null");
      this.temporaryPath = Objects.requireNonNull(temporaryPath, "temporaryPath must not be null");
      this.targetPath = Objects.requireNonNull(targetPath, "targetPath must not be null");
    }

    /**
     * Returns the stream used to write temporary persisted state.
     * @return persistence output stream
     */
    @Override
    public OutputStream getOutputStream() {
      return output;
    }

    /**
     * Commits the persistence write by publishing the completed temporary file.
     * <p>
     * If previously committed state exists, it is moved temporarily out of the way before the new
     * state is published. If publication of the new state fails, this method attempts to restore
     * the previous committed state.
     * </p>
     * @throws IOException if the output cannot be closed or the new state cannot be published
     */
    @Override
    public void commit() throws IOException {
      ensureActive();

      closeOutput();

      Path backupPath = getBackupPath(targetPath);
      boolean previousStateMoved = false;

      try {
        if (fileSystem.exists(targetPath)) {
          if (fileSystem.exists(backupPath) && !fileSystem.delete(backupPath, false)) {
            throw new IOException("Failed to delete stale cache persistence backup " + backupPath);
          }

          if (!fileSystem.rename(targetPath, backupPath)) {
            throw new IOException(
              "Failed to preserve existing cache persistence state " + targetPath);
          }
          previousStateMoved = true;
        }

        if (!fileSystem.rename(temporaryPath, targetPath)) {
          throw new IOException(
            "Failed to publish cache persistence state " + temporaryPath + " to " + targetPath);
        }

        committed = true;

        if (previousStateMoved) {
          deleteBackupAfterCommit(backupPath);
        }

      } catch (IOException error) {
        if (!committed && previousStateMoved) {
          restorePreviousState(backupPath, error);
        }
        throw error;
      }
    }

    /**
     * Removes the previous committed-state backup after a new state has been successfully
     * published.
     * <p>
     * Backup cleanup is best-effort. Once the new state has been published, failure to remove the
     * obsolete backup must not cause the persistence operation to be reported as failed.
     * </p>
     * @param backupPath backup containing the previously committed state
     */
    private void deleteBackupAfterCommit(Path backupPath) {
      try {
        if (fileSystem.exists(backupPath) && !fileSystem.delete(backupPath, false)) {
          LOG.warn("Failed to delete obsolete cache persistence backup {}", backupPath);
        }
      } catch (IOException error) {
        LOG.warn("Failed to delete obsolete cache persistence backup {}", backupPath, error);
      }
    }

    /**
     * Aborts the persistence write and removes its temporary file.
     * <p>
     * Calling this method after a successful commit has no effect.
     * </p>
     * @throws IOException if the output cannot be closed or the temporary file cannot be removed
     */
    @Override
    public void abort() throws IOException {
      if (committed || aborted) {
        return;
      }

      IOException failure = null;

      try {
        closeOutput();
      } catch (IOException error) {
        failure = error;
      }

      try {
        deleteTemporaryFile();
      } catch (IOException error) {
        if (failure == null) {
          failure = error;
        } else {
          failure.addSuppressed(error);
        }
      }

      aborted = true;

      if (failure != null) {
        throw failure;
      }
    }

    /**
     * Closes this persistence output.
     * <p>
     * An uncommitted output is aborted. A successfully committed or previously aborted output
     * requires no further action.
     * </p>
     * @throws IOException if an uncommitted write cannot be aborted
     */
    @Override
    public void close() throws IOException {
      if (!committed && !aborted) {
        abort();
      }
    }

    /**
     * Verifies that this persistence output is still available for commit.
     * @throws IllegalStateException if this output has already been committed or aborted
     */
    private void ensureActive() {
      if (committed) {
        throw new IllegalStateException("Persistence output has already been committed");
      }
      if (aborted) {
        throw new IllegalStateException("Persistence output has already been aborted");
      }
    }

    /**
     * Closes the temporary file output stream if it is still open.
     * @throws IOException if the output stream cannot be closed
     */
    private void closeOutput() throws IOException {
      if (!streamClosed) {
        output.close();
        streamClosed = true;
      }
    }

    /**
     * Removes the temporary persistence file if it still exists.
     * @throws IOException if the temporary file exists but cannot be removed
     */
    private void deleteTemporaryFile() throws IOException {
      if (fileSystem.exists(temporaryPath) && !fileSystem.delete(temporaryPath, false)) {
        throw new IOException(
          "Failed to delete temporary cache persistence state " + temporaryPath);
      }
    }

    /**
     * Attempts to restore the previous committed state after publication of new state fails.
     * <p>
     * A rollback failure is added as a suppressed exception to the original publication failure.
     * </p>
     * @param backupPath      path containing the previous committed state
     * @param originalFailure original publication failure
     */
    private void restorePreviousState(Path backupPath, IOException originalFailure) {
      if (backupPath == null) {
        return;
      }

      try {
        if (fileSystem.exists(targetPath)) {
          fileSystem.delete(targetPath, false);
        }

        if (!fileSystem.rename(backupPath, targetPath)) {
          originalFailure.addSuppressed(
            new IOException("Failed to restore previous cache persistence state " + backupPath));
        }
      } catch (IOException rollbackFailure) {
        originalFailure.addSuppressed(rollbackFailure);
      }
    }
  }
}

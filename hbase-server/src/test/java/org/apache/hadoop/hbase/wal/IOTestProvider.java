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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import static org.apache.hadoop.hbase.wal.DefaultWALProvider.DEFAULT_PROVIDER_ID;
import static org.apache.hadoop.hbase.wal.DefaultWALProvider.META_WAL_PROVIDER_ID;
import static org.apache.hadoop.hbase.wal.DefaultWALProvider.WAL_FILE_NAME_DELIMITER;


// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;

/**
 * A WAL Provider that returns a single thread safe WAL that optionally can skip parts of our
 * normal interactions with HDFS.
 *
 * This implementation picks a directory in HDFS based on the same mechanisms as the 
 * {@link DefaultWALProvider}. Users can configure how much interaction
 * we have with HDFS with the configuration property "hbase.wal.iotestprovider.operations".
 * The value should be a comma separated list of allowed operations:
 * <ul>
 *   <li><em>append</em>   : edits will be written to the underlying filesystem
 *   <li><em>sync</em>     : wal syncs will result in hflush calls
 *   <li><em>fileroll</em> : roll requests will result in creating a new file on the underlying
 *                           filesystem.
 * </ul>
 * Additionally, the special cases "all" and "none" are recognized.
 * If ommited, the value defaults to "all."
 * Behavior is undefined if "all" or "none" are paired with additional values. Behavior is also
 * undefined if values not listed above are included.
 *
 * Only those operations listed will occur between the returned WAL and HDFS. All others
 * will be no-ops.
 *
 * Note that in the case of allowing "append" operations but not allowing "fileroll", the returned
 * WAL will just keep writing to the same file. This won't avoid all costs associated with file
 * management over time, becaue the data set size may result in additional HDFS block allocations.
 *
 */
@InterfaceAudience.Private
public class IOTestProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(IOTestProvider.class);

  private static final String ALLOWED_OPERATIONS = "hbase.wal.iotestprovider.operations";
  private enum AllowedOperations {
    all,
    append,
    sync,
    fileroll,
    none;
  }

  private FSHLog log = null;

  /**
   * @param factory factory that made us, identity used for FS layout. may not be null
   * @param conf may not be null
   * @param listeners may be null
   * @param providerId differentiate between providers from one facotry, used for FS layout. may be
   *                   null
   */
  @Override
  public void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, String providerId) throws IOException {
    if (null != log) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    if (null == providerId) {
      providerId = DEFAULT_PROVIDER_ID;
    }
    final String logPrefix = factory.factoryId + WAL_FILE_NAME_DELIMITER + providerId;
    log = new IOTestWAL(FileSystem.get(conf), FSUtils.getRootDir(conf),
        DefaultWALProvider.getWALDirectoryName(factory.factoryId),
        HConstants.HREGION_OLDLOGDIR_NAME, conf, listeners,
        true, logPrefix, META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
  }

  @Override
  public WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException {
   return log;
  }

  @Override
  public void close() throws IOException {
    log.close();
  }

  @Override
  public void shutdown() throws IOException {
    log.shutdown();
  }

  private static class IOTestWAL extends FSHLog {

    private final boolean doFileRolls;

    // Used to differntiate between roll calls before and after we finish construction.
    private final boolean initialized;

    /**
     * Create an edit log at the given <code>dir</code> location.
     *
     * You should never have to load an existing log. If there is a log at
     * startup, it should have already been processed and deleted by the time the
     * WAL object is started up.
     *
     * @param fs filesystem handle
     * @param rootDir path to where logs and oldlogs
     * @param logDir dir where wals are stored
     * @param archiveDir dir where wals are archived
     * @param conf configuration to use
     * @param listeners Listeners on WAL events. Listeners passed here will
     * be registered before we do anything else; e.g. the
     * Constructor {@link #rollWriter()}.
     * @param failIfWALExists If true IOException will be thrown if files related to this wal
     *        already exist.
     * @param prefix should always be hostname and port in distributed env and
     *        it will be URL encoded before being used.
     *        If prefix is null, "wal" will be used
     * @param suffix will be url encoded. null is treated as empty. non-empty must start with
     *        {@link DefaultWALProvider#WAL_FILE_NAME_DELIMITER}
     * @throws IOException
     */
    public IOTestWAL(final FileSystem fs, final Path rootDir, final String logDir,
        final String archiveDir, final Configuration conf,
        final List<WALActionsListener> listeners,
        final boolean failIfWALExists, final String prefix, final String suffix)
        throws IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
      Collection<String> operations = conf.getStringCollection(ALLOWED_OPERATIONS);
      doFileRolls = operations.isEmpty() || operations.contains(AllowedOperations.all.name()) ||
          operations.contains(AllowedOperations.fileroll.name());
      initialized = true;
      LOG.info("Initialized with file rolling " + (doFileRolls ? "enabled" : "disabled"));
    }

    private Writer noRollsWriter;

    // creatWriterInstance is where the new pipeline is set up for doing file rolls
    // if we are skipping it, just keep returning the same writer.
    @Override
    protected Writer createWriterInstance(final Path path) throws IOException {
      // we get called from the FSHLog constructor (!); always roll in this case since
      // we don't know yet if we're supposed to generally roll and
      // we need an initial file in the case of doing appends but no rolls.
      if (!initialized || doFileRolls) {
        LOG.info("creating new writer instance.");
        final ProtobufLogWriter writer = new IOTestWriter();
        writer.init(fs, path, conf, false);
        if (!initialized) {
          LOG.info("storing initial writer instance in case file rolling isn't allowed.");
          noRollsWriter = writer;
        }
        return writer;
      } else {
        LOG.info("WAL rolling disabled, returning the first writer.");
        // Initial assignment happens during the constructor call, so there ought not be
        // a race for first assignment.
        return noRollsWriter;
      }
    }
  }

  /**
   * Presumes init will be called by a single thread prior to any access of other methods.
   */
  private static class IOTestWriter extends ProtobufLogWriter {
    private boolean doAppends;
    private boolean doSyncs;

    @Override
    public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable) throws IOException {
      Collection<String> operations = conf.getStringCollection(ALLOWED_OPERATIONS);
      if (operations.isEmpty() || operations.contains(AllowedOperations.all.name())) {
        doAppends = doSyncs = true;
      } else if (operations.contains(AllowedOperations.none.name())) {
        doAppends = doSyncs = false;
      } else {
        doAppends = operations.contains(AllowedOperations.append.name());
        doSyncs = operations.contains(AllowedOperations.sync.name());
      }
      LOG.info("IOTestWriter initialized with appends " + (doAppends ? "enabled" : "disabled") +
          " and syncs " + (doSyncs ? "enabled" : "disabled"));
      super.init(fs, path, conf, overwritable);
    }

    @Override
    protected String getWriterClassName() {
      return ProtobufLogWriter.class.getSimpleName();
    }

    @Override
    public void append(Entry entry) throws IOException {
      if (doAppends) {
        super.append(entry);
      }
    }

    @Override
    public void sync() throws IOException {
      if (doSyncs) {
        super.sync();
      }
    }
  }

  @Override
  public long getNumLogFiles() {
    return this.log.getNumLogFiles();
  }

  @Override
  public long getLogFileSize() {
    return this.log.getLogFileSize();
  }
}

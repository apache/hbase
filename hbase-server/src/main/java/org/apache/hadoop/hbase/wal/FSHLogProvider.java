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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A WAL provider that use {@link FSHLog}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSHLogProvider extends AbstractFSWALProvider<FSHLog> {

  private static final Logger LOG = LoggerFactory.getLogger(FSHLogProvider.class);

  // Only public so classes back in regionserver.wal can access
  public interface Writer extends WALProvider.Writer {
    /**
     * @throws IOException if something goes wrong initializing an output stream
     * @throws StreamLacksCapabilityException if the given FileSystem can't provide streams that
     *         meet the needs of the given Writer implementation.
     */
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable, long blocksize)
        throws IOException, CommonFSUtils.StreamLacksCapabilityException;
  }

  /**
   * Public because of FSHLog. Should be package-private
   * @param overwritable if the created writer can overwrite. For recovered edits, it is true and
   *          for WAL it is false. Thus we can distinguish WAL and recovered edits by this.
   */
  public static Writer createWriter(final Configuration conf, final FileSystem fs, final Path path,
      final boolean overwritable) throws IOException {
    return createWriter(conf, fs, path, overwritable,
      WALUtil.getWALBlockSize(conf, fs, path, overwritable));
  }

  /**
   * Public because of FSHLog. Should be package-private
   */
  public static Writer createWriter(final Configuration conf, final FileSystem fs, final Path path,
    final boolean overwritable, long blocksize) throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends Writer> logWriterClass =
        conf.getClass("hbase.regionserver.hlog.writer.impl", ProtobufLogWriter.class,
            Writer.class);
    Writer writer = null;
    try {
      writer = logWriterClass.getDeclaredConstructor().newInstance();
      FileSystem rootFs = FileSystem.get(path.toUri(), conf);
      writer.init(rootFs, path, conf, overwritable, blocksize);
      return writer;
    } catch (Exception e) { 
      if (e instanceof CommonFSUtils.StreamLacksCapabilityException) {
        LOG.error("The RegionServer write ahead log provider for FileSystem implementations " +
            "relies on the ability to call " + e.getMessage() + " for proper operation during " +
            "component failures, but the current FileSystem does not support doing so. Please " +
            "check the config value of '" + CommonFSUtils.HBASE_WAL_DIR + "' and ensure " +
            "it points to a FileSystem mount that has suitable capabilities for output streams.");
      } else {
        LOG.debug("Error instantiating log writer.", e);
      }
      throw new IOException("cannot get log writer", e);
    }
  }

  @Override
  protected FSHLog createWAL() throws IOException {
    return new FSHLog(CommonFSUtils.getWALFileSystem(conf), abortable,
        CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.factoryId),
        getWALArchiveDirectoryName(conf, factory.factoryId), conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
  }

  @Override
  protected void doInit(Configuration conf) throws IOException {
  }
}

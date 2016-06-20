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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A WAL provider that use {@link FSHLog}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSHLogProvider extends AbstractFSWALProvider<FSHLog> {

  private static final Log LOG = LogFactory.getLog(FSHLogProvider.class);

  // Only public so classes back in regionserver.wal can access
  public interface Writer extends WALProvider.Writer {
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable) throws IOException;
  }

  /**
   * public because of FSHLog. Should be package-private
   */
  public static Writer createWriter(final Configuration conf, final FileSystem fs, final Path path,
      final boolean overwritable) throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends Writer> logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
      ProtobufLogWriter.class, Writer.class);
    Writer writer = null;
    try {
      writer = logWriterClass.newInstance();
      writer.init(fs, path, conf, overwritable);
      return writer;
    } catch (Exception e) { 
      LOG.debug("Error instantiating log writer.", e);            
      if (writer != null) {
        try{
          writer.close();
        } catch(IOException ee){
          LOG.error("cannot close log writer", ee);
        }
      }
      throw new IOException("cannot get log writer", e);
    }
  }

  @Override
  protected FSHLog createWAL() throws IOException {
    return new FSHLog(FileSystem.get(conf), FSUtils.getRootDir(conf),
        getWALDirectoryName(factory.factoryId), HConstants.HREGION_OLDLOGDIR_NAME, conf, listeners,
        true, logPrefix, META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
  }

  @Override
  protected void doInit(Configuration conf) throws IOException {
  }
}

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


package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;

public class HLogFactory {
    private static final Log LOG = LogFactory.getLog(HLogFactory.class);
    
    public static HLog createHLog(final FileSystem fs, final Path root, final String logName,
        final Configuration conf) throws IOException {
      return new FSHLog(fs, root, logName, conf);
    }
    
    public static HLog createHLog(final FileSystem fs, final Path root, final String logName,
        final String oldLogName, final Configuration conf) throws IOException {
      return new FSHLog(fs, root, logName, oldLogName, conf);
}
    
    public static HLog createHLog(final FileSystem fs, final Path root, final String logName,
        final Configuration conf, final List<WALActionsListener> listeners,
        final String prefix) throws IOException {
      return new FSHLog(fs, root, logName, conf, listeners, prefix);
    }
    
    /*
     * WAL Reader
     */
    
    private static Class<? extends Reader> logReaderClass;
    
    static void resetLogReaderClass() {
      logReaderClass = null;
    }
    
    /**
     * Create a reader for the WAL.
     * @return A WAL reader.  Close when done with it.
     * @throws IOException
     */
    public static HLog.Reader createReader(final FileSystem fs,
        final Path path, Configuration conf)
    throws IOException {
      try {

        if (logReaderClass == null) {

          logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
              SequenceFileLogReader.class, Reader.class);
        }


        HLog.Reader reader = logReaderClass.newInstance();
        reader.init(fs, path, conf);
        return reader;
      } catch (IOException e) {
        throw e;
      }
      catch (Exception e) {
        throw new IOException("Cannot get log reader", e);
      }
    }
    
    /*
     * WAL writer
     */
    
    private static Class<? extends Writer> logWriterClass;
    
    /**
     * Create a writer for the WAL.
     * @return A WAL writer.  Close when done with it.
     * @throws IOException
     */
    public static HLog.Writer createWriter(final FileSystem fs,
        final Path path, Configuration conf)
    throws IOException {
      try {
        if (logWriterClass == null) {
          logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
              SequenceFileLogWriter.class, Writer.class);
        }
        HLog.Writer writer = (HLog.Writer) logWriterClass.newInstance();
        writer.init(fs, path, conf);
        return writer;
      } catch (Exception e) {
        throw new IOException("cannot get log writer", e);
      }
    }
    
}

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
import java.io.InterruptedIOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

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

    public static HLog createMetaHLog(final FileSystem fs, final Path root, final String logName,
        final Configuration conf, final List<WALActionsListener> listeners,
        final String prefix) throws IOException {
      return new FSHLog(fs, root, logName, HConstants.HREGION_OLDLOGDIR_NAME, 
            conf, listeners, false, prefix, true);
    }
    
    /*
     * WAL Reader
     */
    
    private static Class<? extends Reader> logReaderClass;
    
    static void resetLogReaderClass() {
      logReaderClass = null;
    }

    /**
     * Create a reader for the WAL. If you are reading from a file that's being written to
     * and need to reopen it multiple times, use {@link HLog.Reader#reset()} instead of this method
     * then just seek back to the last known good position.
     * @return A WAL reader.  Close when done with it.
     * @throws IOException
     */
    public static HLog.Reader createReader(final FileSystem fs,
        final Path path, Configuration conf) throws IOException {
      if (logReaderClass == null) {
        logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
          SequenceFileLogReader.class, Reader.class);
      }

      try {
        // A hlog file could be under recovery, so it may take several
        // tries to get it open. Instead of claiming it is corrupted, retry
        // to open it up to 5 minutes by default.
        long startWaiting = EnvironmentEdgeManager.currentTimeMillis();
        long openTimeout = conf.getInt("hbase.hlog.open.timeout", 300000) + startWaiting;
        int nbAttempt = 0;
        while (true) {
          try {
            HLog.Reader reader = logReaderClass.newInstance();
            reader.init(fs, path, conf);
            return reader;
          } catch (IOException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("Cannot obtain block length")) {
              if (++nbAttempt == 1) {
                LOG.warn("Lease should have recovered. This is not expected. Will retry", e);
              }
              if (nbAttempt > 2 && openTimeout < EnvironmentEdgeManager.currentTimeMillis()) {
                LOG.error("Can't open after " + nbAttempt + " attempts and "
                  + (EnvironmentEdgeManager.currentTimeMillis() - startWaiting)
                  + "ms " + " for " + path);
              } else {
                try {
                  Thread.sleep(nbAttempt < 3 ? 500 : 1000);
                  continue; // retry
                } catch (InterruptedException ie) {
                  InterruptedIOException iioe = new InterruptedIOException();
                  iioe.initCause(ie);
                  throw iioe;
                }
              }
            }
            throw e;
          }
        }
      } catch (IOException ie) {
        throw ie;
      } catch (Exception e) {
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

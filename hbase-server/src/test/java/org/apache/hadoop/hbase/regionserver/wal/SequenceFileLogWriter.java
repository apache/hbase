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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

/**
 * Implementation of {@link WALProvider.Writer} that delegates to
 * SequenceFile.Writer. Legacy implementation only used for compat tests.
 *
 * Note that because this class writes to the legacy hadoop-specific SequenceFile
 * format, users of it must write {@link HLogKey} keys and not arbitrary
 * {@link WALKey}s because the latter are not Writables (nor made to work with
 * Hadoop serialization).
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SequenceFileLogWriter extends WriterBase {
  private static final Log LOG = LogFactory.getLog(SequenceFileLogWriter.class);
  // The sequence file we delegate to.
  private SequenceFile.Writer writer;
  // This is the FSDataOutputStream instance that is the 'out' instance
  // in the SequenceFile.Writer 'writer' instance above.
  private FSDataOutputStream writer_out;

  // Legacy stuff from pre-PB WAL metadata.
  private static final Text WAL_VERSION_KEY = new Text("version");
  private static final Text WAL_COMPRESSION_TYPE_KEY = new Text("compression.type");
  private static final Text DICTIONARY_COMPRESSION_TYPE = new Text("dictionary");

  /**
   * Default constructor.
   */
  public SequenceFileLogWriter() {
    super();
  }
  /**
   * Create sequence file Metadata for our WAL file with version and compression
   * type (if any).
   * @param conf
   * @param compress
   * @return Metadata instance.
   */
  private static Metadata createMetadata(final Configuration conf,
      final boolean compress) {
    TreeMap<Text, Text> metaMap = new TreeMap<Text, Text>();
    metaMap.put(WAL_VERSION_KEY, new Text("1"));
    if (compress) {
      // Currently we only do one compression type.
      metaMap.put(WAL_COMPRESSION_TYPE_KEY, DICTIONARY_COMPRESSION_TYPE);
    }
    return new Metadata(metaMap);
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
  throws IOException {
    super.init(fs, path, conf, overwritable);
    boolean compress = initializeCompressionContext(conf, path);
    // Create a SF.Writer instance.
    try {
      // reflection for a version of SequenceFile.createWriter that doesn't
      // automatically create the parent directory (see HBASE-2312)
      this.writer = (SequenceFile.Writer) SequenceFile.class
        .getMethod("createWriter", new Class[] {FileSystem.class,
            Configuration.class, Path.class, Class.class, Class.class,
            Integer.TYPE, Short.TYPE, Long.TYPE, Boolean.TYPE,
            CompressionType.class, CompressionCodec.class, Metadata.class})
        .invoke(null, new Object[] {fs, conf, path, HLogKey.class, WALEdit.class,
            Integer.valueOf(FSUtils.getDefaultBufferSize(fs)),
            Short.valueOf((short)
              conf.getInt("hbase.regionserver.hlog.replication",
              FSUtils.getDefaultReplication(fs, path))),
            Long.valueOf(conf.getLong("hbase.regionserver.hlog.blocksize",
                FSUtils.getDefaultBlockSize(fs, path))),
            Boolean.valueOf(false) /*createParent*/,
            SequenceFile.CompressionType.NONE, new DefaultCodec(),
            createMetadata(conf, compress)
            });
    } catch (InvocationTargetException ite) {
      // function was properly called, but threw it's own exception
      throw new IOException(ite.getCause());
    } catch (Exception e) {
      // ignore all other exceptions. related to reflection failure
    }

    // if reflection failed, use the old createWriter
    if (this.writer == null) {
      LOG.debug("new createWriter -- HADOOP-6840 -- not available");
      this.writer = SequenceFile.createWriter(fs, conf, path,
        HLogKey.class, WALEdit.class,
        FSUtils.getDefaultBufferSize(fs),
        (short) conf.getInt("hbase.regionserver.hlog.replication",
          FSUtils.getDefaultReplication(fs, path)),
        conf.getLong("hbase.regionserver.hlog.blocksize",
          FSUtils.getDefaultBlockSize(fs, path)),
        SequenceFile.CompressionType.NONE,
        new DefaultCodec(),
        null,
        createMetadata(conf, compress));
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Using new createWriter -- HADOOP-6840");
    }
    
    this.writer_out = getSequenceFilePrivateFSDataOutputStreamAccessible();
    if (LOG.isTraceEnabled()) LOG.trace("Path=" + path + ", compression=" + compress);
  }

  // Get at the private FSDataOutputStream inside in SequenceFile so we can
  // call sync on it.  Make it accessible.
  private FSDataOutputStream getSequenceFilePrivateFSDataOutputStreamAccessible()
  throws IOException {
    FSDataOutputStream out = null;
    final Field fields [] = this.writer.getClass().getDeclaredFields();
    final String fieldName = "out";
    for (int i = 0; i < fields.length; ++i) {
      if (fieldName.equals(fields[i].getName())) {
        try {
          // Make the 'out' field up in SF.Writer accessible.
          fields[i].setAccessible(true);
          out = (FSDataOutputStream)fields[i].get(this.writer);
          break;
        } catch (IllegalAccessException ex) {
          throw new IOException("Accessing " + fieldName, ex);
        } catch (SecurityException e) {
          LOG.warn("Does not have access to out field from FSDataOutputStream",
              e);
        }
      }
    }
    return out;
  }

  @Override
  public void append(WAL.Entry entry) throws IOException {
    entry.setCompressionContext(compressionContext);
    try {
      this.writer.append(entry.getKey(), entry.getEdit());
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.writer != null) {
      try {
        this.writer.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe);
      }
      this.writer = null;
    }
  }

  @Override
  public void sync() throws IOException {
    try {
      this.writer.syncFs();
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  @Override
  public long getLength() throws IOException {
    try {
      return this.writer.getLength();
    } catch (NullPointerException npe) {
      // Concurrent close...
      throw new IOException(npe);
    }
  }

  /**
   * @return The dfsclient out stream up inside SF.Writer made accessible, or
   * null if not available.
   */
  public FSDataOutputStream getWriterFSDataOutputStream() {
    return this.writer_out;
  }
}

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
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

/**
 * Implementation of {@link HLog.Writer} that delegates to
 * SequenceFile.Writer.
 */
public class SequenceFileLogWriter implements HLog.Writer {
  static final Text WAL_VERSION_KEY = new Text("version");
  // Let the version be 1.  Let absence of a version meta tag be old, version 0.
  // Set this version '1' to be the version that introduces compression,
  // the COMPRESSION_VERSION.
  private static final int COMPRESSION_VERSION = 1;
  static final int VERSION = COMPRESSION_VERSION;
  static final Text WAL_VERSION = new Text("" + VERSION);
  static final Text WAL_COMPRESSION_TYPE_KEY = new Text("compression.type");
  static final Text DICTIONARY_COMPRESSION_TYPE = new Text("dictionary");

  private final Log LOG = LogFactory.getLog(this.getClass());
  // The sequence file we delegate to.
  private SequenceFile.Writer writer;
  // This is the FSDataOutputStream instance that is the 'out' instance
  // in the SequenceFile.Writer 'writer' instance above.
  private FSDataOutputStream writer_out;

  private Class<? extends HLogKey> keyClass;

  /**
   * Context used by our wal dictionary compressor.  Null if we're not to do
   * our custom dictionary compression.  This custom WAL compression is distinct
   * from sequencefile native compression.
   */
  private CompressionContext compressionContext;

  private Method syncFs = null;
  private Method hflush = null;
  private WALEditCodec codec;

  /**
   * Default constructor.
   */
  public SequenceFileLogWriter() {
    super();
  }

  /**
   * This constructor allows a specific HLogKey implementation to override that
   * which would otherwise be chosen via configuration property.
   * 
   * @param keyClass
   */
  public SequenceFileLogWriter(Class<? extends HLogKey> keyClass) {
    this.keyClass = keyClass;
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
    metaMap.put(WAL_VERSION_KEY, WAL_VERSION);
    if (compress) {
      // Currently we only do one compression type.
      metaMap.put(WAL_COMPRESSION_TYPE_KEY, DICTIONARY_COMPRESSION_TYPE);
    }
    return new Metadata(metaMap);
  }

  /**
   * Call this method after init() has been executed
   * 
   * @return whether WAL compression is enabled
   */
  static boolean isWALCompressionEnabled(final Metadata metadata) {
    // Check version is >= VERSION?
    Text txt = metadata.get(WAL_VERSION_KEY);
    if (txt == null || Integer.parseInt(txt.toString()) < COMPRESSION_VERSION) {
      return false;
    }
    // Now check that compression type is present.  Currently only one value.
    txt = metadata.get(WAL_COMPRESSION_TYPE_KEY);
    return txt != null && txt.equals(DICTIONARY_COMPRESSION_TYPE);
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf)
  throws IOException {
    // Should we do our custom WAL compression?
    boolean compress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (compress) {
      try {
        if (this.compressionContext == null) {
          this.compressionContext = new CompressionContext(LRUDictionary.class);
        } else {
          this.compressionContext.clear();
        }
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }

    if (null == keyClass) {
      keyClass = HLog.getKeyClass(conf);
    }

    // Create a SF.Writer instance.
    try {
      // reflection for a version of SequenceFile.createWriter that doesn't
      // automatically create the parent directory (see HBASE-2312)
      this.writer = (SequenceFile.Writer) SequenceFile.class
        .getMethod("createWriter", new Class[] {FileSystem.class,
            Configuration.class, Path.class, Class.class, Class.class,
            Integer.TYPE, Short.TYPE, Long.TYPE, Boolean.TYPE,
            CompressionType.class, CompressionCodec.class, Metadata.class})
        .invoke(null, new Object[] {fs, conf, path, HLog.getKeyClass(conf),
            WALEdit.class,
            Integer.valueOf(fs.getConf().getInt("io.file.buffer.size", 4096)),
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
        HLog.getKeyClass(conf), WALEdit.class,
        fs.getConf().getInt("io.file.buffer.size", 4096),
        (short) conf.getInt("hbase.regionserver.hlog.replication",
          FSUtils.getDefaultReplication(fs, path)),
        conf.getLong("hbase.regionserver.hlog.blocksize",
          FSUtils.getDefaultBlockSize(fs, path)),
        SequenceFile.CompressionType.NONE,
        new DefaultCodec(),
        null,
        createMetadata(conf, compress));
    } else {
      LOG.debug("using new createWriter -- HADOOP-6840");
    }

    // setup the WALEditCodec
    this.codec = WALEditCodec.create(conf, compressionContext);
    this.writer_out = getSequenceFilePrivateFSDataOutputStreamAccessible();
    this.syncFs = getSyncFs();
    this.hflush = getHFlush();
    String msg = "Path=" + path +
      ", syncFs=" + (this.syncFs != null) +
      ", hflush=" + (this.hflush != null) +
      ", compression=" + compress;
    if (this.syncFs != null || this.hflush != null) {
      LOG.debug(msg);
    } else {
      LOG.warn("No sync support! " + msg);
    }
  }

  /**
   * Now do dirty work to see if syncFs is available on the backing this.writer.
   * It will be available in branch-0.20-append and in CDH3.
   * @return The syncFs method or null if not available.
   * @throws IOException
   */
  private Method getSyncFs()
  throws IOException {
    Method m = null;
    try {
      // function pointer to writer.syncFs() method; present when sync is hdfs-200.
      m = this.writer.getClass().getMethod("syncFs", new Class<?> []{});
    } catch (SecurityException e) {
      throw new IOException("Failed test for syncfs", e);
    } catch (NoSuchMethodException e) {
      // Not available
    }
    return m;
  }

  /**
   * See if hflush (0.21 and 0.22 hadoop) is available.
   * @return The hflush method or null if not available.
   * @throws IOException
   */
  private Method getHFlush()
  throws IOException {
    Method m = null;
    try {
      Class<? extends OutputStream> c = getWriterFSDataOutputStream().getClass();
      m = c.getMethod("hflush", new Class<?> []{});
    } catch (SecurityException e) {
      throw new IOException("Failed test for hflush", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
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
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    return out;
  }

  @Override
  public void append(HLog.Entry entry) throws IOException {
    entry.getEdit().setCodec(this.codec);
    entry.getKey().setCompressionContext(compressionContext);

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
    if (this.syncFs != null) {
      try {
       this.syncFs.invoke(this.writer, HLog.NO_ARGS);
      } catch (Exception e) {
        throw new IOException("Reflection", e);
      }
    } else if (this.hflush != null) {
      try {
        this.hflush.invoke(getWriterFSDataOutputStream(), HLog.NO_ARGS);
      } catch (Exception e) {
        throw new IOException("Reflection", e);
      }
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

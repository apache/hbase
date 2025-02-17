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
package org.apache.hadoop.hbase.io.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression related stuff. Copied from hadoop-3315 tfile.
 */
@InterfaceAudience.Private
public final class Compression {
  private static final Logger LOG = LoggerFactory.getLogger(Compression.class);

  // LZO

  public static final String LZO_CODEC_CLASS_KEY = "hbase.io.compress.lzo.codec";
  public static final String LZO_CODEC_CLASS_DEFAULT = "com.hadoop.compression.lzo.LzoCodec";

  // GZ

  public static final String GZ_CODEC_CLASS_KEY = "hbase.io.compress.gz.codec";
  // Our ReusableStreamGzipCodec fixes an inefficiency in Hadoop's Gzip codec, allowing us to
  // reuse compression streams, but still requires the Hadoop native codec.
  public static final String GZ_CODEC_CLASS_DEFAULT =
    "org.apache.hadoop.hbase.io.compress.ReusableStreamGzipCodec";

  // SNAPPY

  public static final String SNAPPY_CODEC_CLASS_KEY = "hbase.io.compress.snappy.codec";
  public static final String SNAPPY_CODEC_CLASS_DEFAULT =
    "org.apache.hadoop.io.compress.SnappyCodec";

  // LZ4

  public static final String LZ4_CODEC_CLASS_KEY = "hbase.io.compress.lz4.codec";
  public static final String LZ4_CODEC_CLASS_DEFAULT = "org.apache.hadoop.io.compress.Lz4Codec";

  // ZSTD

  public static final String ZSTD_CODEC_CLASS_KEY = "hbase.io.compress.zstd.codec";
  public static final String ZSTD_CODEC_CLASS_DEFAULT =
    "org.apache.hadoop.io.compress.ZStandardCodec";

  // BZIP2

  public static final String BZIP2_CODEC_CLASS_KEY = "hbase.io.compress.bzip2.codec";
  public static final String BZIP2_CODEC_CLASS_DEFAULT = "org.apache.hadoop.io.compress.BZip2Codec";

  // LZMA

  /** @deprecated Deprecated in 2.5 and removed in 2.6 and up. See HBASE-28506. **/
  @Deprecated
  public static final String LZMA_CODEC_CLASS_KEY = "hbase.io.compress.lzma.codec";
  /** @deprecated Deprecated in 2.5 and removed in 2.6 and up. See HBASE-28506. **/
  @Deprecated
  public static final String LZMA_CODEC_CLASS_DEFAULT =
    "org.apache.hadoop.hbase.io.compress.xz.LzmaCodec";

  // Brotli

  public static final String BROTLI_CODEC_CLASS_KEY = "hbase.io.compress.brotli.codec";
  public static final String BROTLI_CODEC_CLASS_DEFAULT =
    "org.apache.hadoop.hbase.io.compress.brotli.BrotliCodec";

  /**
   * Prevent the instantiation of class.
   */
  private Compression() {
    super();
  }

  static class FinishOnFlushCompressionStream extends FilterOutputStream {
    public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Returns the classloader to load the Codec class from.
   */
  private static ClassLoader getClassLoaderForCodec() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = Compression.class.getClassLoader();
    }
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }
    if (cl == null) {
      throw new RuntimeException("A ClassLoader to load the Codec could not be determined");
    }
    return cl;
  }

  /**
   * Compression algorithms. The ordinal of these cannot change or else you risk breaking all
   * existing HFiles out there. Even the ones that are not compressed! (They use the NONE algorithm)
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED",
      justification = "We are not serializing so doesn't apply (not sure why transient though)")
  @SuppressWarnings("ImmutableEnumChecker")
  @InterfaceAudience.Public
  public static enum Algorithm {
    // LZO is GPL and requires extra install to setup. See
    // https://stackoverflow.com/questions/23441142/class-com-hadoop-compression-lzo-lzocodec-not-found-for-spark-on-cdh-5
    LZO("lzo", LZO_CODEC_CLASS_KEY, LZO_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec lzoCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (lzoCodec == null) {
          synchronized (lock) {
            if (lzoCodec == null) {
              lzoCodec = buildCodec(conf, this);
            }
          }
        }
        return lzoCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          lzoCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return lzoCodec;
        }
      }
    },

    GZ("gz", GZ_CODEC_CLASS_KEY, GZ_CODEC_CLASS_DEFAULT) {
      private volatile transient CompressionCodec gzCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (gzCodec == null) {
          synchronized (lock) {
            if (gzCodec == null) {
              gzCodec = buildCodec(conf, this);
            }
          }
        }
        return gzCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          gzCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return gzCodec;
        }
      }
    },

    NONE("none", "", "") {
      @Override
      CompressionCodec getCodec(Configuration conf) {
        return null;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(InputStream downStream,
        Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(OutputStream downStream,
        Compressor compressor, int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }
    },
    SNAPPY("snappy", SNAPPY_CODEC_CLASS_KEY, SNAPPY_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec snappyCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (snappyCodec == null) {
          synchronized (lock) {
            if (snappyCodec == null) {
              snappyCodec = buildCodec(conf, this);
            }
          }
        }
        return snappyCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          snappyCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return snappyCodec;
        }
      }
    },
    LZ4("lz4", LZ4_CODEC_CLASS_KEY, LZ4_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec lz4Codec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (lz4Codec == null) {
          synchronized (lock) {
            if (lz4Codec == null) {
              lz4Codec = buildCodec(conf, this);
            }
          }
        }
        return lz4Codec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          lz4Codec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return lz4Codec;
        }
      }
    },
    BZIP2("bzip2", BZIP2_CODEC_CLASS_KEY, BZIP2_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec bzipCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (bzipCodec == null) {
          synchronized (lock) {
            if (bzipCodec == null) {
              bzipCodec = buildCodec(conf, this);
            }
          }
        }
        return bzipCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          bzipCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return bzipCodec;
        }
      }
    },
    ZSTD("zstd", ZSTD_CODEC_CLASS_KEY, ZSTD_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec zStandardCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (zStandardCodec == null) {
          synchronized (lock) {
            if (zStandardCodec == null) {
              zStandardCodec = buildCodec(conf, this);
            }
          }
        }
        return zStandardCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          zStandardCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return zStandardCodec;
        }
      }
    },
    LZMA("lzma", LZMA_CODEC_CLASS_KEY, LZMA_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec lzmaCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (lzmaCodec == null) {
          synchronized (lock) {
            if (lzmaCodec == null) {
              lzmaCodec = buildCodec(conf, this);
            }
          }
        }
        return lzmaCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          lzmaCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return lzmaCodec;
        }
      }
    },

    BROTLI("brotli", BROTLI_CODEC_CLASS_KEY, BROTLI_CODEC_CLASS_DEFAULT) {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec brotliCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (brotliCodec == null) {
          synchronized (lock) {
            if (brotliCodec == null) {
              brotliCodec = buildCodec(conf, this);
            }
          }
        }
        return brotliCodec;
      }

      @Override
      public CompressionCodec reload(Configuration conf) {
        synchronized (lock) {
          brotliCodec = buildCodec(conf, this);
          LOG.warn("Reloaded configuration for {}", name());
          return brotliCodec;
        }
      }
    };

    private final Configuration conf;
    private final String compressName;
    private final String confKey;
    private final String confDefault;
    /** data input buffer size to absorb small reads from application. */
    private static final int DATA_IBUF_SIZE = 1 * 1024;
    /** data output buffer size to absorb small writes from application. */
    private static final int DATA_OBUF_SIZE = 4 * 1024;

    Algorithm(String name, String confKey, String confDefault) {
      this.conf = HBaseConfiguration.create();
      this.conf.setBoolean("io.native.lib.available", true);
      this.compressName = name;
      this.confKey = confKey;
      this.confDefault = confDefault;
    }

    abstract CompressionCodec getCodec(Configuration conf);

    /**
     * Reload configuration for the given algorithm.
     * <p>
     * NOTE: Experts only. This can only be done safely during process startup, before the
     * algorithm's codecs are in use. If the codec implementation is changed, the new implementation
     * may not be fully compatible with what was loaded at static initialization time, leading to
     * potential data corruption. Mostly used by unit tests.
     * @param conf configuration
     */
    public abstract CompressionCodec reload(Configuration conf);

    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
      int downStreamBufferSize) throws IOException {
      CompressionCodec codec = getCodec(conf);
      // Set the internal buffer size to read from down stream.
      if (downStreamBufferSize > 0) {
        ((Configurable) codec).getConf().setInt("io.file.buffer.size", downStreamBufferSize);
      }
      CompressionInputStream cis = codec.createInputStream(downStream, decompressor);
      BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
      return bis2;

    }

    public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
      int downStreamBufferSize) throws IOException {
      OutputStream bos1 = null;
      if (downStreamBufferSize > 0) {
        bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
      } else {
        bos1 = downStream;
      }
      CompressionOutputStream cos = createPlainCompressionStream(bos1, compressor);
      BufferedOutputStream bos2 =
        new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
      return bos2;
    }

    /**
     * Creates a compression stream without any additional wrapping into buffering streams.
     */
    public CompressionOutputStream createPlainCompressionStream(OutputStream downStream,
      Compressor compressor) throws IOException {
      CompressionCodec codec = getCodec(conf);
      ((Configurable) codec).getConf().setInt("io.file.buffer.size", 32 * 1024);
      return codec.createOutputStream(downStream, compressor);
    }

    public Compressor getCompressor() {
      CompressionCodec codec = getCodec(conf);
      if (codec != null) {
        Compressor compressor = CodecPool.getCompressor(codec);
        if (LOG.isTraceEnabled()) LOG.trace("Retrieved compressor " + compressor + " from pool.");
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using it.
            LOG.warn("Compressor obtained from CodecPool is already finished()");
          }
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compressor compressor) {
      if (compressor != null) {
        if (LOG.isTraceEnabled()) LOG.trace("Returning compressor " + compressor + " to pool.");
        CodecPool.returnCompressor(compressor);
      }
    }

    public Decompressor getDecompressor() {
      CompressionCodec codec = getCodec(conf);
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (LOG.isTraceEnabled())
          LOG.trace("Retrieved decompressor " + decompressor + " from pool.");
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using it.
            LOG.warn("Decompressor {} obtained from CodecPool is already finished", decompressor);
          }
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Decompressor decompressor) {
      if (decompressor != null) {
        if (LOG.isTraceEnabled()) LOG.trace("Returning decompressor " + decompressor + " to pool.");
        CodecPool.returnDecompressor(decompressor);
        if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
          if (LOG.isTraceEnabled()) LOG.trace("Ending decompressor " + decompressor);
          decompressor.end();
        }
      }
    }

    public String getName() {
      return compressName;
    }
  }

  public static Algorithm getCompressionAlgorithmByName(String compressName) {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    for (Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new IllegalArgumentException("Unsupported compression algorithm name: " + compressName);
  }

  /**
   * Get names of supported compression algorithms.
   * @return Array of strings, each represents a supported compression algorithm. Currently, the
   *         following compression algorithms are supported.
   */
  public static String[] getSupportedAlgorithms() {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    String[] ret = new String[algos.length];
    int i = 0;
    for (Algorithm a : algos) {
      ret[i++] = a.getName();
    }

    return ret;
  }

  /**
   * Load a codec implementation for an algorithm using the supplied configuration.
   * @param conf the configuration to use
   * @param algo the algorithm to implement
   */
  private static CompressionCodec buildCodec(final Configuration conf, final Algorithm algo) {
    try {
      String codecClassName = conf.get(algo.confKey, algo.confDefault);
      if (codecClassName == null) {
        throw new RuntimeException("No codec configured for " + algo.confKey);
      }
      Class<?> codecClass = getClassLoaderForCodec().loadClass(codecClassName);
      // The class is from hadoop so we use hadoop's ReflectionUtils to create it
      CompressionCodec codec =
        (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration(conf));
      LOG.info("Loaded codec {} for compression algorithm {}", codec.getClass().getCanonicalName(),
        algo.name());
      return codec;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    java.util.Map<String, CompressionCodec> implMap = new java.util.HashMap<>();
    for (Algorithm algo : Algorithm.class.getEnumConstants()) {
      try {
        implMap.put(algo.name(), algo.getCodec(conf));
      } catch (Exception e) {
        // Ignore failures to load codec native implementations while building the report.
        // We are to report what is configured.
      }
    }
    for (Algorithm algo : Algorithm.class.getEnumConstants()) {
      System.out.println(algo.name() + ":");
      System.out.println("    name: " + algo.getName());
      System.out.println("    confKey: " + algo.confKey);
      System.out.println("    confDefault: " + algo.confDefault);
      CompressionCodec codec = implMap.get(algo.name());
      System.out.println(
        "    implClass: " + (codec != null ? codec.getClass().getCanonicalName() : "<none>"));
    }
  }

}

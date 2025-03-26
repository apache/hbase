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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for managing compressor/decompressor dictionary loading and caching of load
 * results. Useful for any codec that can support changing dictionaries at runtime, such as
 * ZStandard.
 */
@InterfaceAudience.Private
public final class DictionaryCache {

  public static final String DICTIONARY_MAX_SIZE_KEY = "hbase.io.compress.dictionary.max.size";
  public static final int DEFAULT_DICTIONARY_MAX_SIZE = 10 * 1024 * 1024;
  public static final String RESOURCE_SCHEME = "resource://";

  private static final Logger LOG = LoggerFactory.getLogger(DictionaryCache.class);
  private static final Cache<String, byte[]> BYTE_ARRAY_CACHE =
    Caffeine.newBuilder().maximumSize(100).expireAfterAccess(10, TimeUnit.MINUTES).build();

  private DictionaryCache() {
  }

  /**
   * Load a dictionary or return a previously cached load.
   * @param conf configuration
   * @param path the hadoop Path where the dictionary is located, as a String
   * @return the dictionary bytes if successful, null otherwise
   */
  public static byte[] getDictionary(final Configuration conf, final String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }

    // Get or load the dictionary for the given path
    return BYTE_ARRAY_CACHE.get(path, s -> {
      final int maxSize = conf.getInt(DICTIONARY_MAX_SIZE_KEY, DEFAULT_DICTIONARY_MAX_SIZE);
      byte[] bytes;
      try {
        if (path.startsWith(RESOURCE_SCHEME)) {
          bytes = loadFromResource(conf, path, maxSize);
        } else {
          bytes = loadFromHadoopFs(conf, path, maxSize);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      LOG.info("Loaded dictionary from {} (size {})", s, bytes.length);
      return bytes;
    });
  }

  // Visible for testing
  public static byte[] loadFromResource(final Configuration conf, final String s, final int maxSize)
    throws IOException {
    if (!s.startsWith(RESOURCE_SCHEME)) {
      throw new IOException("Path does not start with " + RESOURCE_SCHEME);
    }
    final String path = s.substring(RESOURCE_SCHEME.length());
    LOG.info("Loading resource {}", path);
    final InputStream in = DictionaryCache.class.getClassLoader().getResourceAsStream(path);
    if (in == null) {
      throw new FileNotFoundException("Resource " + path + " not found");
    }
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      final byte[] buffer = new byte[8192];
      int n, len = 0;
      do {
        n = in.read(buffer);
        if (n > 0) {
          len += n;
          if (len > maxSize) {
            throw new IOException("Dictionary " + s + " is too large, limit=" + maxSize);
          }
          baos.write(buffer, 0, n);
        }
      } while (n > 0);
    } finally {
      in.close();
    }
    return baos.toByteArray();
  }

  private static byte[] loadFromHadoopFs(final Configuration conf, final String s,
    final int maxSize) throws IOException {
    final Path path = new Path(s);
    final FileSystem fs = FileSystem.get(path.toUri(), conf);
    LOG.info("Loading file {}", path);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final FSDataInputStream in = fs.open(path);
    try {
      final byte[] buffer = new byte[8192];
      int n, len = 0;
      do {
        n = in.read(buffer);
        if (n > 0) {
          len += n;
          if (len > maxSize) {
            throw new IOException("Dictionary " + s + " is too large, limit=" + maxSize);
          }
          baos.write(buffer, 0, n);
        }
      } while (n > 0);
    } finally {
      in.close();
    }
    return baos.toByteArray();
  }

  // Visible for testing
  public static boolean contains(String dictionaryPath) {
    return BYTE_ARRAY_CACHE.asMap().containsKey(dictionaryPath);
  }

}

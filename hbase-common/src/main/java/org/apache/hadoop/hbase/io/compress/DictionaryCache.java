/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.io.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for managing compressor/decompressor dictionary loading and caching of load
 * results. Useful for any codec that can support changing dictionaries at runtime,
 * such as ZStandard.
 */
@InterfaceAudience.Private
public class DictionaryCache {

  public static final String DICTIONARY_MAX_SIZE_KEY = "hbase.io.compress.dictionary.max.size";
  public static final int DEFAULT_DICTIONARY_MAX_SIZE = 10 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(DictionaryCache.class);
  private static volatile LoadingCache<String, byte[]> CACHE;

  /**
   * Load a dictionary or return a previously cached load.
   * @param conf configuration
   * @param path the hadoop Path where the dictionary is located, as a String
   * @return the dictionary bytes if successful, null otherwise
   */
  public static byte[] getDictionary(final Configuration conf, final String path)
      throws IOException {
    if (path == null || path.isEmpty()) {
      return null;
    }
    // Create the dictionary loading cache if we haven't already
    if (CACHE == null) {
      synchronized (DictionaryCache.class) {
        if (CACHE == null) {
          CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(
              new CacheLoader<String, byte[]>() {
                public byte[] load(String s) throws Exception {
                  final Path path = new Path(s);
                  final FileSystem fs = FileSystem.get(path.toUri(), conf);
                  final FileStatus stat = fs.getFileStatus(path);
                  if (!stat.isFile()) {
                    throw new IllegalArgumentException(s + " is not a file");
                  }
                  final int limit = conf.getInt(DICTIONARY_MAX_SIZE_KEY,
                    DEFAULT_DICTIONARY_MAX_SIZE);
                  if (stat.getLen() > limit) {
                    throw new IllegalArgumentException("Dictionary " + s + " is too large" +
                      ", size=" + stat.getLen() + ", limit=" + limit);
                  }
                  final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  final byte[] buffer = new byte[8192];
                  try (final FSDataInputStream in = fs.open(path)) {
                    int n;
                    do {
                      n = in.read(buffer);
                      if (n > 0) {
                        baos.write(buffer, 0, n);
                         }
                    } while (n > 0);
                  }
                  LOG.info("Loaded dictionary from {} (size {})", s, stat.getLen());
                  return baos.toByteArray();
                }
            });
        }
      }
    }

    // Get or load the dictionary for the given path
    try {
      return CACHE.get(path);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

}

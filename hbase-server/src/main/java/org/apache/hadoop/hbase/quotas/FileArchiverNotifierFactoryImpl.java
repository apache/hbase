/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A factory for getting instances of {@link FileArchiverNotifier}.
 */
@InterfaceAudience.Private
public final class FileArchiverNotifierFactoryImpl implements FileArchiverNotifierFactory {
  private static final FileArchiverNotifierFactoryImpl DEFAULT_INSTANCE =
      new FileArchiverNotifierFactoryImpl();
  private static volatile FileArchiverNotifierFactory CURRENT_INSTANCE = DEFAULT_INSTANCE;
  private final ConcurrentHashMap<TableName,FileArchiverNotifier> CACHE;

  private FileArchiverNotifierFactoryImpl() {
    CACHE = new ConcurrentHashMap<>();
  }

  public static FileArchiverNotifierFactory getInstance() {
    return CURRENT_INSTANCE;
  }

  static void setInstance(FileArchiverNotifierFactory inst) {
    CURRENT_INSTANCE = Objects.requireNonNull(inst);
  }

  static void reset() {
    CURRENT_INSTANCE = DEFAULT_INSTANCE;
  }

  /**
   * Returns the {@link FileArchiverNotifier} instance for the given {@link TableName}.
   *
   * @param tn The table to obtain a notifier for
   * @return The notifier for the given {@code tablename}.
   */
  public FileArchiverNotifier get(
      Connection conn, Configuration conf, FileSystem fs, TableName tn) {
    // Ensure that only one instance is exposed to callers
    final FileArchiverNotifier newMapping = new FileArchiverNotifierImpl(conn, conf, fs, tn);
    final FileArchiverNotifier previousMapping = CACHE.putIfAbsent(tn, newMapping);
    if (previousMapping == null) {
      return newMapping;
    }
    return previousMapping;
  }

  public int getCacheSize() {
    return CACHE.size();
  }

  static class CacheKey {
    final Connection conn;
    final Configuration conf;
    final FileSystem fs;
    final TableName tn;

    CacheKey(Connection conn, Configuration conf, FileSystem fs, TableName tn) {
      this.conn = conn;
      this.conf = conf;
      this.fs = fs;
      this.tn = tn;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey other = (CacheKey) o;
      // TableName should be the only thing differing..
      return tn.equals(other.tn) && conn.equals(other.conn) && conf.equals(other.conf)
          && fs.equals(other.fs);
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(conn).append(conf).append(fs).append(tn).toHashCode();
    }

    @Override
    public String toString() {
      return "CacheKey[TableName=" + tn + "]";
    }
  }
}
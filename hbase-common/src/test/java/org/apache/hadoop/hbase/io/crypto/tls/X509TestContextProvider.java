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
package org.apache.hadoop.hbase.io.crypto.tls;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

/**
 * Will cache X509TestContext to speed up tests.
 */
public class X509TestContextProvider {

  private static final class CacheKey {
    private final X509KeyType caKeyType;

    private final X509KeyType certKeyType;

    private final char[] keyPassword;

    CacheKey(X509KeyType caKeyType, X509KeyType certKeyType, char[] keyPassword) {
      this.caKeyType = caKeyType;
      this.certKeyType = certKeyType;
      this.keyPassword = keyPassword;
    }

    @Override
    public int hashCode() {
      return Objects.hash(caKeyType, certKeyType, Arrays.hashCode(keyPassword));
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CacheKey)) {
        return false;
      }
      CacheKey other = (CacheKey) obj;
      return caKeyType == other.caKeyType && certKeyType == other.certKeyType
        && Arrays.equals(keyPassword, other.keyPassword);
    }
  }

  private final Configuration conf;

  private final File tempDir;

  private final LoadingCache<CacheKey, X509TestContext> ctxs =
    CacheBuilder.newBuilder().build(new CacheLoader<CacheKey, X509TestContext>() {

      @Override
      public X509TestContext load(CacheKey key) throws Exception {
        return X509TestContext.newBuilder(conf).setTempDir(tempDir)
          .setKeyStorePassword(key.keyPassword).setKeyStoreKeyType(key.certKeyType)
          .setTrustStorePassword(key.keyPassword).setTrustStoreKeyType(key.caKeyType).build();
      }
    });

  public X509TestContextProvider(Configuration conf, File tempDir) {
    this.conf = conf;
    this.tempDir = tempDir;
  }

  public X509TestContext get(X509KeyType caKeyType, X509KeyType certKeyType, char[] keyPassword) {
    return ctxs.getUnchecked(new CacheKey(caKeyType, certKeyType, keyPassword));
  }
}

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

/**
 * Implementation of {@link FileKeyStoreLoader} that loads from PKCS12 files.
 * <p/>
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/main/java/org/apache/zookeeper/common/PKCS12FileLoader.java">Base
 *      revision</a>
 */
final class PKCS12FileLoader extends StandardTypeFileKeyStoreLoader {
  private PKCS12FileLoader(String keyStorePath, String trustStorePath, char[] keyStorePassword,
    char[] trustStorePassword) {
    super(keyStorePath, trustStorePath, keyStorePassword, trustStorePassword,
      SupportedStandardKeyFormat.PKCS12);
  }

  static class Builder extends FileKeyStoreLoader.Builder<PKCS12FileLoader> {
    @Override
    PKCS12FileLoader build() {
      return new PKCS12FileLoader(keyStorePath, trustStorePath, keyStorePassword,
        trustStorePassword);
    }
  }
}

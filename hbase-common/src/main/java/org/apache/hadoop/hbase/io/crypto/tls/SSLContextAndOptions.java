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

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hbase.io.crypto.tls.X509Util.CONFIG_PREFIX;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.handler.ssl.ClientAuth;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.IdentityCipherSuiteFilter;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.JdkSslContext;
import org.apache.hbase.thirdparty.io.netty.handler.ssl.SslContext;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/c74658d398cdc1d207aa296cb6e20de00faec03e/zookeeper-server/src/main/java/org/apache/zookeeper/common/SSLContextAndOptions.java">Base
 *      revision</a>
 */
@InterfaceAudience.Private
public class SSLContextAndOptions {
  private static final String TLS_ENABLED_PROTOCOLS = CONFIG_PREFIX + "enabledProtocols";
  private static final String TLS_CIPHER_SUITES = CONFIG_PREFIX + "ciphersuites";

  private final String[] enabledProtocols;
  private final List<String> cipherSuitesAsList;
  private final SSLContext sslContext;

  /**
   * Note: constructor is intentionally package-private, only the X509Util class should be creating
   * instances of this class.
   * @param config     The HBase configuration
   * @param sslContext The SSLContext.
   */
  SSLContextAndOptions(final Configuration config, final SSLContext sslContext) {
    this.sslContext = requireNonNull(sslContext);
    this.enabledProtocols = getEnabledProtocols(requireNonNull(config), sslContext);
    String[] ciphers = getCipherSuites(config);
    this.cipherSuitesAsList = Collections.unmodifiableList(Arrays.asList(ciphers));
  }

  public SSLContext getSSLContext() {
    return sslContext;
  }

  public SslContext createNettyJdkSslContext(SSLContext sslContext, boolean isClientSocket) {
    return new JdkSslContext(sslContext, isClientSocket, cipherSuitesAsList,
      IdentityCipherSuiteFilter.INSTANCE, null, ClientAuth.NONE, enabledProtocols, false);
  }

  private String[] getEnabledProtocols(final Configuration config, final SSLContext sslContext) {
    String enabledProtocolsInput = config.get(TLS_ENABLED_PROTOCOLS);
    if (enabledProtocolsInput == null) {
      return new String[] { sslContext.getProtocol() };
    }
    return enabledProtocolsInput.split(",");
  }

  private String[] getCipherSuites(final Configuration config) {
    String cipherSuitesInput = config.get(TLS_CIPHER_SUITES);
    if (cipherSuitesInput == null) {
      return X509Util.getDefaultCipherSuites();
    } else {
      return cipherSuitesInput.split(",");
    }
  }
}

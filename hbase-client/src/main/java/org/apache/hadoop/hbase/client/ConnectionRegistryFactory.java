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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.ServiceLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * The entry point for creating a {@link ConnectionRegistry}.
 */
@InterfaceAudience.Private
public final class ConnectionRegistryFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionRegistryFactory.class);

  private static final ImmutableMap<String, ConnectionRegistryURIFactory> FACTORIES;
  static {
    ImmutableMap.Builder<String, ConnectionRegistryURIFactory> builder = ImmutableMap.builder();
    for (ConnectionRegistryURIFactory factory : ServiceLoader
      .load(ConnectionRegistryURIFactory.class)) {
      builder.put(factory.getScheme().toLowerCase(), factory);
    }
    // throw IllegalArgumentException if there are duplicated keys
    FACTORIES = builder.buildOrThrow();
  }

  private ConnectionRegistryFactory() {
  }

  /**
   * Returns the connection registry implementation to use, for the given connection url
   * {@code uri}.
   * <p/>
   * We use {@link ServiceLoader} to load different implementations, and use the scheme of the given
   * {@code uri} to select. And if there is no protocol specified, or we can not find a
   * {@link ConnectionRegistryURIFactory} implementation for the given scheme, we will fallback to
   * use the old way to create the {@link ConnectionRegistry}. Notice that, if fallback happens, the
   * specified connection url {@code uri} will not take effect, we will load all the related
   * configurations from the given Configuration instance {@code conf}
   */
  static ConnectionRegistry create(URI uri, Configuration conf, User user) throws IOException {
    if (StringUtils.isBlank(uri.getScheme())) {
      LOG.warn("No scheme specified for {}, fallback to use old way", uri);
      return create(conf, user);
    }
    ConnectionRegistryURIFactory factory = FACTORIES.get(uri.getScheme().toLowerCase());
    if (factory == null) {
      LOG.warn("No factory registered for {}, fallback to use old way", uri);
      return create(conf, user);
    }
    return factory.create(uri, conf, user);
  }

  /**
   * Returns the connection registry implementation to use.
   * <p/>
   * This is used when we do not have a connection url, we will use the old way to load the
   * connection registry, by checking the
   * {@literal HConstants#CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY} configuration.
   */
  static ConnectionRegistry create(Configuration conf, User user) {
    Class<? extends ConnectionRegistry> clazz =
      conf.getClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        RpcConnectionRegistry.class, ConnectionRegistry.class);
    return ReflectionUtils.newInstance(clazz, conf, user);
  }

  /**
   * Check whether the given {@code uri} is valid.
   * <p/>
   * Notice that there is no fallback logic for this method, so passing an URI with null scheme can
   * not pass.
   * @throws IOException if this is not a valid connection registry URI
   */
  public static void validate(URI uri) throws IOException {
    if (StringUtils.isBlank(uri.getScheme())) {
      throw new IOException("No schema for uri: " + uri);
    }
    ConnectionRegistryURIFactory factory = FACTORIES.get(uri.getScheme().toLowerCase(Locale.ROOT));
    if (factory == null) {
      throw new IOException(
        "No factory registered for scheme " + uri.getScheme() + ", uri: " + uri);
    }
    factory.validate(uri);
  }

  /**
   * If the given {@code clusterKey} can be parsed to a {@link URI}, and the scheme of the
   * {@link URI} is supported by us, return the {@link URI}, otherwise return {@code null}.
   * @param clusterKey the cluster key, typically from replication peer config
   * @return a {@link URI} or {@code null}.
   */
  public static URI tryParseAsConnectionURI(String clusterKey) {
    // The old cluster key format may not be parsed as URI if we use ip address as the zookeeper
    // address, so here we need to catch the URISyntaxException and return false
    URI uri;
    try {
      uri = new URI(clusterKey);
    } catch (URISyntaxException e) {
      LOG.debug("failed to parse cluster key to URI: {}", clusterKey, e);
      return null;
    }
    if (FACTORIES.containsKey(uri.getScheme())) {
      return uri;
    } else {
      return null;
    }
  }
}

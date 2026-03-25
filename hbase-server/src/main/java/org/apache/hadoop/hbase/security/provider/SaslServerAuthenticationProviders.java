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
package org.apache.hadoop.hbase.security.provider;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class SaslServerAuthenticationProviders {
  private static final Logger LOG =
    LoggerFactory.getLogger(SaslClientAuthenticationProviders.class);

  public static final String EXTRA_PROVIDERS_KEY = "hbase.server.sasl.provider.extras";

  private static final AtomicReference<SaslServerAuthenticationProviders> HOLDER =
    new AtomicReference<>();

  private final Map<Byte, SaslServerAuthenticationProvider> providers;

  /**
   * Creates a new instance of SaslServerAuthenticationProviders.
   * @param conf the configuration to use for loading providers
   */
  public SaslServerAuthenticationProviders(Configuration conf) {
    ServiceLoader<SaslServerAuthenticationProvider> loader =
      ServiceLoader.load(SaslServerAuthenticationProvider.class);
    HashMap<Byte, SaslServerAuthenticationProvider> providerMap = new HashMap<>();
    for (SaslServerAuthenticationProvider provider : loader) {
      addProviderIfNotExists(provider, providerMap);
    }

    addExtraProviders(conf, providerMap);

    if (LOG.isTraceEnabled()) {
      String loadedProviders = providerMap.values().stream()
        .map((provider) -> provider.getClass().getName()).collect(Collectors.joining(", "));
      if (loadedProviders.isEmpty()) {
        loadedProviders = "None!";
      }
      LOG.trace("Found SaslServerAuthenticationProviders {}", loadedProviders);
    }

    // Initialize the providers once, before we get into the RPC path.
    providerMap.forEach((b, provider) -> {
      try {
        // Give them a copy, just to make sure there is no funny-business going on.
        provider.init(new Configuration(conf));
      } catch (IOException e) {
        LOG.error("Failed to initialize {}", provider.getClass(), e);
        throw new RuntimeException("Failed to initialize " + provider.getClass().getName(), e);
      }
    });
    this.providers = Collections.unmodifiableMap(providerMap);
  }

  /**
   * Returns the number of registered providers.
   */
  public int getNumRegisteredProviders() {
    return providers.size();
  }

  /**
   * Returns a singleton instance of {@link SaslServerAuthenticationProviders}.
   * @deprecated Since 2.5.14 and 2.6.4, will be removed in newer minor release lines. This class
   *             should not be singleton, please do not use it any more. see HBASE-29144 for more
   *             details.
   */
  @Deprecated
  public static SaslServerAuthenticationProviders getInstance(Configuration conf) {
    SaslServerAuthenticationProviders providers = HOLDER.get();
    if (null == providers) {
      synchronized (HOLDER) {
        // Someone else beat us here
        providers = HOLDER.get();
        if (null != providers) {
          return providers;
        }

        providers = new SaslServerAuthenticationProviders(conf);
        HOLDER.set(providers);
      }
    }
    return providers;
  }

  /**
   * Removes the cached singleton instance of {@link SaslServerAuthenticationProviders}.
   * @deprecated Since 2.5.14 and 2.6.4, will be removed in newer minor release lines. This class
   *             should not be singleton, please do not use it any more. see HBASE-29144 for more
   *             details.
   */
  @Deprecated
  public static void reset() {
    synchronized (HOLDER) {
      HOLDER.set(null);
    }
  }

  /**
   * Adds the given provider into the map of providers if a mapping for the auth code does not
   * already exist in the map.
   */
  static void addProviderIfNotExists(SaslServerAuthenticationProvider provider,
    HashMap<Byte, SaslServerAuthenticationProvider> providers) {
    final byte newProviderAuthCode = provider.getSaslAuthMethod().getCode();
    final SaslServerAuthenticationProvider alreadyRegisteredProvider =
      providers.get(newProviderAuthCode);
    if (alreadyRegisteredProvider != null) {
      throw new RuntimeException("Trying to load SaslServerAuthenticationProvider "
        + provider.getClass() + ", but " + alreadyRegisteredProvider.getClass()
        + " is already registered with the same auth code");
    }
    providers.put(newProviderAuthCode, provider);
  }

  /**
   * Adds any providers defined in the configuration.
   */
  static void addExtraProviders(Configuration conf,
    HashMap<Byte, SaslServerAuthenticationProvider> providers) {
    for (String implName : conf.getStringCollection(EXTRA_PROVIDERS_KEY)) {
      Class<?> clz;
      try {
        clz = Class.forName(implName);
      } catch (ClassNotFoundException e) {
        LOG.warn("Failed to find SaslServerAuthenticationProvider class {}", implName, e);
        continue;
      }

      if (!SaslServerAuthenticationProvider.class.isAssignableFrom(clz)) {
        LOG.warn("Server authentication class {} is not an instance of "
          + "SaslServerAuthenticationProvider", clz);
        continue;
      }

      try {
        SaslServerAuthenticationProvider provider =
          (SaslServerAuthenticationProvider) clz.getConstructor().newInstance();
        addProviderIfNotExists(provider, providers);
      } catch (InstantiationException | IllegalAccessException | NoSuchMethodException
        | InvocationTargetException e) {
        LOG.warn("Failed to instantiate {}", clz, e);
      }
    }
  }

  /**
   * Selects the appropriate SaslServerAuthenticationProvider from those available. If there is no
   * matching provider for the given {@code authByte}, this method will return null.
   */
  public SaslServerAuthenticationProvider selectProvider(byte authByte) {
    return providers.get(Byte.valueOf(authByte));
  }

  /**
   * Extracts the SIMPLE authentication provider.
   */
  public SaslServerAuthenticationProvider getSimpleProvider() {
    Optional<SaslServerAuthenticationProvider> opt = providers.values().stream()
      .filter((p) -> p instanceof SimpleSaslServerAuthenticationProvider).findFirst();
    if (!opt.isPresent()) {
      throw new RuntimeException("SIMPLE authentication provider not available when it should be");
    }
    return opt.get();
  }
}

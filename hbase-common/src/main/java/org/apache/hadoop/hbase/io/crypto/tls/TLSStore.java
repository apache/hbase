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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One keystore or truststore, resolved from configuration with every attribute drawn from a single
 * key prefix.
 * <p/>
 * Each TLS surface exposes two prefixes for the same store: a role-scoped one added for single-EKU
 * certificate support (e.g. {@code hbase.rpc.tls.server.}) and the historical unscoped one (e.g.
 * {@code hbase.rpc.tls.}). Resolving attribute by attribute would let a store's location come from
 * one prefix while its password or type came from the other, which opens either the wrong file or
 * the right file with the wrong credentials. {@link #resolve} therefore picks the prefix once, from
 * the location, and reads the rest of the store from that same prefix.
 */
@InterfaceAudience.Private
public final class TLSStore {

  private static final Logger LOG = LoggerFactory.getLogger(TLSStore.class);

  /** Stores already checked for unused legacy keys, so the scan and the warning happen once. */
  private static final Set<String> LOGGED_STORES = ConcurrentHashMap.newKeySet();

  /**
   * The key postfixes making up one store, by surface. The RPC keys use {@code .location} while the
   * servlet surfaces (REST, Thrift) use {@code .store}, and only the latter have a separate
   * {@code keypassword}.
   */
  public enum Keys {
    RPC_KEYSTORE("keystore.location", "keystore.password", "keystore.type", null),
    RPC_TRUSTSTORE("truststore.location", "truststore.password", "truststore.type", null),
    SERVLET_KEYSTORE("keystore.store", "keystore.password", "keystore.type",
      "keystore.keypassword"),
    SERVLET_TRUSTSTORE("truststore.store", "truststore.password", "truststore.type", null);

    private final String location;
    private final String password;
    private final String type;
    private final String keyPassword;

    Keys(String location, String password, String type, String keyPassword) {
      this.location = location;
      this.password = password;
      this.type = type;
      this.keyPassword = keyPassword;
    }

    private String[] all() {
      return keyPassword == null
        ? new String[] { location, password, type }
        : new String[] { location, password, type, keyPassword };
    }
  }

  private final String location;
  private final char[] password;
  private final char[] keyPassword;
  private final String type;

  private TLSStore(String location, char[] password, char[] keyPassword, String type) {
    this.location = location;
    this.password = password;
    this.keyPassword = keyPassword;
    this.type = type;
  }

  /**
   * Reads one store from {@code config}. The prefix supplying the location supplies every other
   * attribute too, so the two prefixes are never combined within a single store.
   * <p/>
   * Only the location is mandatory under a prefix: a store may have no password at all, and the
   * type is auto-detected from the file extension when absent (see
   * {@link KeyStoreFileType#fromPropertyValueOrFileName}). Keeping the legacy keys in place while
   * migrating is valid -- the ones left unused are logged once so they can be cleaned up.
   * @param config       the configuration to read from
   * @param rolePrefix   role-scoped prefix, e.g. {@code hbase.rpc.tls.server.}
   * @param legacyPrefix historical unscoped prefix, e.g. {@code hbase.rpc.tls.}
   * @param keys         which store to read, and under which postfixes
   * @throws IllegalArgumentException if {@code rolePrefix} supplies any attribute but not the
   *                                  location, since the location would then be taken from
   *                                  {@code legacyPrefix}
   */
  public static TLSStore resolve(Configuration config, String rolePrefix, String legacyPrefix,
    Keys keys) throws IOException {
    String roleLocationKey = rolePrefix + keys.location;
    boolean useRole = config.get(roleLocationKey) != null;
    if (!useRole) {
      for (String postfix : keys.all()) {
        if (config.get(rolePrefix + postfix) != null) {
          throw new IllegalArgumentException(
            rolePrefix + postfix + " is set, but " + roleLocationKey + " is not. Once any "
              + rolePrefix + " key is used, that prefix must supply the store location.");
        }
      }
    }
    String prefix = useRole ? rolePrefix : legacyPrefix;
    if (useRole) {
      logOnce(config, rolePrefix, legacyPrefix, keys);
    }
    return new TLSStore(config.get(prefix + keys.location, ""),
      config.getPassword(prefix + keys.password),
      keys.keyPassword == null ? null : config.getPassword(prefix + keys.keyPassword),
      config.get(prefix + keys.type, ""));
  }

  private static void logOnce(Configuration config, String rolePrefix, String legacyPrefix,
    Keys keys) {
    if (!LOGGED_STORES.add(rolePrefix + keys.location)) {
      return;
    }
    List<String> unused = Arrays.stream(keys.all()).map(postfix -> legacyPrefix + postfix)
      .filter(key -> config.get(key) != null).collect(Collectors.toList());
    if (!unused.isEmpty()) {
      LOG.warn("{} supplies this store, so these keys are unused: {}. Remove them once the"
        + " migration is complete.", rolePrefix, String.join(", ", unused));
    }
  }

  public String getLocation() {
    return location;
  }

  public String getPassword() {
    return password == null ? null : new String(password);
  }

  /** Falls back to the store password when no separate key password is configured. */
  public String getKeyPassword() {
    char[] effective = getKeyPasswordChars();
    return effective == null ? null : new String(effective);
  }

  /** For the netty paths, whose key/trust manager factories take char[]. */
  public char[] getPasswordChars() {
    return password;
  }

  /** @see #getKeyPassword() */
  public char[] getKeyPasswordChars() {
    return keyPassword != null ? keyPassword : password;
  }

  public String getType() {
    return type;
  }
}

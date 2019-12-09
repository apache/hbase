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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DefaultProviderSelector implements AuthenticationProviderSelector {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultProviderSelector.class);
  private final ReadWriteLock LOCK = new ReentrantReadWriteLock();

  Configuration conf;
  SimpleSaslClientAuthenticationProvider simpleAuth = null;
  GssSaslClientAuthenticationProvider krbAuth = null;
  DigestSaslClientAuthenticationProvider digestAuth = null;

  @Override
  public void configure(Configuration conf, Map<Byte,SaslClientAuthenticationProvider> providers) {
    try {
      LOCK.writeLock().lock();
      if (this.conf != null) {
        throw new IllegalStateException("configure() should only be called once");
      }
      this.conf = Objects.requireNonNull(conf);
    } finally {
      LOCK.writeLock().unlock();
    }
    
    for (SaslClientAuthenticationProvider provider : Objects.requireNonNull(providers).values()) {
      if (provider instanceof SimpleSaslClientAuthenticationProvider) {
        if (simpleAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple SimpleSaslClientAuthenticationProvider instances");
        }
        simpleAuth = (SimpleSaslClientAuthenticationProvider) provider;
      } else if (provider instanceof GssSaslClientAuthenticationProvider) {
        if (krbAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple GssSaslClientAuthenticationProvider instances");
        }
        krbAuth = (GssSaslClientAuthenticationProvider) provider;
      } else if (provider instanceof DigestSaslClientAuthenticationProvider) {
        if (digestAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple DigestSaslClientAuthenticationProvider instances");
        }
        digestAuth = (DigestSaslClientAuthenticationProvider) provider;
      } else {
        LOG.warn("Ignoring unknown SaslClientAuthenticationProvider: {}", provider.getClass());
      }
    }
    if (simpleAuth == null || krbAuth == null || digestAuth == null) {
      throw new IllegalStateException("Failed to load SIMPLE, KERBEROS, and DIGEST authentication "
          + "providers. Classpath is not sane.");
    }
  }

  @Override
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      Text clusterId, UserGroupInformation ugi) {
    if (clusterId == null) {
      throw new NullPointerException("Null clusterId was given");
    }
    try {
      LOCK.readLock().lock();
      // Superfluous: we dont' do SIMPLE auth over SASL, but we should to simplify.
      if (!User.isHBaseSecurityEnabled(conf)) {
        return new Pair<>(simpleAuth, null);
      }
    } finally {
      LOCK.readLock().unlock();
    }
    // Must be digest auth, look for a token.
    // TestGenerateDelegationToken is written expecting DT is used when DT and Krb are both present.
    // (for whatever that's worth).
    for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
      // We need to check for two things:
      //   1. This token is for the HBase cluster we want to talk to
      //   2. We have suppporting client implementation to handle the token (the "kind" of token)
      if (clusterId.equals(token.getService()) &&
          digestAuth.getTokenKind().equals(token.getKind())) {
        return new Pair<>(digestAuth, token);
      }
    }
    if (ugi.hasKerberosCredentials()) {
      return new Pair<>(krbAuth, null);
    }
    LOG.warn("No matching SASL authentication provider and supporting token found from providers.");
    return null;
  }

}

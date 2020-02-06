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

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Objects;

import net.jcip.annotations.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link AuthenticationProviderSelector} which can choose from the
 * authentication implementations which HBase provides out of the box: Simple, Kerberos, and
 * Delegation Token authentication.
 *
 * This implementation will ignore any {@link SaslAuthenticationProvider}'s which are available
 * on the classpath or specified in the configuration because HBase cannot correctly choose which
 * token should be returned to a client when multiple are present. It is expected that users
 * implement their own {@link AuthenticationProviderSelector} when writing a custom provider.
 *
 * This implementation is not thread-safe. {@link #configure(Configuration, Collection)} and
 * {@link #selectProvider(String, User)} is not safe if they are called concurrently.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@NotThreadSafe
public class BuiltInProviderSelector implements AuthenticationProviderSelector {
  private static final Logger LOG = LoggerFactory.getLogger(BuiltInProviderSelector.class);

  Configuration conf;
  SimpleSaslClientAuthenticationProvider simpleAuth = null;
  GssSaslClientAuthenticationProvider krbAuth = null;
  DigestSaslClientAuthenticationProvider digestAuth = null;
  Text digestAuthTokenKind = null;

  @Override
  public void configure(
      Configuration conf, Collection<SaslClientAuthenticationProvider> providers) {
    if (this.conf != null) {
      throw new IllegalStateException("configure() should only be called once");
    }
    this.conf = Objects.requireNonNull(conf);

    for (SaslClientAuthenticationProvider provider : Objects.requireNonNull(providers)) {
      final String name = provider.getSaslAuthMethod().getName();
      if (SimpleSaslAuthenticationProvider.SASL_AUTH_METHOD.getName().contentEquals(name)) {
        if (simpleAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple SimpleSaslClientAuthenticationProvider instances");
        }
        simpleAuth = (SimpleSaslClientAuthenticationProvider) provider;
      } else if (GssSaslAuthenticationProvider.SASL_AUTH_METHOD.getName().equals(name)) {
        if (krbAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple GssSaslClientAuthenticationProvider instances");
        }
        krbAuth = (GssSaslClientAuthenticationProvider) provider;
      } else if (DigestSaslAuthenticationProvider.SASL_AUTH_METHOD.getName().equals(name)) {
        if (digestAuth != null) {
          throw new IllegalStateException(
              "Encountered multiple DigestSaslClientAuthenticationProvider instances");
        }
        digestAuth = (DigestSaslClientAuthenticationProvider) provider;
        digestAuthTokenKind = new Text(digestAuth.getTokenKind());
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
      String clusterId, User user) {
    requireNonNull(clusterId, "Null clusterId was given");
    requireNonNull(user, "Null user was given");

    // Superfluous: we don't do SIMPLE auth over SASL, but we should to simplify.
    if (!User.isHBaseSecurityEnabled(conf)) {
      return new Pair<>(simpleAuth, null);
    }

    final Text clusterIdAsText = new Text(clusterId);

    // Must be digest auth, look for a token.
    // TestGenerateDelegationToken is written expecting DT is used when DT and Krb are both present.
    // (for whatever that's worth).
    for (Token<? extends TokenIdentifier> token : user.getTokens()) {
      // We need to check for two things:
      //   1. This token is for the HBase cluster we want to talk to
      //   2. We have suppporting client implementation to handle the token (the "kind" of token)
      if (clusterIdAsText.equals(token.getService()) &&
          digestAuthTokenKind.equals(token.getKind())) {
        return new Pair<>(digestAuth, token);
      }
    }
    // Unwrap PROXY auth'n method if that's what we have coming in.
    final UserGroupInformation currentUser = user.getUGI();
    // May be null if Hadoop AuthenticationMethod is PROXY
    final UserGroupInformation realUser = currentUser.getRealUser();
    if (currentUser.hasKerberosCredentials() ||
        (realUser != null && realUser.hasKerberosCredentials())) {
      return new Pair<>(krbAuth, null);
    }
    // This indicates that a client is requesting some authentication mechanism which the servers
    // don't know how to process (e.g. there is no provider which can support it). This may be
    // a bug or simply a misconfiguration of client *or* server.
    LOG.warn("No matching SASL authentication provider and supporting token found from providers"
        + " for user: {}", user);
    return null;
  }

}

package org.apache.hadoop.hbase.security.provider;

import java.util.Map;

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

  Configuration conf;
  SimpleSaslClientAuthenticationProvider simpleAuth = null;
  GssSaslClientAuthenticationProvider krbAuth = null;
  DigestSaslClientAuthenticationProvider digestAuth = null;
  Map<Byte,SaslClientAuthenticationProvider> providers;

  @Override
  public void configure(Configuration conf, Map<Byte,SaslClientAuthenticationProvider> providers) {
    this.conf = conf;
    this.providers = providers;
    for (SaslClientAuthenticationProvider provider : providers.values()) {
      if (provider instanceof SimpleSaslClientAuthenticationProvider) {
        if (simpleAuth != null) {
          LOG.warn("Ignoring duplicate SimpleSaslClientAuthenticationProvider: previous={}, ignored={}",
              simpleAuth.getClass(), provider.getClass());
        } else {
          simpleAuth = (SimpleSaslClientAuthenticationProvider) provider;
        }
      } else if (provider instanceof GssSaslClientAuthenticationProvider) {
        if (krbAuth != null) {
          LOG.warn("Ignoring duplicate GssSaslClientAuthenticationProvider: previous={}, ignored={}",
              krbAuth.getClass(), provider.getClass());
        } else {
          krbAuth = (GssSaslClientAuthenticationProvider) provider;
        }
      } else if (provider instanceof DigestSaslClientAuthenticationProvider) {
        if (digestAuth != null) {
          LOG.warn("Ignoring duplicate DigestSaslClientAuthenticationProvider: previous={}, ignored={}",
              digestAuth.getClass(), provider.getClass());
        } else {
          digestAuth = (DigestSaslClientAuthenticationProvider) provider;
        }
      } else {
        LOG.warn("Ignoring unknown SaslClientAuthenticationProvider: {}", provider.getClass());
      }
    }
  }

  @Override
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      Text clusterId, UserGroupInformation ugi) {
    if (clusterId == null) {
      throw new NullPointerException("Null clusterId was given");
    }
    // Superfluous: we dont' do SIMPLE auth over SASL, but we should to simplify.
    if (!User.isHBaseSecurityEnabled(conf)) {
      return new Pair<>(simpleAuth, null);
    }
    // Must be digest auth, look for a token.
    // TestGenerateDelegationToken is written expecting DT is used when DT and Krb are both present.
    // (for whatever that's worth).
    for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
      // We need to check for two things:
      //   1. This token is for the HBase cluster we want to talk to
      //   2. We have suppporting client implementation to handle the token (the "kind" of token)
      if (clusterId.equals(token.getService()) && digestAuth.getTokenKind().equals(token.getKind())) {
        return new Pair<>(digestAuth, token);
      }
    }
    if (ugi.hasKerberosCredentials()) {
      return new Pair<>(krbAuth, null);
    }
    LOG.warn("No matching SASL authentication provider and supporting token found from providers {} to HBase cluster {}", providers, clusterId);
    return null;
  }

}

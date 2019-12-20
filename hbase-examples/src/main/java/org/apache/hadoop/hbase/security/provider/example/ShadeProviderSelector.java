package org.apache.hadoop.hbase.security.provider.example;

import java.util.Collection;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.BuiltInProviderSelector;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

public class ShadeProviderSelector extends BuiltInProviderSelector {

  private ShadeSaslClientAuthenticationProvider shade;

  @Override
  public void configure(
      Configuration conf, Collection<SaslClientAuthenticationProvider> providers) {
    super.configure(conf, providers);

    this.shade = (ShadeSaslClientAuthenticationProvider) providers.stream()
        .filter((p) -> p instanceof ShadeSaslClientAuthenticationProvider)
        .findFirst()
        .orElseThrow(() -> new RuntimeException(
            "ShadeSaslClientAuthenticationProvider not loaded"));
  }

  @Override
  public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      Text clusterId, UserGroupInformation ugi) {
    Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair =
        super.selectProvider(clusterId, ugi);

    Optional<Token<?>> optional = ugi.getTokens().stream()
        .filter((t) -> ShadeSaslAuthenticationProvider.TOKEN_KIND.equals(t.getKind()))
        .findFirst();
    if (optional.isPresent()) {
      return new Pair<>(shade, optional.get());
    }

    return pair;
  }
}

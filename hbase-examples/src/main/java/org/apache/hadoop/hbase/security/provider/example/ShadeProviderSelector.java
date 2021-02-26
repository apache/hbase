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
package org.apache.hadoop.hbase.security.provider.example;

import java.util.Collection;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.BuiltInProviderSelector;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ShadeProviderSelector extends BuiltInProviderSelector {

  private final Text SHADE_TOKEN_KIND_TEXT = new Text(ShadeSaslAuthenticationProvider.TOKEN_KIND);
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
      String clusterId, User user) {
    Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair =
        super.selectProvider(clusterId, user);

    Optional<Token<?>> optional = user.getTokens().stream()
        .filter((t) -> SHADE_TOKEN_KIND_TEXT.equals(t.getKind()))
        .findFirst();
    if (optional.isPresent()) {
      return new Pair<>(shade, optional.get());
    }

    return pair;
  }
}

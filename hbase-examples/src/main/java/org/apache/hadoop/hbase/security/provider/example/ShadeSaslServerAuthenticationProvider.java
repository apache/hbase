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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.security.provider.AttemptingUserProvidingSaslServer;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ShadeSaslServerAuthenticationProvider extends ShadeSaslAuthenticationProvider
    implements SaslServerAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      ShadeSaslServerAuthenticationProvider.class);

  public static final String PASSWORD_FILE_KEY = "hbase.security.shade.password.file";
  static final char SEPARATOR = '=';

  private AtomicReference<UserGroupInformation> attemptingUser = new AtomicReference<>(null);
  private Map<String,char[]> passwordDatabase;

  @Override
  public void init(Configuration conf) throws IOException {
    passwordDatabase = readPasswordDB(conf);
  }

  @Override
  public AttemptingUserProvidingSaslServer createServer(
      SecretManager<TokenIdentifier> secretManager, Map<String, String> saslProps)
          throws IOException {
    return new AttemptingUserProvidingSaslServer(
        new SaslPlainServer(
            new ShadeSaslServerCallbackHandler(attemptingUser, passwordDatabase)),
      () -> attemptingUser.get());
  }

  Map<String,char[]> readPasswordDB(Configuration conf) throws IOException {
    String passwordFileName = conf.get(PASSWORD_FILE_KEY);
    if (passwordFileName == null) {
      throw new RuntimeException(PASSWORD_FILE_KEY
          + " is not defined in configuration, cannot use this implementation");
    }

    Path passwordFile = new Path(passwordFileName);
    FileSystem fs = passwordFile.getFileSystem(conf);
    if (!fs.exists(passwordFile)) {
      throw new RuntimeException("Configured password file does not exist: " + passwordFile);
    }

    Map<String,char[]> passwordDb = new HashMap<>();
    try (FSDataInputStream fdis = fs.open(passwordFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fdis))) {
      String line = null;
      int offset = 0;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        String[] parts = StringUtils.split(line, SEPARATOR);
        if (parts.length < 2) {
          LOG.warn("Password file contains invalid record on line {}, skipping", offset + 1);
          continue;
        }

        final String username = parts[0];
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i < parts.length; i++) {
          if (builder.length() > 0) {
            builder.append(SEPARATOR);
          }
          builder.append(parts[i]);
        }

        passwordDb.put(username, builder.toString().toCharArray());
        offset++;
      }
    }

    return passwordDb;
  }

  @Override
  public boolean supportsProtocolAuthentication() {
    return false;
  }

  @Override
  public UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
    return UserGroupInformation.createRemoteUser(authzId);
  }

  static class ShadeSaslServerCallbackHandler implements CallbackHandler {
    private final AtomicReference<UserGroupInformation> attemptingUser;
    private final Map<String,char[]> passwordDatabase;

    public ShadeSaslServerCallbackHandler(AtomicReference<UserGroupInformation> attemptingUser,
        Map<String,char[]> passwordDatabase) {
      this.attemptingUser = attemptingUser;
      this.passwordDatabase = passwordDatabase;
    }

    @Override public void handle(Callback[] callbacks)
        throws InvalidToken, UnsupportedCallbackException {
      LOG.info("SaslServerCallbackHandler called", new Exception());
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL PLAIN Callback");
        }
      }

      if (nc != null && pc != null) {
        String username = nc.getName();

        UserGroupInformation ugi = createUgiForRemoteUser(username);
        attemptingUser.set(ugi);

        char[] clientPassword = pc.getPassword();
        char[] actualPassword = passwordDatabase.get(username);
        if (!Arrays.equals(clientPassword, actualPassword)) {
          throw new InvalidToken("Authentication failed for " + username);
        }
      }

      if (ac != null) {
        String authenticatedUserId = ac.getAuthenticationID();
        String userRequestedToExecuteAs = ac.getAuthorizationID();
        if (authenticatedUserId.equals(userRequestedToExecuteAs)) {
          ac.setAuthorized(true);
          ac.setAuthorizedID(userRequestedToExecuteAs);
        } else {
          ac.setAuthorized(false);
        }
      }
    }

    UserGroupInformation createUgiForRemoteUser(String username) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
      ugi.setAuthenticationMethod(ShadeSaslAuthenticationProvider.METHOD.getAuthMethod());
      return ugi;
    }
  }
}

/**
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
package org.apache.hadoop.hbase.http;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Statics to get access to Http related configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HttpConfig {
  private Policy policy;
  public enum Policy {
    HTTP_ONLY,
    HTTPS_ONLY,
    HTTP_AND_HTTPS;

    public Policy fromString(String value) {
      if (HTTPS_ONLY.name().equalsIgnoreCase(value)) {
        return HTTPS_ONLY;
      } else if (HTTP_AND_HTTPS.name().equalsIgnoreCase(value)) {
        return HTTP_AND_HTTPS;
      }
      return HTTP_ONLY;
    }

    public boolean isHttpEnabled() {
      return this == HTTP_ONLY || this == HTTP_AND_HTTPS;
    }

    public boolean isHttpsEnabled() {
      return this == HTTPS_ONLY || this == HTTP_AND_HTTPS;
    }
  }

   public HttpConfig(final Configuration conf) {
    boolean sslEnabled = conf.getBoolean(
      ServerConfigurationKeys.HBASE_SSL_ENABLED_KEY,
      ServerConfigurationKeys.HBASE_SSL_ENABLED_DEFAULT);
    policy = sslEnabled ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY;
    if (sslEnabled) {
      conf.addResource("ssl-server.xml");
      conf.addResource("ssl-client.xml");
    }
  }

  public void setPolicy(Policy policy) {
    this.policy = policy;
  }

  public boolean isSecure() {
    return policy == Policy.HTTPS_ONLY;
  }

  public String getSchemePrefix() {
    return (isSecure()) ? "https://" : "http://";
  }

  public String getScheme(Policy policy) {
    return policy == Policy.HTTPS_ONLY ? "https://" : "http://";
  }
}

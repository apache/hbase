/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.jetty;

import java.io.IOException;
import java.util.ArrayList;

import javax.net.ssl.SSLEngine;

import org.mortbay.jetty.security.SslSelectChannelConnector;

/**
 * Avoid SSL V3.0 "Poodle" Vulnerability - CVE-2014-3566
 */
public class SslSelectChannelConnectorSecure extends SslSelectChannelConnector {
  @Override
  protected SSLEngine createSSLEngine() throws IOException {
    SSLEngine sslEngine = super.createSSLEngine();
    ArrayList<String> secureProtocols = new ArrayList<String>();
    for (String p : sslEngine.getEnabledProtocols()) {
      if (!p.contains("SSLv3")) {
        secureProtocols.add(p);
      }
    }
    sslEngine.setEnabledProtocols(secureProtocols.toArray(new String[secureProtocols.size()]));
    return sslEngine;
  }
}

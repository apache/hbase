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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

import javax.net.ssl.SSLSocket;
import javax.rmi.ssl.SslRMIClientSocketFactory;

/**
 * Avoid SSL V3.0 "Poodle" Vulnerability - CVE-2014-3566
 */
@SuppressWarnings("serial")
public class SslRMIClientSocketFactorySecure extends SslRMIClientSocketFactory {
  @Override
  public Socket createSocket(String host, int port) throws IOException {
    SSLSocket socket = (SSLSocket) super.createSocket(host, port);
    ArrayList<String> secureProtocols = new ArrayList<String>();
    for (String p : socket.getEnabledProtocols()) {
      if (!p.contains("SSLv3")) {
        secureProtocols.add(p);
      }
    }
    socket.setEnabledProtocols(secureProtocols.toArray(
            new String[secureProtocols.size()]));
    return socket;
  }
}

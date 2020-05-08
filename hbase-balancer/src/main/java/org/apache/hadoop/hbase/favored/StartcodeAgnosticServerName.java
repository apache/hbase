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
package org.apache.hadoop.hbase.favored;

import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Addressing;

/**
 * This class differs from ServerName in that start code is always ignored. This is because
 * start code, ServerName.NON_STARTCODE is used to persist favored nodes and keeping this separate
 * from {@link ServerName} is much cleaner. This should only be used by Favored node specific
 * classes and should not be used outside favored nodes.
 */
@InterfaceAudience.Private
class StartcodeAgnosticServerName extends ServerName {

  public StartcodeAgnosticServerName(final String hostname, final int port, long startcode) {
    super(hostname, port, startcode);
  }

  public static StartcodeAgnosticServerName valueOf(final ServerName serverName) {
    return new StartcodeAgnosticServerName(serverName.getHostname(), serverName.getPort(),
        serverName.getStartcode());
  }

  public static StartcodeAgnosticServerName valueOf(final String hostnameAndPort, long startcode) {
    return new StartcodeAgnosticServerName(Addressing.parseHostname(hostnameAndPort),
        Addressing.parsePort(hostnameAndPort), startcode);
  }

  public static StartcodeAgnosticServerName valueOf(final HostAndPort hostnameAndPort, long startcode) {
    return new StartcodeAgnosticServerName(hostnameAndPort.getHost(),
      hostnameAndPort.getPort(), startcode);
  }

  @Override
  public int compareTo(ServerName other) {
    int compare = this.getHostname().compareTo(other.getHostname());
    if (compare != 0) return compare;
    compare = this.getPort() - other.getPort();
    if (compare != 0) return compare;
    return 0;
  }

  @Override
  public int hashCode() {
    return getAddress().hashCode();
  }

  // Do not need @Override #equals() because super.equals() delegates to compareTo(), which ends
  // up doing the right thing. We have a test for it, so the checkstyle warning here would be a
  // false positive.
}

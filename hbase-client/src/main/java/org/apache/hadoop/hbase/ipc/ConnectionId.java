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
package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

/**
 * This class holds the address and the user ticket, etc. The client connections
 * to servers are uniquely identified by &lt;remoteAddress, ticket, serviceName&gt;
 */
@InterfaceAudience.Private
class ConnectionId {
  private static final int PRIME = 16777619;
  final User ticket;
  final String serviceName;
  final InetSocketAddress address;

  public ConnectionId(User ticket, String serviceName, InetSocketAddress address) {
    this.address = address;
    this.ticket = ticket;
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return this.serviceName;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public User getTicket() {
    return ticket;
  }

  @Override
  public String toString() {
    return this.address.toString() + "/" + this.serviceName + "/" + this.ticket;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConnectionId) {
      ConnectionId id = (ConnectionId) obj;
      return address.equals(id.address) &&
            ((ticket != null && ticket.equals(id.ticket)) ||
             (ticket == id.ticket)) &&
             this.serviceName == id.serviceName;
    }
    return false;
  }

  @Override  // simply use the default Object#hashcode() ?
  public int hashCode() {
    return hashCode(ticket,serviceName,address);
  }

  public static int hashCode(User ticket, String serviceName, InetSocketAddress address){
    return (address.hashCode() +
        PRIME * (PRIME * serviceName.hashCode() ^
            (ticket == null ? 0 : ticket.hashCode())));
  }
}

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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

import java.net.InetSocketAddress;

/**
 * This class holds the address and the user ticket, etc. The client connections
 * to servers are uniquely identified by &lt;remoteAddress, ticket, serviceName&gt;
 */
@InterfaceAudience.Private
public class ConnectionId {
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
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((address == null) ? 0 : address.hashCode());
    result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
    result = prime * result + ((ticket == null) ? 0 : ticket.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ConnectionId other = (ConnectionId) obj;
    if (address == null) {
      if (other.address != null) {
        return false;
      }
    } else if (!address.equals(other.address)) {
      return false;
    }
    if (serviceName == null) {
      if (other.serviceName != null) {
        return false;
      }
    } else if (!serviceName.equals(other.serviceName)) {
      return false;
    }
    if (ticket == null) {
      if (other.ticket != null) {
        return false;
      }
    } else if (!ticket.equals(other.ticket)) {
      return false;
    }
    return true;
  }

  public static int hashCode(User ticket, String serviceName, InetSocketAddress address){
    return (address.hashCode() +
        PRIME * (PRIME * serviceName.hashCode() ^
            (ticket == null ? 0 : ticket.hashCode())));
  }
}

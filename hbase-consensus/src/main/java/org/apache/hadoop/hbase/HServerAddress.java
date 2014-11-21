/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

/**
 * HServerAddress is a "label" for a HBase server made of host and port number.
 */
public class HServerAddress implements WritableComparable<HServerAddress> {

  private static final Log LOG = LogFactory.getLog(HServerAddress.class);

  private InetSocketAddress address;
  private String stringValue;
  private String hostAddress;

  /** 
   * We don't expect the IP addresses of HBase servers to change, so we cache them
   * indefinitely. At this level we only do positive caching.
   */
  private static ConcurrentMap<String, InetSocketAddress> addressCache =
      new ConcurrentHashMap<String, InetSocketAddress>();

  public HServerAddress() {
    this.address = null;
    this.stringValue = null;
    this.hostAddress = null;
  }

  /**
   * Construct an instance from an {@link InetSocketAddress}.
   * @param address InetSocketAddress of server
   */
  public HServerAddress(InetSocketAddress address) {
    this.address = address;
    this.stringValue = getHostAddressWithPort();
    checkBindAddressCanBeResolved();
  }

  /**
   * @param hostAndPort Hostname and port formatted as <code>&lt;hostname> ':' &lt;port></code>
   */
  public HServerAddress(String hostAndPort) {
    int colonIndex = hostAndPort.lastIndexOf(':');
    if (colonIndex < 0) {
      throw new IllegalArgumentException("Not a host:port pair: " + hostAndPort);
    }
    String host = hostAndPort.substring(0, colonIndex);
    int port = Integer.parseInt(hostAndPort.substring(colonIndex + 1));
    address = addressCache.get(hostAndPort);
    if (address == null) {
      this.address = new InetSocketAddress(host, port);
      if (getBindAddress() != null) {
        // Resolved the hostname successfully, cache it.
        InetSocketAddress existingAddress = addressCache.putIfAbsent(hostAndPort, address);
        if (existingAddress != null) {
          // Another thread cached the address ahead of us, reuse it.
          this.address = existingAddress;
        }
      }
    }
    this.stringValue = getHostAddressWithPort();
    checkBindAddressCanBeResolved();
  }

  /**
   * @param bindAddress Hostname
   * @param port Port number
   */
  public HServerAddress(String bindAddress, int port) {
    this.address = new InetSocketAddress(bindAddress, port);
    this.stringValue = getHostAddressWithPort();
    checkBindAddressCanBeResolved();
  }

  /**
   * Copy-constructor.
   * @param other HServerAddress to copy from
   */
  public HServerAddress(HServerAddress other) {
    String bindAddress = other.getBindAddress();
    int port = other.getPort();
    this.address = new InetSocketAddress(bindAddress, port);
    stringValue = other.stringValue;
    checkBindAddressCanBeResolved();
  }
  
  /**
   * Get the normalized hostAddress:port as a string format
   * @param address
   * @return the normalized hostAddress:port as a string format
   */
  public String getHostAddressWithPort() {
    if (address == null) return null;
    return this.getBindAddress() + ":" + address.getPort();
  }

  /**
   * Get the normalized hostName:port as a string format
   * @param address
   * @return the normalized hostName:port as a string format
   */
  public String getHostNameWithPort() {
    if (address == null) return null;
    return address.getHostName() + ":" +
      address.getPort();
  }

  /** @return Bind address */
  public String getBindAddress() {
    if (this.hostAddress != null)
      return hostAddress;
    
    final InetAddress addr = address.getAddress();
    if (addr != null) {
      return addr.getHostAddress();
    } else {
      LOG.error("Could not resolve the DNS name of " + stringValue);
      return null;
    }
  }

  private void checkBindAddressCanBeResolved() {
    if ((this.hostAddress = getBindAddress()) == null) {
      throw new IllegalArgumentException("Could not resolve the"
          + " DNS name of " + stringValue);
    }
  }

  /** @return Port number */
  public int getPort() {
    return address.getPort();
  }

  /** @return Hostname */
  public String getHostname() {
    return address.getHostName();
  }

  /** @return The InetSocketAddress */
  public InetSocketAddress getInetSocketAddress() {
    return address;
  }

  /**
   * @return String formatted as <code>&lt;bind address> ':' &lt;port></code>
   */
  @Override
  public String toString() {
    return stringValue == null ? "" : stringValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    return compareTo((HServerAddress) o) == 0;
  }

  @Override
  public int hashCode() {
    return address.hashCode();
  }

  //
  // Writable
  //

  @Override
  public void readFields(DataInput in) throws IOException {
    String bindAddress = in.readUTF();
    int port = in.readInt();

    if (bindAddress == null || bindAddress.length() == 0) {
      address = null;
      stringValue = null;
    } else {
      address = new InetSocketAddress(bindAddress, port);
      stringValue = getHostAddressWithPort();
      checkBindAddressCanBeResolved();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (address == null) {
      out.writeUTF("");
      out.writeInt(0);
    } else {
      out.writeUTF(address.getAddress().getHostAddress());
      out.writeInt(address.getPort());
    }
  }

  //
  // Comparable
  //

  @Override
  public int compareTo(HServerAddress o) {
    if (address == null) return -1;
    // Addresses as Strings may not compare though address is for the one
    // server with only difference being that one address has hostname
    // resolved whereas other only has IP.
    if (address.equals(o.address)) return 0;
    return toString().compareTo(o.toString());
  }
}

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
package org.apache.hadoop.hbase.net;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;

/**
 * An immutable type to hold a hostname and port combo, like an Endpoint or
 * java.net.InetSocketAddress (but without danger of our calling resolve -- we do NOT want a resolve
 * happening every time we want to hold a hostname and port combo). This class is also
 * {@link Comparable}
 * <p>
 * In implementation this class is a facade over Guava's {@link HostAndPort}. We cannot have Guava
 * classes in our API hence this Type.
 */
@InterfaceAudience.Public
public class Address implements Comparable<Address> {
  private final HostAndPort hostAndPort;

  private Address(HostAndPort hostAndPort) {
    this.hostAndPort = hostAndPort;
  }

  public static Address fromParts(String hostname, int port) {
    return new Address(HostAndPort.fromParts(hostname, port));
  }

  public static Address fromString(String hostnameAndPort) {
    return new Address(HostAndPort.fromString(hostnameAndPort));
  }

  public static Address fromSocketAddress(InetSocketAddress addr) {
    return Address.fromParts(addr.getHostString(), addr.getPort());
  }

  public static InetSocketAddress toSocketAddress(Address addr) {
    return new InetSocketAddress(addr.getHostName(), addr.getPort());
  }

  public static InetSocketAddress[] toSocketAddress(Address[] addrs) {
    if (addrs == null) {
      return null;
    }
    InetSocketAddress[] result = new InetSocketAddress[addrs.length];
    for (int i = 0; i < addrs.length; i++) {
      result[i] = toSocketAddress(addrs[i]);
    }
    return result;
  }

  public String getHostName() {
    return this.hostAndPort.getHost();
  }

  /**
   * @deprecated Since 1.7.0, 2.4.18 and 2.5.0, will be removed in 4.0.0. Use {@link #getHostName()}
   *             instead.
   */
  @Deprecated
  public String getHostname() {
    return this.hostAndPort.getHost();
  }

  public int getPort() {
    return this.hostAndPort.getPort();
  }

  @Override
  public String toString() {
    return this.hostAndPort.toString();
  }

  /**
   * If hostname is a.b.c and the port is 123, return a:123 instead of a.b.c:123.
   * @return if host looks like it is resolved -- not an IP -- then strip the domain portion
   *         otherwise returns same as {@link #toString()}}
   */
  public String toStringWithoutDomain() {
    String hostname = getHostName();
    List<String> parts = Splitter.on('.').splitToList(hostname);
    if (parts.size() > 1) {
      Iterator<String> i = parts.iterator();
      String base = i.next();
      while (i.hasNext()) {
        String part = i.next();
        if (!StringUtils.isNumeric(part)) {
          return Address.fromParts(base, getPort()).toString();
        }
      }
    }
    return toString();
  }

  @Override
  // Don't use HostAndPort equals... It is wonky including
  // ipv6 brackets
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof Address) {
      Address that = (Address) other;
      return this.getHostName().equals(that.getHostName()) && this.getPort() == that.getPort();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getHostName().hashCode() ^ getPort();
  }

  @Override
  public int compareTo(Address that) {
    int compare = this.getHostName().compareTo(that.getHostName());
    if (compare != 0) {
      return compare;
    }

    return this.getPort() - that.getPort();
  }
}

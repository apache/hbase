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
package org.apache.hadoop.hbase;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Interner;
import org.apache.hbase.thirdparty.com.google.common.collect.Interners;
import org.apache.hbase.thirdparty.com.google.common.net.InetAddresses;

/**
 * Name of a particular incarnation of an HBase Server. A {@link ServerName} is used uniquely
 * identifying a server instance in a cluster and is made of the combination of hostname, port, and
 * startcode. The startcode distinguishes restarted servers on same hostname and port (startcode is
 * usually timestamp of server startup). The {@link #toString()} format of ServerName is safe to use
 * in the filesystem and as znode name up in ZooKeeper. Its format is:
 * <code>&lt;hostname&gt; '{@link #SERVERNAME_SEPARATOR}' &lt;port&gt;
 * '{@link #SERVERNAME_SEPARATOR}' &lt;startcode&gt;</code>. For example, if hostname is
 * <code>www.example.org</code>, port is <code>1234</code>, and the startcode for the regionserver
 * is <code>1212121212</code>, then the {@link #toString()} would be
 * <code>www.example.org,1234,1212121212</code>.
 * <p>
 * You can obtain a versioned serialized form of this class by calling {@link #getVersionedBytes()}.
 * To deserialize, call {@link #parseVersionedServerName(byte[])}.
 * <p>
 * Use {@link #getAddress()} to obtain the Server hostname + port (Endpoint/Socket Address).
 * <p>
 * Immutable.
 */
@InterfaceAudience.Public
public class ServerName implements Comparable<ServerName>, Serializable {
  private static final long serialVersionUID = 1367463982557264981L;

  /**
   * Version for this class. Its a short rather than a byte so I can for sure distinguish between
   * this version of this class and the version previous to this which did not have a version.
   */
  private static final short VERSION = 0;
  static final byte[] VERSION_BYTES = Bytes.toBytes(VERSION);
  private static final String COLON_ENCODED_VALUE = "%3a";

  // IPV6 address separated by COLON(:)
  public static final String COLON = ":";

  /**
   * What to use if no startcode supplied.
   */
  public static final int NON_STARTCODE = -1;

  /**
   * This character is used as separator between server hostname, port and startcode.
   */
  public static final String SERVERNAME_SEPARATOR = ",";

  public static final Pattern SERVERNAME_PATTERN =
    Pattern.compile("[^" + SERVERNAME_SEPARATOR + "]+" + SERVERNAME_SEPARATOR
      + Addressing.VALID_PORT_REGEX + SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX + "$");

  /**
   * What to use if server name is unknown.
   */
  public static final String UNKNOWN_SERVERNAME = "#unknown#";

  private final String serverName;
  private final long startCode;
  private transient Address address;

  /**
   * Cached versioned bytes of this ServerName instance.
   * @see #getVersionedBytes()
   */
  private byte[] bytes;
  public static final List<ServerName> EMPTY_SERVER_LIST = new ArrayList<>(0);

  /**
   * Intern ServerNames. The Set of ServerNames is mostly-fixed changing slowly as Servers restart.
   * Rather than create a new instance everytime, try and return existing instance if there is one.
   */
  private static final Interner<ServerName> INTERN_POOL = Interners.newWeakInterner();

  protected ServerName(final String hostname, final int port, final long startCode) {
    this(Address.fromParts(hostname, port), startCode);
  }

  private ServerName(final Address address, final long startCode) {
    // Use HostAndPort to host port and hostname. Does validation and can do ipv6
    this.address = address;
    this.startCode = startCode;
    this.serverName = getServerName(this.address.getHostname(), this.address.getPort(), startCode);
  }

  private ServerName(final String hostAndPort, final long startCode) {
    this(Address.fromString(hostAndPort), startCode);
  }

  /**
   * @param hostname the hostname string to get the actual hostname from
   * @return hostname minus the domain, if there is one (will do pass-through on ip addresses)
   */
  private static String getHostNameMinusDomain(final String hostname) {
    if (InetAddresses.isInetAddress(hostname)) {
      return hostname;
    }
    List<String> parts = Splitter.on('.').splitToList(hostname);
    if (parts.size() == 0) {
      return hostname;
    }
    Iterator<String> i = parts.iterator();
    return i.next();
  }

  /**
   * Retrieve an instance of ServerName. Callers should use the equals method to compare returned
   * instances, though we may return a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostname, final int port, final long startCode) {
    return INTERN_POOL.intern(new ServerName(hostname, port, startCode));
  }

  /**
   * Retrieve an instance of ServerName. Callers should use the equals method to compare returned
   * instances, though we may return a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String serverName) {
    int firstSep = serverName.indexOf(SERVERNAME_SEPARATOR);
    int lastSep = serverName.lastIndexOf(SERVERNAME_SEPARATOR);
    String hostname = serverName.substring(0, firstSep);
    int port = Integer.parseInt(serverName.substring(firstSep + 1, lastSep));
    long startCode = Long.parseLong(serverName.substring(lastSep + 1));
    return valueOf(hostname, port, startCode);
  }

  /**
   * Retrieve an instance of ServerName. Callers should use the equals method to compare returned
   * instances, though we may return a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostAndPort, final long startCode) {
    return INTERN_POOL.intern(new ServerName(hostAndPort, startCode));
  }

  /**
   * Retrieve an instance of {@link ServerName}. Callers should use the {@link #equals(Object)}
   * method to compare returned instances, though we may return a shared immutable object as an
   * internal optimization.
   * @param address   the {@link Address} to use for getting the {@link ServerName}
   * @param startCode the startcode to use for getting the {@link ServerName}
   * @return the constructed {@link ServerName}
   * @see #valueOf(String, int, long)
   */
  public static ServerName valueOf(final Address address, final long startCode) {
    return valueOf(address.getHostname(), address.getPort(), startCode);
  }

  @Override
  public String toString() {
    return getServerName();
  }

  /**
   * Return a SHORT version of {@link #toString()}, one that has the host only, minus the domain,
   * and the port only -- no start code; the String is for us internally mostly tying threads to
   * their server. Not for external use. It is lossy and will not work in in compares, etc.
   */
  public String toShortString() {
    return Addressing.createHostAndPortStr(getHostNameMinusDomain(this.address.getHostname()),
      this.address.getPort());
  }

  /**
   * Return {@link #getServerName()} as bytes with a short-sized prefix with the {@link #VERSION} of
   * this class.
   */
  public synchronized byte[] getVersionedBytes() {
    if (this.bytes == null) {
      this.bytes = Bytes.add(VERSION_BYTES, Bytes.toBytes(getServerName()));
    }
    return this.bytes;
  }

  public String getServerName() {
    return serverName;
  }

  public String getHostname() {
    return this.address.getHostname();
  }

  public String getHostnameLowerCase() {
    return this.address.getHostname().toLowerCase(Locale.ROOT);
  }

  public int getPort() {
    return this.address.getPort();
  }

  /**
   * Return the start code.
   * @deprecated Since 2.5.0, will be removed in 4.0.0. Use {@link #getStartCode()} instead.
   */
  @Deprecated
  public long getStartcode() {
    return startCode;
  }

  /** Return the start code. */
  public long getStartCode() {
    return startCode;
  }

  /**
   * For internal use only.
   * @param hostName  the name of the host to use
   * @param port      the port on the host to use
   * @param startCode the startcode to use for formatting
   * @return Server name made of the concatenation of hostname, port and startcode formatted as
   *         <code>&lt;hostname&gt; ',' &lt;port&gt; ',' &lt;startcode&gt;</code>
   */
  private static String getServerName(String hostName, int port, long startCode) {
    return hostName.toLowerCase(Locale.ROOT) + SERVERNAME_SEPARATOR + port + SERVERNAME_SEPARATOR
      + startCode;
  }

  public Address getAddress() {
    return this.address;
  }

  @Override
  public int compareTo(ServerName other) {
    int compare;
    if (other == null) {
      return -1;
    }
    if (this.getHostname() == null) {
      if (other.getHostname() != null) {
        return 1;
      }
    } else {
      if (other.getHostname() == null) {
        return -1;
      }
      compare = this.getHostname().compareToIgnoreCase(other.getHostname());
      if (compare != 0) {
        return compare;
      }
    }
    compare = this.getPort() - other.getPort();
    if (compare != 0) {
      return compare;
    }
    return Long.compare(this.getStartCode(), other.getStartCode());
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof ServerName)) {
      return false;
    }
    return this.compareTo((ServerName) o) == 0;
  }

  /**
   * Compare two addresses
   * @param left  the first server address to compare
   * @param right the second server address to compare
   * @return {@code true} if {@code left} and {@code right} have the same hostname and port.
   */
  public static boolean isSameAddress(final ServerName left, final ServerName right) {
    return left.getAddress().equals(right.getAddress());
  }

  /**
   * Use this method instantiating a {@link ServerName} from bytes gotten from a call to
   * {@link #getVersionedBytes()}. Will take care of the case where bytes were written by an earlier
   * version of hbase.
   * @param versionedBytes Pass bytes gotten from a call to {@link #getVersionedBytes()}
   * @return A ServerName instance.
   * @see #getVersionedBytes()
   */
  public static ServerName parseVersionedServerName(final byte[] versionedBytes) {
    // Version is a short.
    short version = Bytes.toShort(versionedBytes);
    if (version == VERSION) {
      int length = versionedBytes.length - Bytes.SIZEOF_SHORT;
      return valueOf(Bytes.toString(versionedBytes, Bytes.SIZEOF_SHORT, length));
    }
    // Presume the bytes were written with an old version of hbase and that the
    // bytes are actually a String of the form "'<hostname>' ':' '<port>'".
    return valueOf(Bytes.toString(versionedBytes), NON_STARTCODE);
  }

  /**
   * Parse a ServerName from a string
   * @param str Either an instance of {@link #toString()} or a "'&lt;hostname&gt;' ':'
   *            '&lt;port&gt;'".
   * @return A ServerName instance.
   */
  public static ServerName parseServerName(final String str) {
    ServerName sn =
      SERVERNAME_PATTERN.matcher(str).matches() ? valueOf(str) : valueOf(str, NON_STARTCODE);
    String hostname = sn.getHostname();
    // Decodes the WAL directory name back into a ServerName.
    // This reverses the URL encoding of IPv6 addresses done during path creation,
    // decoding "%3A" back to colons (:) before reconstructing ServerName.
    if (hostname.contains(COLON_ENCODED_VALUE)) {
      try {
        hostname = URLDecoder.decode(sn.getHostname(), StandardCharsets.UTF_8.name());
        return ServerName.valueOf(hostname, sn.getPort(), sn.getStartCode());
      } catch (UnsupportedEncodingException e) {
        throw new IllegalArgumentException("Exception occurred while decoding server name", e);
      }
    }
    return sn;
  }

  /**
   * Checks if the provided server address is formatted as an IPv6 address. This method uses Java's
   * InetAddress to parse the input and confirms if the address is an instance of Inet6Address for
   * robust validation instead of relying on string splitting.
   * @param serverAddress The address string to validate
   * @return true if the address is a valid IPv6 address; false otherwise.
   */
  public static boolean isIpv6ServerName(String serverAddress) {
    try {
      InetAddress address = InetAddress.getByName(serverAddress);
      return address instanceof Inet6Address && !address.getHostName().equals(serverAddress)
        && serverAddress.contains(COLON);
    } catch (UnknownHostException e) {
      return false; // Not a valid IP, let the logic fall through
    }
  }

  /**
   * Encodes the IPv6 address component of ServerName to escape colon characters which are not
   * allowed in HDFS paths (e.g., WAL directory names). This encoding ensures WAL paths are valid
   * and portable across filesystems.
   * @param serverName the ServerName instance containing IPv6 address.
   * @return encoded string safe for filesystem path usage.
   */
  public static ServerName getEncodedServerName(final String serverName) {
    ServerName sn = SERVERNAME_PATTERN.matcher(serverName).matches()
      ? valueOf(serverName)
      : valueOf(serverName, NON_STARTCODE);
    try {
      return ServerName.valueOf(URLEncoder.encode(sn.getHostname(), StandardCharsets.UTF_8.name()),
        sn.getPort(), sn.getStartCode());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Exception occurred while encoding server name", e);
    }
  }

  /** Returns true if the String follows the pattern of {@link #toString()}, false otherwise. */
  public static boolean isFullServerName(final String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    return SERVERNAME_PATTERN.matcher(str).matches();
  }
}

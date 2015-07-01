/**
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

import com.google.common.net.InetAddresses;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.MetaRegionServer;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Instance of an HBase ServerName.
 * A server name is used uniquely identifying a server instance in a cluster and is made
 * of the combination of hostname, port, and startcode.  The startcode distingushes restarted
 * servers on same hostname and port (startcode is usually timestamp of server startup). The
 * {@link #toString()} format of ServerName is safe to use in the  filesystem and as znode name
 * up in ZooKeeper.  Its format is:
 * <code>&lt;hostname> '{@link #SERVERNAME_SEPARATOR}' &lt;port> '{@link #SERVERNAME_SEPARATOR}' &lt;startcode></code>.
 * For example, if hostname is <code>www.example.org</code>, port is <code>1234</code>,
 * and the startcode for the regionserver is <code>1212121212</code>, then
 * the {@link #toString()} would be <code>www.example.org,1234,1212121212</code>.
 * 
 * <p>You can obtain a versioned serialized form of this class by calling
 * {@link #getVersionedBytes()}.  To deserialize, call {@link #parseVersionedServerName(byte[])}
 * 
 * <p>Immutable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ServerName implements Comparable<ServerName>, Serializable {
  private static final long serialVersionUID = 1367463982557264981L;

  /**
   * Version for this class.
   * Its a short rather than a byte so I can for sure distinguish between this
   * version of this class and the version previous to this which did not have
   * a version.
   */
  private static final short VERSION = 0;
  static final byte [] VERSION_BYTES = Bytes.toBytes(VERSION);

  /**
   * What to use if no startcode supplied.
   */
  public static final int NON_STARTCODE = -1;

  /**
   * This character is used as separator between server hostname, port and
   * startcode.
   */
  public static final String SERVERNAME_SEPARATOR = ",";

  public static final Pattern SERVERNAME_PATTERN =
    Pattern.compile("[^" + SERVERNAME_SEPARATOR + "]+" +
      SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX +
      SERVERNAME_SEPARATOR + Addressing.VALID_PORT_REGEX + "$");

  /**
   * What to use if server name is unknown.
   */
  public static final String UNKNOWN_SERVERNAME = "#unknown#";

  private final String servername;
  private final String hostnameOnly;
  private final int port;
  private final long startcode;

  /**
   * Cached versioned bytes of this ServerName instance.
   * @see #getVersionedBytes()
   */
  private byte [] bytes;
  public static final List<ServerName> EMPTY_SERVER_LIST = new ArrayList<ServerName>(0);

  private ServerName(final String hostname, final int port, final long startcode) {
    // Drop the domain is there is one; no need of it in a local cluster.  With it, we get long
    // unwieldy names.
    this.hostnameOnly = hostname;
    this.port = port;
    this.startcode = startcode;
    this.servername = getServerName(this.hostnameOnly, port, startcode);
  }

  /**
   * @param hostname
   * @return hostname minus the domain, if there is one (will do pass-through on ip addresses)
   */
  static String getHostNameMinusDomain(final String hostname) {
    if (InetAddresses.isInetAddress(hostname)) return hostname;
    String [] parts = hostname.split("\\.");
    if (parts == null || parts.length == 0) return hostname;
    return parts[0];
  }

  private ServerName(final String serverName) {
    this(parseHostname(serverName), parsePort(serverName),
      parseStartcode(serverName));
  }

  private ServerName(final String hostAndPort, final long startCode) {
    this(Addressing.parseHostname(hostAndPort),
      Addressing.parsePort(hostAndPort), startCode);
  }

  public static String parseHostname(final String serverName) {
    if (serverName == null || serverName.length() <= 0) {
      throw new IllegalArgumentException("Passed hostname is null or empty");
    }
    if (!Character.isLetterOrDigit(serverName.charAt(0))) {
      throw new IllegalArgumentException("Bad passed hostname, serverName=" + serverName);
    }
    int index = serverName.indexOf(SERVERNAME_SEPARATOR);
    return serverName.substring(0, index);
  }

  public static int parsePort(final String serverName) {
    String [] split = serverName.split(SERVERNAME_SEPARATOR);
    return Integer.parseInt(split[1]);
  }

  public static long parseStartcode(final String serverName) {
    int index = serverName.lastIndexOf(SERVERNAME_SEPARATOR);
    return Long.parseLong(serverName.substring(index + 1));
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostname, final int port, final long startcode) {
    return new ServerName(hostname, port, startcode);
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String serverName) {
    return new ServerName(serverName);
  }

  /**
   * Retrieve an instance of ServerName.
   * Callers should use the equals method to compare returned instances, though we may return
   * a shared immutable object as an internal optimization.
   */
  public static ServerName valueOf(final String hostAndPort, final long startCode) {
    return new ServerName(hostAndPort, startCode);
  }

  @Override
  public String toString() {
    return getServerName();
  }

  /**
   * @return Return a SHORT version of {@link ServerName#toString()}, one that has the host only,
   * minus the domain, and the port only -- no start code; the String is for us internally mostly
   * tying threads to their server.  Not for external use.  It is lossy and will not work in
   * in compares, etc.
   */
  public String toShortString() {
    return Addressing.createHostAndPortStr(getHostNameMinusDomain(this.hostnameOnly), this.port);
  }

  /**
   * @return {@link #getServerName()} as bytes with a short-sized prefix with
   * the ServerName#VERSION of this class.
   */
  public synchronized byte [] getVersionedBytes() {
    if (this.bytes == null) {
      this.bytes = Bytes.add(VERSION_BYTES, Bytes.toBytes(getServerName()));
    }
    return this.bytes;
  }

  public String getServerName() {
    return servername;
  }

  public String getHostname() {
    return hostnameOnly;
  }

  public int getPort() {
    return port;
  }

  public long getStartcode() {
    return startcode;
  }

  /**
   * For internal use only.
   * @param hostName
   * @param port
   * @param startcode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  static String getServerName(String hostName, int port, long startcode) {
    final StringBuilder name = new StringBuilder(hostName.length() + 1 + 5 + 1 + 13);
    name.append(hostName.toLowerCase());
    name.append(SERVERNAME_SEPARATOR);
    name.append(port);
    name.append(SERVERNAME_SEPARATOR);
    name.append(startcode);
    return name.toString();
  }

  /**
   * @param hostAndPort String in form of &lt;hostname> ':' &lt;port>
   * @param startcode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public static String getServerName(final String hostAndPort,
      final long startcode) {
    int index = hostAndPort.indexOf(":");
    if (index <= 0) throw new IllegalArgumentException("Expected <hostname> ':' <port>");
    return getServerName(hostAndPort.substring(0, index),
      Integer.parseInt(hostAndPort.substring(index + 1)), startcode);
  }

  /**
   * @return Hostname and port formatted as described at
   * {@link Addressing#createHostAndPortStr(String, int)}
   */
  public String getHostAndPort() {
    return Addressing.createHostAndPortStr(this.hostnameOnly, this.port);
  }

  /**
   * @param serverName ServerName in form specified by {@link #getServerName()}
   * @return The server start code parsed from <code>servername</code>
   */
  public static long getServerStartcodeFromServerName(final String serverName) {
    int index = serverName.lastIndexOf(SERVERNAME_SEPARATOR);
    return Long.parseLong(serverName.substring(index + 1));
  }

  /**
   * Utility method to excise the start code from a server name
   * @param inServerName full server name
   * @return server name less its start code
   */
  public static String getServerNameLessStartCode(String inServerName) {
    if (inServerName != null && inServerName.length() > 0) {
      int index = inServerName.lastIndexOf(SERVERNAME_SEPARATOR);
      if (index > 0) {
        return inServerName.substring(0, index);
      }
    }
    return inServerName;
  }

  @Override
  public int compareTo(ServerName other) {
    int compare = this.getHostname().compareToIgnoreCase(other.getHostname());
    if (compare != 0) return compare;
    compare = this.getPort() - other.getPort();
    if (compare != 0) return compare;
    return (int)(this.getStartcode() - other.getStartcode());
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof ServerName)) return false;
    return this.compareTo((ServerName)o) == 0;
  }

  /**
   * @param left
   * @param right
   * @return True if <code>other</code> has same hostname and port.
   */
  public static boolean isSameHostnameAndPort(final ServerName left,
      final ServerName right) {
    if (left == null) return false;
    if (right == null) return false;
    return left.getHostname().compareToIgnoreCase(right.getHostname()) == 0 &&
      left.getPort() == right.getPort();
  }

  /**
   * Use this method instantiating a {@link ServerName} from bytes
   * gotten from a call to {@link #getVersionedBytes()}.  Will take care of the
   * case where bytes were written by an earlier version of hbase.
   * @param versionedBytes Pass bytes gotten from a call to {@link #getVersionedBytes()}
   * @return A ServerName instance.
   * @see #getVersionedBytes()
   */
  public static ServerName parseVersionedServerName(final byte [] versionedBytes) {
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
   * @param str Either an instance of {@link ServerName#toString()} or a
   * "'<hostname>' ':' '<port>'".
   * @return A ServerName instance.
   */
  public static ServerName parseServerName(final String str) {
    return SERVERNAME_PATTERN.matcher(str).matches()? valueOf(str) :
        valueOf(str, NON_STARTCODE);
  }


  /**
   * @return true if the String follows the pattern of {@link ServerName#toString()}, false
   *  otherwise.
   */
  public static boolean isFullServerName(final String str){
    if (str == null ||str.isEmpty()) return false;
    return SERVERNAME_PATTERN.matcher(str).matches();
  }

  /**
   * Get a ServerName from the passed in data bytes.
   * @param data Data with a serialize server name in it; can handle the old style
   * servername where servername was host and port.  Works too with data that
   * begins w/ the pb 'PBUF' magic and that is then followed by a protobuf that
   * has a serialized {@link ServerName} in it.
   * @return Returns null if <code>data</code> is null else converts passed data
   * to a ServerName instance.
   * @throws DeserializationException 
   */
  public static ServerName parseFrom(final byte [] data) throws DeserializationException {
    if (data == null || data.length <= 0) return null;
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int prefixLen = ProtobufUtil.lengthOfPBMagic();
      try {
        ZooKeeperProtos.Master rss =
          ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
        org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName sn = rss.getMaster();
        return valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
      } catch (InvalidProtocolBufferException e) {
        // A failed parse of the znode is pretty catastrophic. Rather than loop
        // retrying hoping the bad bytes will changes, and rather than change
        // the signature on this method to add an IOE which will send ripples all
        // over the code base, throw a RuntimeException.  This should "never" happen.
        // Fail fast if it does.
        throw new DeserializationException(e);
      }
    }
    // The str returned could be old style -- pre hbase-1502 -- which was
    // hostname and port seperated by a colon rather than hostname, port and
    // startcode delimited by a ','.
    String str = Bytes.toString(data);
    int index = str.indexOf(ServerName.SERVERNAME_SEPARATOR);
    if (index != -1) {
      // Presume its ServerName serialized with versioned bytes.
      return ServerName.parseVersionedServerName(data);
    }
    // Presume it a hostname:port format.
    String hostname = Addressing.parseHostname(str);
    int port = Addressing.parsePort(str);
    return valueOf(hostname, port, -1L);
  }
}

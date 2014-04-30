/**
 * Copyright 2010 The Apache Software Foundation
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
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * HServerInfo is meta info about an {@link HRegionServer}.  It is the token
 * by which a master distingushes a particular regionserver from the rest.
 * It holds hostname, ports, regionserver startcode, and load.  Each server has
 * a <code>servername</code> where servername is made up of a concatenation of
 * hostname, port, and regionserver startcode.  This servername is used in
 * various places identifying this regionserver.  Its even used as part of
 * a pathname in the filesystem.  As part of the initialization,
 * master will pass the regionserver the address that it knows this regionserver
 * by.  In subsequent communications, the regionserver will pass a HServerInfo
 * with the master-supplied address.
 */
@ThriftStruct
public class HServerInfo implements WritableComparable<HServerInfo> {

  private static final Log LOG = LogFactory.getLog(HServerInfo.class);

  /*
   * This character is used as separator between server hostname and port and
   * its startcode. Servername is formatted as
   * <code>&lt;hostname> '{@ink #SERVERNAME_SEPARATOR"}' &lt;port> '{@ink #SERVERNAME_SEPARATOR"}' &lt;startcode></code>.
   */
  static final String SERVERNAME_SEPARATOR = ",";

  // RE to match startcode, hostname and port will be parsed by HostAndPort and InternetDomainName
  private static final Pattern SERVER_START_CODE_RE = Pattern.compile(
      "-?[0-9]{1," + String.valueOf(Long.MAX_VALUE).length() + "}");

  private HServerAddress serverAddress;
  // start time of the regionserver
  private long startCode;
  private HServerLoad load;
  // Servername is made of hostname, port and startcode.
  private String serverName = null;
  // Hostname of the regionserver.
  private String hostname;
  private String cachedHostnamePort = null;
  private boolean sendSequenceIds = true;

  // For each region, store the last sequence id that was flushed
  // from MemStore to an HFile
  private SortedMap<byte[], Long> flushedSequenceIdByRegion =
      new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  public HServerInfo() {
    this(new HServerAddress(), 0, "default name");
  }

  /**
   * Constructor that creates a HServerInfo with a generated startcode and an
   * empty load.
   * @param serverAddress An {@link InetSocketAddress} encased in a {@link Writable}
   * @param infoPort Port the webui runs on.
   * @param hostname Server hostname.
   */
  public HServerInfo(HServerAddress serverAddress, final String hostname) {
    this(serverAddress, System.currentTimeMillis(), hostname);
  }

  /** Initializes from server address and start code. Used in unit tests. */
  HServerInfo(HServerAddress serverAddress, long startCode) {
    this(serverAddress, startCode, serverAddress.getHostname());
  }

  /** Auto-generates a start code. */
  public HServerInfo(final HServerAddress serverAddress) {
    this(serverAddress, 0, serverAddress.getHostname());
  }

  public HServerInfo(HServerAddress serverAddress, long startCode, String hostname) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.load = new HServerLoad();
    this.hostname = hostname;
    }

  @ThriftConstructor
  public HServerInfo(
      @ThriftField(1) HServerAddress serverAddress,
      @ThriftField(2) long startCode,
      @ThriftField(3) String hostname,
      @ThriftField(4) Map<byte[], Long> flushedSequenceIdByRegion,
      @ThriftField(5) boolean sendSequenceIds,
      @ThriftField(6) String cachedHostnamePort,
      @ThriftField(7) HServerLoad load) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.hostname = hostname;
    this.flushedSequenceIdByRegion = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
    this.flushedSequenceIdByRegion.putAll(flushedSequenceIdByRegion);
    this.sendSequenceIds = sendSequenceIds;
    this.cachedHostnamePort = cachedHostnamePort;
    this.load = load;
  }

  /**
   * Copy-constructor
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.load = other.getLoad();
    this.hostname = other.hostname;
    this.flushedSequenceIdByRegion.putAll(other.flushedSequenceIdByRegion);
  }

  @ThriftField(7)
  public HServerLoad getLoad() {
    return load;
  }

  public void setLoad(HServerLoad load) {
    this.load = load;
  }

  @ThriftField(1)
  public synchronized HServerAddress getServerAddress() {
    return new HServerAddress(serverAddress);
  }

  public synchronized void setServerAddress(HServerAddress serverAddress) {
    this.serverAddress = serverAddress;
    this.serverName = null;
  }

  @ThriftField(2)
  public synchronized long getStartCode() {
    return startCode;
  }

  @ThriftField(3)
  public String getHostname() {
    return this.hostname;
  }

  @ThriftField(4)
  public Map<byte[], Long> getFlushedSequenceIdByRegion() {
    return this.flushedSequenceIdByRegion;
  }



  public void setFlushedSequenceIdForRegion(byte[] region, long sequenceId) {
    flushedSequenceIdByRegion.put(region, sequenceId);
  }

  public long getFlushedSequenceIdForRegion(byte[] region) {
    return flushedSequenceIdByRegion.get(region);
  }

  /**
   * @return The hostname and port concatenated with a ':' as separator.
   */
  public synchronized String getHostnamePort() {
    if (this.cachedHostnamePort == null) {
      this.cachedHostnamePort = getHostnamePort(this.hostname, this.serverAddress.getPort());
    }
    return this.cachedHostnamePort;
  }

  @ThriftField(6)
  public String getCachedHostnamePort() {
    return this.cachedHostnamePort;
  }

  /**
   * @param hostname
   * @param port
   * @return The hostname and port concatenated with a ':' as separator.
   */
  public static String getHostnamePort(final String hostname, final int port) {
    return hostname + ":" + port;
  }

  /**
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public synchronized String getServerName() {
    if (this.serverName == null) {
      this.serverName = getServerName(this.hostname,
        this.serverAddress.getPort(), this.startCode);
    }
    return this.serverName;
  }

  public static synchronized String getServerName(final String hostAndPort,
      final long startcode) {
    // use lastIndexOf to comply with IPv6 addresses
    int index = hostAndPort.lastIndexOf(":");
    if (index <= 0) throw new IllegalArgumentException("Expected <hostname> ':' <port>");
    return getServerName(hostAndPort.substring(0, index),
      Integer.parseInt(hostAndPort.substring(index + 1)), startcode);
  }


  /**
   * @param hostName
   * @param port
   * @param startCode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public static String getServerName(String hostName, int port, long startCode) {
    StringBuilder name = new StringBuilder(hostName);
    name.append(SERVERNAME_SEPARATOR);
    name.append(port);
    name.append(SERVERNAME_SEPARATOR);
    name.append(startCode);
    return name.toString();
  }

  public void setSendSequenceIds(boolean sendSequenceIds) {
    this.sendSequenceIds = sendSequenceIds;
  }
  
  @ThriftField(5)
  public boolean getSendSequenceIds() {
    return this.sendSequenceIds;
  }

  /**
   * @return ServerName and load concatenated.
   * @see #getServerName()
   * @see #getLoad()
   */
  @Override
  public String toString() {
    return "serverName=" + getServerName() +
      ", load=(" + this.load.toString() + ")";
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
    return compareTo((HServerInfo)obj) == 0;
  }

  @Override
  public int hashCode() {
    return this.getServerName().hashCode();
  }

  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
    in.readInt();
    this.hostname = in.readUTF();
    if (sendSequenceIds) {
      HbaseMapWritable<byte[], Long> sequenceIdsWritable =
          new HbaseMapWritable<byte[], Long>(flushedSequenceIdByRegion);
      sequenceIdsWritable.readFields(in);
    }
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
    // Still serializing the info port for backward compatibility but it is not used.
    out.writeInt(HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    out.writeUTF(hostname);
    if (sendSequenceIds) {
      HbaseMapWritable<byte[], Long> sequenceIdsWritable =
          new HbaseMapWritable<byte[], Long>(flushedSequenceIdByRegion);
      sequenceIdsWritable.write(out);
    }
  }

  public int compareTo(HServerInfo o) {
    return this.getServerName().compareTo(o.getServerName());
  }

  /**
   * Utility method that does a find of a servername or a hostandport combination
   * in the passed Set.
   * @param servers Set of server names
   * @param serverName Name to look for
   * @param hostAndPortOnly If <code>serverName</code> is a
   * <code>hostname ':' port</code>
   * or <code>hostname , port , startcode</code>.
   * @return true if <code>serverName</code> found in <code>servers</code>
   */
  public static boolean isServer(final Set<String> servers,
      final String serverName, final boolean hostAndPortOnly) {
    if (!hostAndPortOnly) return servers.contains(serverName);
    String serverNameColonReplaced =
      serverName.replaceFirst(":", SERVERNAME_SEPARATOR);
    for (String hostPortStartCode: servers) {
      int index = hostPortStartCode.lastIndexOf(SERVERNAME_SEPARATOR);
      String hostPortStrippedOfStartCode = hostPortStartCode.substring(0, index);
      if (hostPortStrippedOfStartCode.equals(serverNameColonReplaced)) return true;
    }
    return false;
  }

  /**
   * Parses a server name of the format
   * <code>&lt;hostname>,&lt;port>,&lt;startcode></code>. We use this when
   * interpreting znode names.
   */
  public static HServerInfo fromServerName(String serverName)
      throws IllegalArgumentException {
    String[] components = serverName.split(SERVERNAME_SEPARATOR);
    if (components.length != 3) {
      String msg = "Invalid number of components in server name: " + serverName;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    String hostName = components[0];
    
    int port;
    long startCode;
    try {
      port = Integer.valueOf(components[1]);
      startCode = Long.valueOf(components[2]); 
    } catch (NumberFormatException ex) {
      String msg = "Could not parse server port or start code in server name: " + serverName;
      throw new IllegalArgumentException(msg);
    }

    return new HServerInfo(new HServerAddress(hostName, port), startCode, hostName);
  }

  public static boolean isValidServerName(String serverName) {
    String[] parts = serverName.split(SERVERNAME_SEPARATOR);
    if (parts.length == 3 && isValidHostAndPort(parts[0] + ":" + parts[1])) {
      // check the other parts
      return SERVER_START_CODE_RE.matcher(parts[2]).matches();
    }
    return false;
  }

  public static boolean isValidHostAndPort(String hostPort) {
    try {
      int portIndex = hostPort.lastIndexOf(":");
      // HostAndPort.fromString will treat multiple ":" as host-only string
      // need to separate port number and use HostAndPort.fromParts
      HostAndPort hostAndPort = HostAndPort.fromParts(hostPort.substring(0, portIndex),
          Integer.valueOf(hostPort.substring(portIndex+1)));
      // HostAndPort will guarantee port validity
      // check hostname sanity
      try {
        String hostname = hostAndPort.getHostText();
        // chop off zone index part b/c InetAddresses does not parse that
        if (hostname.contains("%")) {
          hostname = hostname.substring(0, hostname.indexOf("%"));
        }
        InetAddresses.forString(hostname);
      } catch (IllegalArgumentException e) {
        // not an ip string
        try {
          // not using HostSpecifier because it requires a public suffix
          InternetDomainName.from(hostAndPort.getHostText());
        } catch (IllegalArgumentException e1) {
          return false;
        }
      } catch (Exception e) {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  public static HServerAddress getAddress(HServerInfo hsi) {
    return hsi != null ? hsi.getServerAddress() : null;
  }

  /**
   * If the serverName represents the local machine and
   *    there is an global ipv6 address for the host
   * return the global ipv6 address
   * otherwise return the serverName itself
   * @param serverName
   * @return
   * @throws SocketException
   */
  public static String getIPv6AddrIfLocalMachine(String serverName) throws SocketException {
    // if it is null or already an IPv6 address, return it
    if (serverName == null || serverName.contains(":")) {
      return serverName;
    }
    // For all nics get all hostnames and IPs
    List<String> ips = new ArrayList<String>();
    List<String> ip6s = new ArrayList<String>();
    Enumeration<?> nics = NetworkInterface.getNetworkInterfaces();
    while(nics.hasMoreElements()) {
      Enumeration<?> rawAdrs =
        ((NetworkInterface)nics.nextElement()).getInetAddresses();
      while(rawAdrs.hasMoreElements()) {
        InetAddress inet = (InetAddress) rawAdrs.nextElement();
        ips.add(inet.getHostName());
        String hostAddr = inet.getHostAddress();
        ips.add(hostAddr);
        // Record global IPv6 address
        if (hostAddr.contains(":") && !inet.isLinkLocalAddress()) {
          if (hostAddr.contains("%")) {
            ips.add(hostAddr.substring(0, hostAddr.indexOf("%")));
            ip6s.add(hostAddr.substring(0, hostAddr.indexOf("%")));
          } else {
            ip6s.add(hostAddr);
          }
        }
      }
    }
    // if on local machine and has global ipv6 address,
    // use IPv6 address
    if (!ip6s.isEmpty() &&
      (serverName.equals("localhost") || ips.contains(serverName))) {
      return ip6s.get(0);
    }
    // return the original serverName
    return serverName;
  }
}

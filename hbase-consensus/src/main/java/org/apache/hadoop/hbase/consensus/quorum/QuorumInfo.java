package org.apache.hadoop.hbase.consensus.quorum;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.nio.ByteBuffer;

public class QuorumInfo {
  public static final int PAYLOAD_HEADER_SIZE =
          + Bytes.SIZEOF_BYTE   // Magic value
          + Bytes.SIZEOF_BYTE   // Payload type
          + Bytes.SIZEOF_BYTE;  // Payload version

  // For compatability with non-hydrabase mode
  public static String LOCAL_DC_KEY = "LOCAL_DC_KEY_FOR_NON_HYDRABASE_MODE";

  private Map<String, Map<HServerAddress, Integer>> peers = null;
  private Map<HServerAddress, Integer> peersWithRank = null;
  private Set<String> peersAsString = null;
  private final String quorumName;

  public QuorumInfo(final Map<String, Map<HServerAddress, Integer>> peers,
                    final String quorumName) {
    this.peers = peers;
    this.quorumName = quorumName;
    populateInternalMaps();
  }

  public QuorumInfo(final QuorumInfo info) {
    this.quorumName = info.quorumName;
    peers = new HashMap<>();
    for (String domain : info.getPeers().keySet()) {
      final Map<HServerAddress, Integer> peersInDomain = new HashMap<>();
      for (HServerAddress peer : info.getPeers().get(domain).keySet()) {
        peersInDomain.put(new HServerAddress(peer.getHostname(), peer.getPort()),
          info.getPeersWithRank().get(peer));
      }
      peers.put(domain, peersInDomain);
    }
    populateInternalMaps();
  }

  public int getQuorumSize() {
    return (peers == null  ? 0 : peers.values().iterator().next().size());
  }

  public Map<HServerAddress,Integer> getPeersWithRank() {
    return peersWithRank;
  }

  public Set<String> getPeersAsString() {
    return peersAsString;
  }

  public Map<HServerAddress, String> getPeersWithCluster() {
    if (peers != null) {
      // TODO: Consider cache this map instead of computing it every time
      Map<HServerAddress, String> peersWithCluster = new TreeMap<HServerAddress, String>();
      for (Map.Entry<String, Map<HServerAddress, Integer>> entry : peers
        .entrySet()) {
        String cluster = entry.getKey();
        for (HServerAddress serverAddress : entry.getValue().keySet()) {
          peersWithCluster.put(serverAddress, cluster);
        }
      }
      return peersWithCluster;
    }
    return null;
  }

  public Map<String, Map<HServerAddress, Integer>> getPeers() {
    return peers;
  }

  public void setPeers(Map<String, Map<HServerAddress, Integer>> peers) {
    this.peers = peers;
    populateInternalMaps();
  }

  public String getQuorumName() {
    return quorumName;
  }

  public static ByteBuffer serializeToBuffer(final List<QuorumInfo> configs) {
    final ByteBuffer payload = ByteBuffer.allocate(
      getPayloadSize(configs) + PAYLOAD_HEADER_SIZE);

    // Write the MAGIC VALUE
    payload.put(HConstants.CONSENSUS_PAYLOAD_MAGIC_VALUE);

    // Write that the payload is Quorum Membership Change
    payload.put(HConstants.QUORUM_MEMBERSHIP_CHANGE_TYPE);

    // Write the version of Quorum Membership Change
    payload.put(HConstants.QUORUM_MEMBERSHIP_CHANGE_VERSION);

    // Write the total number of WALEdits
    payload.putInt(configs.size());

    byte[] quorumName, dcName, currPeerInfo = null;
    for (QuorumInfo s : configs) {
      // Quorum Name
      quorumName = s.getQuorumName().getBytes();
      payload.putInt(quorumName.length);
      payload.put(quorumName);

      // Num of DC's
      payload.putInt(s.getPeers().size());
      for (String dc : s.getPeers().keySet()) {
        // DC Name
        dcName = dc.getBytes();
        payload.putInt(dcName.length);
        payload.put(dcName);

        Set<HServerAddress> numPeers = s.getPeers().get(dc).keySet();

        // Number of peers
        payload.putInt(numPeers.size());

        for (HServerAddress peer : numPeers) {
          // Peer Info
          currPeerInfo = peer.getHostAddressWithPort().getBytes();
          payload.putInt(currPeerInfo.length);

          payload.put(currPeerInfo);

          // Peer Rank
          payload.putInt(s.getPeers().get(dc).get(peer));
        }
      }
    }

    payload.flip();
    return payload;
  }

  /**
   * This method reads the ByteBuffer and returns valid List of QuorumInfo objects.
   * This method assumes that the contents of the ByteBuffer are immutable.
   *
   * This method does not modify the members of the ByteBuffer like position,
   * limit, mark and capacity. It should be thread safe.
   * @param data
   * @return
   */
  public static List<QuorumInfo> deserializeFromByteBuffer(final ByteBuffer data) {
    if (!isQuorumChangeRequest(data)) {
      return null;
    }

    // The check above already read the magic value and type fields, so move on
    // to the version field.
    int currOffset = data.position() + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_BYTE;

    // Read the version
    if (data.get(currOffset) != HConstants.QUORUM_MEMBERSHIP_CHANGE_VERSION) {
      return null;
    }
    currOffset += Bytes.SIZEOF_BYTE;

    int numConfigs = data.getInt(currOffset);

    currOffset += Bytes.SIZEOF_INT;
    List<QuorumInfo> configs = new ArrayList<>();

    int numDCs, numPeers, quorumNameLength, dcNameLength, peerNameLength = 0;
    String quorumName, dcName, peerName;
    Map<String, Map<HServerAddress, Integer>> dcLevelInfo;
    Map<HServerAddress, Integer> perDCPeersMap;

    for (int confIndex = 0; confIndex < numConfigs; ++confIndex) {

      // Quorum Name
      quorumNameLength = data.getInt(currOffset);
      currOffset += Bytes.SIZEOF_INT;

      quorumName = new String(data.array(), data.arrayOffset() + currOffset,
        quorumNameLength);
      currOffset += quorumNameLength;

      numDCs = data.getInt(currOffset);
      currOffset += Bytes.SIZEOF_INT;

      // Initialize the dc map
      dcLevelInfo = new HashMap<>(numDCs);

      for (int dcIndex = 0; dcIndex < numDCs; ++dcIndex) {

        // DC Name
        dcNameLength = data.getInt(currOffset);
        currOffset += Bytes.SIZEOF_INT;

        dcName = new String(data.array(), data.arrayOffset() + currOffset,
          dcNameLength);
        currOffset += dcNameLength;

        // Num of peers in this DC
        numPeers = data.getInt(currOffset);
        currOffset += Bytes.SIZEOF_INT;

        // Initialize the peerMap
        perDCPeersMap = new HashMap<>(numPeers);
        for (int peerIndex = 0; peerIndex < numPeers; ++peerIndex) {
          // Peer Name
          peerNameLength = data.getInt(currOffset);
          currOffset += Bytes.SIZEOF_INT;

          peerName = new String(data.array(), data.arrayOffset() + currOffset, peerNameLength);
          currOffset += peerNameLength;

          // Put the peer name and rank in the peer Map
          perDCPeersMap.put(new HServerAddress(peerName), data.getInt(currOffset));
          currOffset += Bytes.SIZEOF_INT;
        }

        // add the dc info to map
        dcLevelInfo.put(dcName, perDCPeersMap);
      }

      // add the config to the list of configs to return
      configs.add(new QuorumInfo(dcLevelInfo, quorumName));
    }
    return configs;
  }

  public static int getPayloadSize(final List<QuorumInfo> configs) {
    // Number of Lists
    int size = Bytes.SIZEOF_INT;

    for (QuorumInfo s : configs) {
      // Quorum Name length
      size += Bytes.SIZEOF_INT;
      size += s.getQuorumName().length();

      // Num of DC's
      size += Bytes.SIZEOF_INT;

      for (String dc : s.getPeers().keySet()) {
        // DC Name length
        size += Bytes.SIZEOF_INT;
        size += dc.getBytes().length;

        Set<HServerAddress> numPeers = s.getPeers().get(dc).keySet();

        // Number of peers
        size += Bytes.SIZEOF_INT;

        for (HServerAddress peer : numPeers) {
          // Peer Address in String format
          size += Bytes.SIZEOF_INT;
          size += peer.getHostAddressWithPort().length();
          // Peer Rank
          size += Bytes.SIZEOF_INT;
        }
      }
    }
    return size;
  }

  /**
   * Test whether the given buffer contains a quorum change request. This method
   * does not change the position pointer while reading the buffer data.
   * @param data buffer containing a possible quorum change request
   * @return true if the buffer contains a change request, false otherwise
   */
  public static boolean isQuorumChangeRequest(final ByteBuffer data) {
    int currOffset = data.position();

    // Read the Magic Value
    if (data.remaining() < PAYLOAD_HEADER_SIZE ||
      data.get(currOffset) != HConstants.CONSENSUS_PAYLOAD_MAGIC_VALUE) {
      return false;
    }
    currOffset += Bytes.SIZEOF_BYTE;

    // Read the type
    if (data.get(currOffset) != HConstants.QUORUM_MEMBERSHIP_CHANGE_TYPE) {
      return false;
    }
    return true;
  }

  public void refresh() {
    populateInternalMaps();
  }

  private void populateInternalMaps() {
    if (peers != null) {
      peersAsString = new HashSet<>();
      peersWithRank = new TreeMap<>();
      for (Map<HServerAddress, Integer> map : peers.values()) {
        peersWithRank.putAll(map);
        for (HServerAddress peer : map.keySet()) {
          peersAsString.add(RaftUtil.getLocalConsensusAddress(peer).getHostAddressWithPort());
        }
      }
    }
  }

  public String getDomain(final String serverAddr) {
    String domain = "";
    for (String c : peers.keySet()) {
      for (HServerAddress peer : peers.get(c).keySet()) {
        if (serverAddr.equals(peer.getHostAddressWithPort())) {
          domain = c;
          break;
        }
      }
    }
    return domain;
  }

  public int getRank(final HServerAddress address) {
    int rank = 0;
    if (peersWithRank.containsKey(address)) {
      rank = peersWithRank.get(address);
    }
    return rank;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof QuorumInfo)) {
      return false;
    }
    QuorumInfo that = (QuorumInfo)o;
    if (!this.quorumName.equals(that.quorumName)) {
      return false;
    }
    if (!this.peers.equals(that.peers)) {
      return false;
    }
    if (!this.peersAsString.equals(that.peersAsString)) {
      return false;
    }
    if (!this.peersWithRank.equals(that.peersWithRank)) {
      return false;
    }
    return true;
  }

  public boolean hasEqualReplicaSet(final QuorumInfo that) {
    return this.peersWithRank.keySet().equals(that.peersWithRank.keySet());
  }

  @Override
  public String toString() {
    return String.format("{ Quorum Name = %s, peersWithRank = %s }",
      getQuorumName(), peersWithRank);
  }
}

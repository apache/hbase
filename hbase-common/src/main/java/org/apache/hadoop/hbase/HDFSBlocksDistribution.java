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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Data structure to describe the distribution of HDFS blocks among hosts.
 *
 * Adding erroneous data will be ignored silently.
 */
@InterfaceAudience.Private
public class HDFSBlocksDistribution {
  private Map<String,HostAndWeight> hostAndWeights = null;
  private long uniqueBlocksTotalWeight = 0;

  /**
   * Stores the hostname and weight for that hostname.
   *
   * This is used when determining the physical locations of the blocks making
   * up a region.
   *
   * To make a prioritized list of the hosts holding the most data of a region,
   * this class is used to count the total weight for each host.  The weight is
   * currently just the size of the file.
   */
  public static class HostAndWeight {

    private final String host;
    private long weight;
    private long weightForSsd;

    /**
     * Constructor
     * @param host the host name
     * @param weight the weight
     * @param weightForSsd the weight for ssd
     */
    public HostAndWeight(String host, long weight, long weightForSsd) {
      this.host = host;
      this.weight = weight;
      this.weightForSsd = weightForSsd;
    }

    /**
     * add weight
     * @param weight the weight
     * @param weightForSsd the weight for ssd
     */
    public void addWeight(long weight, long weightForSsd) {
      this.weight += weight;
      this.weightForSsd += weightForSsd;
    }

    /**
     * @return the host name
     */
    public String getHost() {
      return host;
    }

    /**
     * @return the weight
     */
    public long getWeight() {
      return weight;
    }

    /**
     * @return the weight for ssd
     */
    public long getWeightForSsd() {
      return weightForSsd;
    }

    /**
     * comparator used to sort hosts based on weight
     */
    public static class WeightComparator implements Comparator<HostAndWeight> {
      @Override
      public int compare(HostAndWeight l, HostAndWeight r) {
        if(l.getWeight() == r.getWeight()) {
          return l.getHost().compareTo(r.getHost());
        }
        return l.getWeight() < r.getWeight() ? -1 : 1;
      }
    }
  }

  /**
   * Constructor
   */
  public HDFSBlocksDistribution() {
    this.hostAndWeights = new TreeMap<>();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    return "number of unique hosts in the distribution=" +
      this.hostAndWeights.size();
  }

  /**
   * add some weight to a list of hosts, update the value of unique block weight
   * @param hosts the list of the host
   * @param weight the weight
   */
  public void addHostsAndBlockWeight(String[] hosts, long weight) {
    addHostsAndBlockWeight(hosts, weight, null);
  }

  /**
   * add some weight to a list of hosts, update the value of unique block weight
   * @param hosts the list of the host
   * @param weight the weight
   */
  public void addHostsAndBlockWeight(String[] hosts, long weight, StorageType[] storageTypes) {
    if (hosts == null || hosts.length == 0) {
      // erroneous data
      return;
    }

    addUniqueWeight(weight);
    if (storageTypes != null && storageTypes.length == hosts.length) {
      for (int i = 0; i < hosts.length; i++) {
        long weightForSsd = 0;
        if (storageTypes[i] == StorageType.SSD) {
          weightForSsd = weight;
        }
        addHostAndBlockWeight(hosts[i], weight, weightForSsd);
      }
    } else {
      for (String hostname : hosts) {
        addHostAndBlockWeight(hostname, weight, 0);
      }
    }
  }

  /**
   * add some weight to the total unique weight
   * @param weight the weight
   */
  private void addUniqueWeight(long weight) {
    uniqueBlocksTotalWeight += weight;
  }

  /**
   * add some weight to a specific host
   * @param host the host name
   * @param weight the weight
   * @param weightForSsd the weight for ssd
   */
  private void addHostAndBlockWeight(String host, long weight, long weightForSsd) {
    if (host == null) {
      // erroneous data
      return;
    }

    HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
    if(hostAndWeight == null) {
      hostAndWeight = new HostAndWeight(host, weight, weightForSsd);
      this.hostAndWeights.put(host, hostAndWeight);
    } else {
      hostAndWeight.addWeight(weight, weightForSsd);
    }
  }

  /**
   * @return the hosts and their weights
   */
  public Map<String,HostAndWeight> getHostAndWeights() {
    return this.hostAndWeights;
  }

  /**
   * return the weight for a specific host, that will be the total bytes of all
   * blocks on the host
   * @param host the host name
   * @return the weight of the given host
   */
  public long getWeight(String host) {
    long weight = 0;
    if (host != null) {
      HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
      if(hostAndWeight != null) {
        weight = hostAndWeight.getWeight();
      }
    }
    return weight;
  }

  /**
   * @return the sum of all unique blocks' weight
   */
  public long getUniqueBlocksTotalWeight() {
    return uniqueBlocksTotalWeight;
  }

  /**
   * Implementations 'visit' hostAndWeight.
   */
  public interface Visitor {
    long visit(final HostAndWeight hostAndWeight);
  }

  /**
   * @param host the host name
   * @return the locality index of the given host
   */
  public float getBlockLocalityIndex(String host) {
    if (uniqueBlocksTotalWeight == 0) {
      return 0.0f;
    } else {
      return (float) getBlocksLocalityWeightInternal(host, HostAndWeight::getWeight)
        / (float) uniqueBlocksTotalWeight;
    }
  }

  /**
   * @param host the host name
   * @return the locality index with ssd of the given host
   */
  public float getBlockLocalityIndexForSsd(String host) {
    if (uniqueBlocksTotalWeight == 0) {
      return 0.0f;
    } else {
      return (float) getBlocksLocalityWeightInternal(host, HostAndWeight::getWeightForSsd)
        / (float) uniqueBlocksTotalWeight;
    }
  }

  /**
   * @param host the host name
   * @return the blocks local weight of the given host
   */
  public long getBlocksLocalWeight(String host) {
    return getBlocksLocalityWeightInternal(host, HostAndWeight::getWeight);
  }

  /**
   * @param host the host name
   * @return the blocks local with ssd weight of the given host
   */
  public long getBlocksLocalWithSsdWeight(String host) {
    return getBlocksLocalityWeightInternal(host, HostAndWeight::getWeightForSsd);
  }

  /**
   * @param host the host name
   * @return the locality index of the given host
   */
  private long getBlocksLocalityWeightInternal(String host, Visitor visitor) {
    long localityIndex = 0;
    HostAndWeight hostAndWeight = this.hostAndWeights.get(host);
    // Compatible with local mode, see HBASE-24569
    if (hostAndWeight == null) {
      String currentHost = "";
      try {
        currentHost = DNS.getDefaultHost("default", "default");
      } catch (Exception e) {
        // Just ignore, it's ok, avoid too many log info
      }
      if (host.equals(currentHost)) {
        hostAndWeight = this.hostAndWeights.get(HConstants.LOCALHOST);
      }
    }
    if (hostAndWeight != null && uniqueBlocksTotalWeight != 0) {
      localityIndex = visitor.visit(hostAndWeight);
    }
    return localityIndex;
  }

  /**
   * This will add the distribution from input to this object
   * @param otherBlocksDistribution the other hdfs blocks distribution
   */
  public void add(HDFSBlocksDistribution otherBlocksDistribution) {
    Map<String,HostAndWeight> otherHostAndWeights =
      otherBlocksDistribution.getHostAndWeights();
    for (Map.Entry<String, HostAndWeight> otherHostAndWeight:
      otherHostAndWeights.entrySet()) {
      addHostAndBlockWeight(otherHostAndWeight.getValue().host,
        otherHostAndWeight.getValue().weight, otherHostAndWeight.getValue().weightForSsd);
    }
    addUniqueWeight(otherBlocksDistribution.getUniqueBlocksTotalWeight());
  }

  /**
   * return the sorted list of hosts in terms of their weights
   */
  public List<String> getTopHosts() {
    HostAndWeight[] hostAndWeights = getTopHostsWithWeights();
    List<String> topHosts = new ArrayList<>(hostAndWeights.length);
    for(HostAndWeight haw : hostAndWeights) {
      topHosts.add(haw.getHost());
    }
    return topHosts;
  }

  /**
   * return the sorted list of hosts in terms of their weights
   */
  public HostAndWeight[] getTopHostsWithWeights() {
    NavigableSet<HostAndWeight> orderedHosts = new TreeSet<>(new HostAndWeight.WeightComparator());
    orderedHosts.addAll(this.hostAndWeights.values());
    return orderedHosts.descendingSet().toArray(new HostAndWeight[orderedHosts.size()]);
  }

}

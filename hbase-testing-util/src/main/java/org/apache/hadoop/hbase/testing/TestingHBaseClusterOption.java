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
package org.apache.hadoop.hbase.testing;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Options for starting up a mini testing cluster {@link TestingHBaseCluster} (including an hbase,
 * dfs and zookeeper clusters) in test. The options include HDFS options to build mini dfs cluster,
 * Zookeeper options to build mini zk cluster, and mostly HBase options to build mini hbase cluster.
 * To create an object, use a {@link Builder}. Example usage:
 *
 * <pre>
 *    TestingHBaseClusterOption option = TestingHBaseClusterOption.builder().
 *        .numMasters(3).createWALDir(true).build();
 * </pre>
 *
 * Default values can be found in {@link Builder}.
 */
@InterfaceAudience.Public
public final class TestingHBaseClusterOption {

  /**
   * Configuration for this testing cluster. Can be {@code null}.
   */
  private final Configuration conf;

  /**
   * Number of masters to start up. We'll start this many hbase masters.
   */
  private final int numMasters;

  /**
   * Number of masters that always remain standby. These set of masters never transition to active
   * even if an active master does not exist.
   */
  private final int numAlwaysStandByMasters;

  /**
   * Number of region servers to start up. If this value is > 1, then make sure config
   * "hbase.regionserver.info.port" is -1 (i.e. no ui per regionserver) otherwise bind errors.
   */
  private final int numRegionServers;

  /**
   * Ports that RegionServer should use. Pass ports if you want to test cluster restart where for
   * sure the regionservers come up on same address+port (but just with different startcode); by
   * default mini hbase clusters choose new arbitrary ports on each cluster start.
   */
  private final List<Integer> rsPorts;

  /**
   * Number of datanodes. Used to create mini DSF cluster. Surpassed by {@link #dataNodeHosts} size.
   */
  private final int numDataNodes;

  /**
   * The hostnames of DataNodes to run on. This is useful if you want to run datanode on distinct
   * hosts for things like HDFS block location verification. If you start MiniDFSCluster without
   * host names, all instances of the datanodes will have the same host name.
   */
  private final String[] dataNodeHosts;

  /**
   * Number of Zookeeper servers.
   */
  private final int numZkServers;

  /**
   * Whether to create a new root or data directory path. If true, the newly created data directory
   * will be configured as HBase rootdir. This will overwrite existing root directory config.
   */
  private final boolean createRootDir;

  /**
   * Whether to create a new WAL directory. If true, the newly created directory will be configured
   * as HBase wal.dir which is separate from HBase rootdir.
   */
  private final boolean createWALDir;

  private final String externalDfsUri;

  private final String externalZkConnectString;

  /**
   * Private constructor. Use {@link Builder#build()}.
   */
  private TestingHBaseClusterOption(Configuration conf, int numMasters, int numAlwaysStandByMasters,
    int numRegionServers, List<Integer> rsPorts, int numDataNodes, String[] dataNodeHosts,
    int numZkServers, boolean createRootDir, boolean createWALDir, String externalDfsUri,
    String externalZkConnectString) {
    this.conf = conf;
    this.numMasters = numMasters;
    this.numAlwaysStandByMasters = numAlwaysStandByMasters;
    this.numRegionServers = numRegionServers;
    this.rsPorts = rsPorts;
    this.numDataNodes = numDataNodes;
    this.dataNodeHosts = dataNodeHosts;
    this.numZkServers = numZkServers;
    this.createRootDir = createRootDir;
    this.createWALDir = createWALDir;
    this.externalDfsUri = externalDfsUri;
    this.externalZkConnectString = externalZkConnectString;
  }

  public Configuration conf() {
    return conf;
  }

  public int getNumMasters() {
    return numMasters;
  }

  public int getNumAlwaysStandByMasters() {
    return numAlwaysStandByMasters;
  }

  public int getNumRegionServers() {
    return numRegionServers;
  }

  public List<Integer> getRsPorts() {
    return rsPorts;
  }

  public int getNumDataNodes() {
    return numDataNodes;
  }

  public String[] getDataNodeHosts() {
    return dataNodeHosts;
  }

  public int getNumZkServers() {
    return numZkServers;
  }

  public boolean isCreateRootDir() {
    return createRootDir;
  }

  public boolean isCreateWALDir() {
    return createWALDir;
  }

  public String getExternalDfsUri() {
    return externalDfsUri;
  }

  public String getExternalZkConnectString() {
    return externalZkConnectString;
  }

  @Override
  public String toString() {
    return "StartMiniClusterOption{" + "numMasters=" + numMasters + ", numRegionServers="
      + numRegionServers + ", rsPorts=" + StringUtils.join(rsPorts) + ", numDataNodes="
      + numDataNodes + ", dataNodeHosts=" + Arrays.toString(dataNodeHosts) + ", numZkServers="
      + numZkServers + ", createRootDir=" + createRootDir + ", createWALDir=" + createWALDir + '}';
  }

  /**
   * Convert to the internal option. Not for public use so package private.
   */
  StartMiniClusterOption convert() {
    return StartMiniClusterOption.builder().numMasters(numMasters)
      .numAlwaysStandByMasters(numAlwaysStandByMasters).numRegionServers(numRegionServers)
      .rsPorts(rsPorts).numDataNodes(numDataNodes).dataNodeHosts(dataNodeHosts)
      .numZkServers(numZkServers).createRootDir(createRootDir).createWALDir(createWALDir).build();
  }

  /**
   * Returns a new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder pattern for creating an {@link TestingHBaseClusterOption}. The default values of its
   * fields should be considered public and constant. Changing the default values may cause other
   * tests fail.
   */
  public static final class Builder {
    private Configuration conf;
    private int numMasters = 1;
    private int numAlwaysStandByMasters = 0;
    private int numRegionServers = 1;
    private List<Integer> rsPorts = null;
    private int numDataNodes = 1;
    private String[] dataNodeHosts = null;
    private int numZkServers = 1;
    private boolean createRootDir = false;
    private boolean createWALDir = false;
    private String externalDfsUri = null;
    private String externalZkConnectString = null;

    private Builder() {
    }

    public TestingHBaseClusterOption build() {
      if (dataNodeHosts != null && dataNodeHosts.length != 0) {
        numDataNodes = dataNodeHosts.length;
      }
      return new TestingHBaseClusterOption(conf, numMasters, numAlwaysStandByMasters,
        numRegionServers, rsPorts, numDataNodes, dataNodeHosts, numZkServers, createRootDir,
        createWALDir, externalDfsUri, externalZkConnectString);
    }

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder numMasters(int numMasters) {
      this.numMasters = numMasters;
      return this;
    }

    public Builder numAlwaysStandByMasters(int numAlwaysStandByMasters) {
      this.numAlwaysStandByMasters = numAlwaysStandByMasters;
      return this;
    }

    public Builder numRegionServers(int numRegionServers) {
      this.numRegionServers = numRegionServers;
      return this;
    }

    public Builder rsPorts(List<Integer> rsPorts) {
      this.rsPorts = rsPorts;
      return this;
    }

    public Builder numDataNodes(int numDataNodes) {
      this.numDataNodes = numDataNodes;
      return this;
    }

    public Builder dataNodeHosts(String[] dataNodeHosts) {
      this.dataNodeHosts = dataNodeHosts;
      return this;
    }

    public Builder numZkServers(int numZkServers) {
      this.numZkServers = numZkServers;
      return this;
    }

    public Builder createRootDir(boolean createRootDir) {
      this.createRootDir = createRootDir;
      return this;
    }

    public Builder createWALDir(boolean createWALDir) {
      this.createWALDir = createWALDir;
      return this;
    }

    public Builder useExternalDfs(String uri) {
      this.externalDfsUri = uri;
      return this;
    }

    public Builder useExternalZooKeeper(String connectString) {
      this.externalZkConnectString = connectString;
      return this;
    }
  }
}

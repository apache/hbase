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

package org.apache.hadoop.hbase.test;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;


/**
 * This is an integration test for replication. It is derived off
 * {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList} that creates a large circular
 * linked list in one cluster and verifies that the data is correct in a sink cluster. The test
 * handles creating the tables and schema and setting up the replication.
 */
public class IntegrationTestReplication extends IntegrationTestBigLinkedList {
  protected String sourceClusterIdString;
  protected String sinkClusterIdString;
  protected int numIterations;
  protected int numMappers;
  protected long numNodes;
  protected String outputDir;
  protected int numReducers;
  protected int generateVerifyGap;
  protected Integer width;
  protected Integer wrapMultiplier;
  protected boolean noReplicationSetup = false;

  private final String SOURCE_CLUSTER_OPT = "sourceCluster";
  private final String DEST_CLUSTER_OPT = "destCluster";
  private final String ITERATIONS_OPT = "iterations";
  private final String NUM_MAPPERS_OPT = "numMappers";
  private final String OUTPUT_DIR_OPT = "outputDir";
  private final String NUM_REDUCERS_OPT = "numReducers";
  private final String NO_REPLICATION_SETUP_OPT = "noReplicationSetup";

  /**
   * The gap (in seconds) from when data is finished being generated at the source
   * to when it can be verified. This is the replication lag we are willing to tolerate
   */
  private final String GENERATE_VERIFY_GAP_OPT = "generateVerifyGap";

  /**
   * The width of the linked list.
   * See {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList} for more details
   */
  private final String WIDTH_OPT = "width";

  /**
   * The number of rows after which the linked list points to the first row.
   * See {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList} for more details
   */
  private final String WRAP_MULTIPLIER_OPT = "wrapMultiplier";

  /**
   * The number of nodes in the test setup. This has to be a multiple of WRAP_MULTIPLIER * WIDTH
   * in order to ensure that the linked list can is complete.
   * See {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList} for more details
   */
  private final String NUM_NODES_OPT = "numNodes";

  private final int DEFAULT_NUM_MAPPERS = 1;
  private final int DEFAULT_NUM_REDUCERS = 1;
  private final int DEFAULT_NUM_ITERATIONS = 1;
  private final int DEFAULT_GENERATE_VERIFY_GAP = 60;
  private final int DEFAULT_WIDTH = 1000000;
  private final int DEFAULT_WRAP_MULTIPLIER = 25;
  private final int DEFAULT_NUM_NODES = DEFAULT_WIDTH * DEFAULT_WRAP_MULTIPLIER;

  /**
   * Wrapper around an HBase ClusterID allowing us
   * to get admin connections and configurations for it
   */
  protected class ClusterID {
    private final Configuration configuration;
    private Connection connection = null;

    /**
     * This creates a new ClusterID wrapper that will automatically build connections and
     * configurations to be able to talk to the specified cluster
     *
     * @param base the base configuration that this class will add to
     * @param key the cluster key in the form of zk_quorum:zk_port:zk_parent_node
     */
    public ClusterID(Configuration base,
                     String key) {
      configuration = new Configuration(base);
      String[] parts = key.split(":");
      configuration.set(HConstants.ZOOKEEPER_QUORUM, parts[0]);
      configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, parts[1]);
      configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts[2]);
    }

    @Override
    public String toString() {
      return Joiner.on(":").join(configuration.get(HConstants.ZOOKEEPER_QUORUM),
                                 configuration.get(HConstants.ZOOKEEPER_CLIENT_PORT),
                                 configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    }

    public Configuration getConfiguration() {
      return this.configuration;
    }

    public Connection getConnection() throws Exception {
      if (this.connection == null) {
        this.connection = ConnectionFactory.createConnection(this.configuration);
      }
      return this.connection;
    }

    public void closeConnection() throws Exception {
      this.connection.close();
      this.connection = null;
    }

    public boolean equals(ClusterID other) {
      return this.toString().equalsIgnoreCase(other.toString());
    }
  }

  /**
   * The main runner loop for the test. It uses
   * {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList}
   * for the generation and verification of the linked list. It is heavily based on
   * {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList.Loop}
   */
  protected class VerifyReplicationLoop extends Configured implements Tool {
    private final Log LOG = LogFactory.getLog(VerifyReplicationLoop.class);
    protected ClusterID source;
    protected ClusterID sink;

    IntegrationTestBigLinkedList integrationTestBigLinkedList;

    /**
     * This tears down any tables that existed from before and rebuilds the tables and schemas on
     * the source cluster. It then sets up replication from the source to the sink cluster by using
     * the {@link org.apache.hadoop.hbase.client.replication.ReplicationAdmin}
     * connection.
     *
     * @throws Exception
     */
    protected void setupTablesAndReplication() throws Exception {
      TableName tableName = getTableName(source.getConfiguration());

      ClusterID[] clusters = {source, sink};

      // delete any old tables in the source and sink
      for (ClusterID cluster : clusters) {
        Admin admin = cluster.getConnection().getAdmin();

        if (admin.tableExists(tableName)) {
          if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
          }

          /**
           * TODO: This is a work around on a replication bug (HBASE-13416)
           * When we recreate a table against that has recently been
           * deleted, the contents of the logs are replayed even though
           * they should not. This ensures that we flush the logs
           * before the table gets deleted. Eventually the bug should be
           * fixed and this should be removed.
           */
          Set<ServerName> regionServers = new TreeSet<>();
          for (HRegionLocation rl :
               cluster.getConnection().getRegionLocator(tableName).getAllRegionLocations()) {
            regionServers.add(rl.getServerName());
          }

          for (ServerName server : regionServers) {
            source.getConnection().getAdmin().rollWALWriter(server);
          }

          admin.deleteTable(tableName);
        }
      }

      // create the schema
      Generator generator = new Generator();
      generator.setConf(source.getConfiguration());
      generator.createSchema();

      // setup the replication on the source
      if (!source.equals(sink)) {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(source.getConfiguration());
        // remove any old replication peers
        for (String oldPeer : replicationAdmin.listPeerConfigs().keySet()) {
          replicationAdmin.removePeer(oldPeer);
        }

        // set the sink to be the target
        ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
        peerConfig.setClusterKey(sink.toString());

        // set the test table to be the table to replicate
        HashMap<TableName, ArrayList<String>> toReplicate = new HashMap<>();
        toReplicate.put(tableName, new ArrayList<String>(0));

        replicationAdmin.addPeer("TestPeer", peerConfig, toReplicate);

        replicationAdmin.enableTableRep(tableName);
        replicationAdmin.close();
      }

      for (ClusterID cluster : clusters) {
        cluster.closeConnection();
      }
    }

    protected void waitForReplication() throws Exception {
      // TODO: we shouldn't be sleeping here. It would be better to query the region servers
      // and wait for them to report 0 replication lag.
      Thread.sleep(generateVerifyGap * 1000);
    }

    /**
     * Run the {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList.Generator} in the
     * source cluster. This assumes that the tables have been setup via setupTablesAndReplication.
     *
     * @throws Exception
     */
    protected void runGenerator() throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path generatorOutput = new Path(outputPath, uuid.toString());

      Generator generator = new Generator();
      generator.setConf(source.getConfiguration());

      int retCode = generator.run(numMappers, numNodes, generatorOutput, width, wrapMultiplier);
      if (retCode > 0) {
        throw new RuntimeException("Generator failed with return code: " + retCode);
      }
    }


    /**
     * Run the {@link org.apache.hadoop.hbase.test.IntegrationTestBigLinkedList.Verify}
     * in the sink cluster. If replication is working properly the data written at the source
     * cluster should be available in the sink cluster after a reasonable gap
     *
     * @param expectedNumNodes the number of nodes we are expecting to see in the sink cluster
     * @throws Exception
     */
    protected void runVerify(long expectedNumNodes) throws Exception {
      Path outputPath = new Path(outputDir);
      UUID uuid = UUID.randomUUID(); //create a random UUID.
      Path iterationOutput = new Path(outputPath, uuid.toString());

      Verify verify = new Verify();
      verify.setConf(sink.getConfiguration());

      int retCode = verify.run(iterationOutput, numReducers);
      if (retCode > 0) {
        throw new RuntimeException("Verify.run failed with return code: " + retCode);
      }

      if (!verify.verify(expectedNumNodes)) {
        throw new RuntimeException("Verify.verify failed");
      }

      LOG.info("Verify finished with success. Total nodes=" + expectedNumNodes);
    }

    /**
     * The main test runner
     *
     * This test has 4 steps:
     *  1: setupTablesAndReplication
     *  2: generate the data into the source cluster
     *  3: wait for replication to propagate
     *  4: verify that the data is available in the sink cluster
     *
     * @param args should be empty
     * @return 0 on success
     * @throws Exception on an error
     */
    @Override
    public int run(String[] args) throws Exception {
      source = new ClusterID(getConf(), sourceClusterIdString);
      sink = new ClusterID(getConf(), sinkClusterIdString);

      if (!noReplicationSetup) {
        setupTablesAndReplication();
      }
      int expectedNumNodes = 0;
      for (int i = 0; i < numIterations; i++) {
        LOG.info("Starting iteration = " + i);

        expectedNumNodes += numMappers * numNodes;

        runGenerator();
        waitForReplication();
        runVerify(expectedNumNodes);
      }

      /**
       * we are always returning 0 because exceptions are thrown when there is an error
       * in the verification step.
       */
      return 0;
    }
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    addRequiredOptWithArg("s", SOURCE_CLUSTER_OPT,
                          "Cluster ID of the source cluster (e.g. localhost:2181:/hbase)");
    addRequiredOptWithArg("r", DEST_CLUSTER_OPT,
                          "Cluster ID of the sink cluster (e.g. localhost:2182:/hbase)");
    addRequiredOptWithArg("d", OUTPUT_DIR_OPT,
                          "Temporary directory where to write keys for the test");

    addOptWithArg("nm", NUM_MAPPERS_OPT,
                  "Number of mappers (default: " + DEFAULT_NUM_MAPPERS + ")");
    addOptWithArg("nr", NUM_REDUCERS_OPT,
                  "Number of reducers (default: " + DEFAULT_NUM_MAPPERS + ")");
    addOptNoArg("nrs", NO_REPLICATION_SETUP_OPT,
                  "Don't setup tables or configure replication before starting test");
    addOptWithArg("n", NUM_NODES_OPT,
                  "Number of nodes. This should be a multiple of width * wrapMultiplier."  +
                  " (default: " + DEFAULT_NUM_NODES + ")");
    addOptWithArg("i", ITERATIONS_OPT, "Number of iterations to run (default: " +
                  DEFAULT_NUM_ITERATIONS +  ")");
    addOptWithArg("t", GENERATE_VERIFY_GAP_OPT,
                  "Gap between generate and verify steps in seconds (default: " +
                  DEFAULT_GENERATE_VERIFY_GAP + ")");
    addOptWithArg("w", WIDTH_OPT,
                  "Width of the linked list chain (default: " + DEFAULT_WIDTH + ")");
    addOptWithArg("wm", WRAP_MULTIPLIER_OPT, "How many times to wrap around (default: " +
                  DEFAULT_WRAP_MULTIPLIER + ")");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    processBaseOptions(cmd);

    sourceClusterIdString = cmd.getOptionValue(SOURCE_CLUSTER_OPT);
    sinkClusterIdString = cmd.getOptionValue(DEST_CLUSTER_OPT);
    outputDir = cmd.getOptionValue(OUTPUT_DIR_OPT);

    /** This uses parseInt from {@link org.apache.hadoop.hbase.util.AbstractHBaseTool} */
    numMappers = parseInt(cmd.getOptionValue(NUM_MAPPERS_OPT,
                                             Integer.toString(DEFAULT_NUM_MAPPERS)),
                          1, Integer.MAX_VALUE);
    numReducers = parseInt(cmd.getOptionValue(NUM_REDUCERS_OPT,
                                              Integer.toString(DEFAULT_NUM_REDUCERS)),
                           1, Integer.MAX_VALUE);
    numNodes = parseInt(cmd.getOptionValue(NUM_NODES_OPT, Integer.toString(DEFAULT_NUM_NODES)),
                        1, Integer.MAX_VALUE);
    generateVerifyGap = parseInt(cmd.getOptionValue(GENERATE_VERIFY_GAP_OPT,
                                                    Integer.toString(DEFAULT_GENERATE_VERIFY_GAP)),
                                 1, Integer.MAX_VALUE);
    numIterations = parseInt(cmd.getOptionValue(ITERATIONS_OPT,
                                                Integer.toString(DEFAULT_NUM_ITERATIONS)),
                             1, Integer.MAX_VALUE);
    width = parseInt(cmd.getOptionValue(WIDTH_OPT, Integer.toString(DEFAULT_WIDTH)),
                                        1, Integer.MAX_VALUE);
    wrapMultiplier = parseInt(cmd.getOptionValue(WRAP_MULTIPLIER_OPT,
                                                 Integer.toString(DEFAULT_WRAP_MULTIPLIER)),
                              1, Integer.MAX_VALUE);

    if (cmd.hasOption(NO_REPLICATION_SETUP_OPT)) {
      noReplicationSetup = true;
    }

    if (numNodes % (width * wrapMultiplier) != 0) {
      throw new RuntimeException("numNodes must be a multiple of width and wrap multiplier");
    }
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    VerifyReplicationLoop tool = new  VerifyReplicationLoop();
    tool.integrationTestBigLinkedList = this;
    return ToolRunner.run(getConf(), tool, null);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestReplication(), args);
    System.exit(ret);
  }
}

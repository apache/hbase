package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.IPv4AddressTruncationMapping;

public class RegionPlacement {
  private static final Log LOG = LogFactory.getLog(RegionPlacement.class
      .getName());

  // The cost of a placement that should never be assigned.
  private static final float MAX_COST = Float.POSITIVE_INFINITY;

  // The cost of a placement that is undesirable but acceptable.
  private static final float AVOID_COST = 100000f;

  // The amount by which the cost of a placement is increased if it is the
  // last slot of the server. This is done to more evenly distribute the slop
  // amongst servers.
  private static final float LAST_SLOT_COST_PENALTY = 0.5f;

  // The amount by which the cost of a primary placement is penalized if it is
  // not the host currently serving the region. This is done to minimize moves.
  private static final float NOT_CURRENT_HOST_PENALTY = 0.1f;

  private Configuration conf;
  private DNSToSwitchMapping switchMapping;
  private Map<HServerInfo, String> rackCache;
  private final boolean enforceRackPolicy;

  public RegionPlacement(Configuration conf, boolean enforceRackPolicy) {
    this.conf = conf;
    this.switchMapping = new IPv4AddressTruncationMapping();
    this.rackCache = new HashMap<HServerInfo, String>();
    this.enforceRackPolicy = enforceRackPolicy;
  }

  /**
   * Get the name of the rack containing a server, according to the DNS to
   * switch mapping.
   * @param info the server for which to get the rack name
   * @return the rack name of the server
   */
  private String getRack(HServerInfo info) {
    String cached = rackCache.get(info);
    if (cached != null) {
      return cached;
    }
    List<String> racks = switchMapping.resolve(Arrays.asList(
        new String[]{info.getServerAddress().getInetSocketAddress()
            .getAddress().getHostAddress()}));
    if (racks != null && racks.size() > 0) {
      rackCache.put(info, racks.get(0));
      return racks.get(0);
    }
    rackCache.put(info, "");
    return "";
  }

  public Map<HRegionInfo, List<HServerInfo>> placeRegions()
      throws MasterNotRunningException, IOException, InterruptedException {
    // Get all regions in the cluster.
    Map<HRegionInfo, HServerAddress> regionMap = getMetaEntries();
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(regionMap.keySet());
    int numRegions = regions.size();

    // Get all servers in the cluster.
    List<HServerInfo> servers = new ArrayList<HServerInfo>();
    servers.addAll(new HBaseAdmin(conf).getClusterStatus().getServerInfo());

    // Each server may serve multiple regions. Assume that each server has equal
    // capacity in terms of the number of regions that may be served.
    int slotsPerServer = (int)Math.ceil((float) numRegions / servers.size());
    int regionSlots = slotsPerServer * servers.size();

    // Get the locality for each region to each server.
    Map<String, Map<String, Float>> localityMap =
        FSUtils.getRegionDegreeLocalityMappingFromFS(conf);

    // Transform the locality mapping into a 2D array, assuming that any
    // unspecified locality value is 0.
    float[][] localityPerServer = new float[numRegions][regionSlots];
    for (int i = 0; i < numRegions; i++) {
      Map<String, Float> serverLocalityMap =
          localityMap.get(regions.get(i).getEncodedName());
      if (serverLocalityMap == null) {
        continue;
      }
      for (int j = 0; j < servers.size(); j++) {
        String serverName = servers.get(j).getHostname();
        if (serverName == null) {
          continue;
        }
        Float locality = serverLocalityMap.get(serverName);
        if (locality == null) {
          continue;
        }
        for (int k = 0; k < slotsPerServer; k++) {
          // If we can't find the locality of a region to a server, which occurs
          // because locality is only reported for servers which have some
          // blocks of a region local, then the locality for that pair is 0.
          localityPerServer[i][j * slotsPerServer + k] = locality.floatValue();
        }
      }
    }

    // Compute the total rack locality for each region in each rack. The total
    // rack locality is the sum of the localities of a region on all servers in
    // a rack.
    Map<String, Map<HRegionInfo, Float>> rackRegionLocality =
        new HashMap<String, Map<HRegionInfo, Float>>();
    for (int i = 0; i < numRegions; i++) {
      HRegionInfo region = regions.get(i);
      for (int j = 0; j < regionSlots; j += slotsPerServer) {
        String rack = getRack(servers.get(j / slotsPerServer));
        Map<HRegionInfo, Float> rackLocality = rackRegionLocality.get(rack);
        if (rackLocality == null) {
          rackLocality = new HashMap<HRegionInfo, Float>();
          rackRegionLocality.put(rack, rackLocality);
        }
        Float localityObj = rackLocality.get(region);
        float locality = localityObj == null ? 0 : localityObj.floatValue();
        locality += localityPerServer[i][j];
        rackLocality.put(region, locality);
      }
    }

    // Compute the primary, secondary and tertiary costs for each region/server
    // pair. These costs are based only on node locality and rack locality, and
    // will be modified later.
    float[][] primaryCost = new float[numRegions][regionSlots];
    float[][] secondaryCost = new float[numRegions][regionSlots];
    float[][] tertiaryCost = new float[numRegions][regionSlots];
    for (int i = 0; i < numRegions; i++) {
      for (int j = 0; j < regionSlots; j++) {
        String rack = getRack(servers.get(j / slotsPerServer));
        Float totalRackLocalityObj =
            rackRegionLocality.get(rack).get(regions.get(i));
        float totalRackLocality = totalRackLocalityObj == null ?
            0 : totalRackLocalityObj.floatValue();

        // Primary cost aims to favor servers with high node locality and low
        // rack locality, so that secondaries and tertiaries can be chosen for
        // nodes with high rack locality. This might give primaries with
        // slightly less locality at first compared to a cost which only
        // considers the node locality, but should be better in the long run.
        primaryCost[i][j] = 1 - (2 * localityPerServer[i][j] -
            totalRackLocality);

        // Secondary cost aims to favor servers with high node locality and high
        // rack locality since the tertiary will be chosen from the same rack as
        // the secondary. This could be negative, but that is okay.
        secondaryCost[i][j] = 2 - (localityPerServer[i][j] + totalRackLocality);

        // Tertiary cost is only concerned with the node locality. It will later
        // be restricted to only hosts on the same rack as the secondary.
        tertiaryCost[i][j] = 1 - localityPerServer[i][j];
      }
    }

    // We want to minimize the number of regions which move as the result of a
    // new assignment. Therefore, slightly penalize any placement which is for
    // a host that is not currently serving the region.
    for (int i = 0; i < numRegions; i++) {
      for (int j = 0; j < servers.size(); j++) {
        if (!regionMap.get(regions.get(i)).equals(
            servers.get(j).getServerAddress())) {
          for (int k = 0; k < slotsPerServer; k++) {
            primaryCost[i][j * slotsPerServer + k] += NOT_CURRENT_HOST_PENALTY;
          }
        }
      }
    }

    // Artificially increase cost of last slot of each server to evenly
    // distribute the slop, otherwise there will be a few servers with too few
    // regions and many servers with the max number of regions.
    for (int i = 0; i < numRegions; i++) {
      for (int j = 0; j < regionSlots; j += slotsPerServer) {
        primaryCost[i][j] += LAST_SLOT_COST_PENALTY;
        secondaryCost[i][j] += LAST_SLOT_COST_PENALTY;
        tertiaryCost[i][j] += LAST_SLOT_COST_PENALTY;
      }
    }

    RandomizedMatrix randomizedMatrix = new RandomizedMatrix(numRegions,
        regionSlots);
    primaryCost = randomizedMatrix.transform(primaryCost);
    int[] primaryAssignment = new MunkresAssignment(primaryCost).solve();
    primaryAssignment = randomizedMatrix.invertIndices(primaryAssignment);

    // Modify the secondary and tertiary costs for each region/server pair to
    // prevent a region from being assigned to the same rack for both primary
    // and either one of secondary or tertiary.
    for (int i = 0; i < numRegions; i++) {
      int slot = primaryAssignment[i];
      String rack = getRack(servers.get(slot / slotsPerServer));
      for (int k = 0; k < servers.size(); k++) {
        if (!getRack(servers.get(k)).equals(rack)) {
          continue;
        }
        if (k == slot / slotsPerServer) {
          // Same node, do not place secondary or tertiary here ever.
          for (int m = 0; m < slotsPerServer; m++) {
            secondaryCost[i][k * slotsPerServer + m] = MAX_COST;
            tertiaryCost[i][k * slotsPerServer + m] = MAX_COST;
          }
        } else {
          // Same rack, do not place secondary or tertiary here if possible.
          for (int m = 0; m < slotsPerServer; m++) {
            secondaryCost[i][k * slotsPerServer + m] = AVOID_COST;
            tertiaryCost[i][k * slotsPerServer + m] = AVOID_COST;
          }
        }
      }
    }

    randomizedMatrix = new RandomizedMatrix(numRegions, regionSlots);
    secondaryCost = randomizedMatrix.transform(secondaryCost);
    int[] secondaryAssignment = new MunkresAssignment(secondaryCost).solve();
    secondaryAssignment = randomizedMatrix.invertIndices(secondaryAssignment);

    // Modify the tertiary costs for each region/server pair to ensure that a
    // region is assigned to a tertiary server on the same rack as its secondary
    // server, but not the same server in that rack.
    for (int i = 0; i < numRegions; i++) {
      int slot = secondaryAssignment[i];
      String rack = getRack(servers.get(slot / slotsPerServer));
      for (int k = 0; k < servers.size(); k++) {
        if (k == slot / slotsPerServer) {
          // Same node, do not place tertiary here ever.
          for (int m = 0; m < slotsPerServer; m++) {
            tertiaryCost[i][k * slotsPerServer + m] = MAX_COST;
          }
        } else {
          if (getRack(servers.get(k)).equals(rack)) {
            continue;
          }
          // Different rack, do not place tertiary here if possible.
          for (int m = 0; m < slotsPerServer; m++) {
            tertiaryCost[i][k * slotsPerServer + m] = AVOID_COST;
          }
        }
      }
    }

    randomizedMatrix = new RandomizedMatrix(numRegions, regionSlots);
    tertiaryCost = randomizedMatrix.transform(tertiaryCost);
    int[] tertiaryAssignment = new MunkresAssignment(tertiaryCost).solve();
    tertiaryAssignment = randomizedMatrix.invertIndices(tertiaryAssignment);

    Map<HRegionInfo, List<HServerInfo>> assignments =
        new TreeMap<HRegionInfo, List<HServerInfo>>();
    for (int i = 0; i < numRegions; i++) {
      List<HServerInfo> assignment = new ArrayList<HServerInfo>(3);
      assignment.add(servers.get(primaryAssignment[i] / slotsPerServer));
      assignment.add(servers.get(secondaryAssignment[i] / slotsPerServer));
      assignment.add(servers.get(tertiaryAssignment[i] / slotsPerServer));

      float max = 0;
      for (int j = 0; j < regionSlots; j += slotsPerServer) {
        max = Math.max(max, localityPerServer[i][j]);
      }

      System.out.println(regions.get(i).getRegionNameAsString());
      System.out.println("\tPrimary:   "
          + servers.get(primaryAssignment[i] / slotsPerServer).getServerName()
          + " (" + localityPerServer[i][primaryAssignment[i]] + ") [" + max
          + "]");
      System.out.println("\tSecondary: "
          + servers.get(secondaryAssignment[i] / slotsPerServer).getServerName()
          + " (" + localityPerServer[i][secondaryAssignment[i]] + ")");
      System.out.println("\tTertiary:  "
          + servers.get(tertiaryAssignment[i] / slotsPerServer).getServerName()
          + " (" + localityPerServer[i][tertiaryAssignment[i]] + ")");

      // Validate that the assignments satisfy the rack constraints.
      if (enforceRackPolicy) {
        if (getRack(assignment.get(0)).equals(getRack(assignment.get(1))) ||
            getRack(assignment.get(0)).equals(getRack(assignment.get(2)))) {
          throw new RuntimeException("Primary and secondary for " +
              regions.get(i).getRegionNameAsString() + " on same rack");
        }
        if (!getRack(assignment.get(1)).equals(getRack(assignment.get(2)))) {
          throw new RuntimeException("Secondaries for " +
              regions.get(i).getRegionNameAsString() + " on different racks");
        }
      }

      assignments.put(regions.get(i), assignment);
    }
    return assignments;
  }

  /**
   * Check that the assignment map has the expected number of assignments for
   * each region, and that none of the assignments are duplicates.
   * @param map the assignments to verify
   */
  private void verifyAssignments(Map<HRegionInfo, List<HServerInfo>> map) {
    for (Map.Entry<HRegionInfo, List<HServerInfo>> entry : map.entrySet()) {
      List<HServerInfo> servers = entry.getValue();
      if (servers.size() != 3) {
        throw new IllegalStateException("Not enough assignments for region "
            + entry.getKey().getRegionNameAsString());
      }
      for (int i = 0; i < servers.size() - 1; i++) {
        HServerInfo first = servers.get(i);
        for (int j = i + 1; j < servers.size(); j++) {
          if (first.equals(servers.get(j))) {
            throw new IllegalStateException("Region " +
                entry.getKey().getRegionNameAsString() + " was assigned to " +
                first.getServerName() + " more than once");
          }
        }
      }
    }
  }

  /**
   * Persist the map of assignment hints into .META.
   * @param map the assignments to be put into .META.
   * @throws IOException if cannot put assignment hint in .META.
   */
  public void putFavoredNodes(Map<HRegionInfo, List<HServerInfo>> map)
      throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (Map.Entry<HRegionInfo, List<HServerInfo>> entry : map.entrySet()) {
      String favoredNodes = "";
      for (HServerInfo info : entry.getValue()) {
        favoredNodes += info.getHostnamePort() + ",";
      }
      favoredNodes = favoredNodes.substring(0, favoredNodes.length() - 1);

      Put put = new Put(entry.getKey().getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.FAVOREDNODES_QUALIFIER,
          System.currentTimeMillis(), favoredNodes.getBytes());
      puts.add(put);

      LOG.debug("Favored nodes region: " + put.toString() + " are " +
          favoredNodes);
    }

    // Write the region assignments to the meta table.
    HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
    metaTable.put(puts);
  }

  /**
   * Get a list of regions from .META., not including .META. itself, mapped to
   * the host currently serving that region. If there is no host serving that
   * region, an empty (not null) server address will be the value of the entry.
   * @return map of regions to servers from .META.
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> getMetaEntries() throws IOException {
    final Map<HRegionInfo, HServerAddress> regions =
        new TreeMap<HRegionInfo, HServerAddress>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          if (regionInfo != null) {
            if (server != null) {
              regions.put(Writables.getHRegionInfo(regionInfo),
                  new HServerAddress(new String(server)));
            } else {
              regions.put(Writables.getHRegionInfo(regionInfo),
                  new HServerAddress());
            }
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };

    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);

    return regions;
  }

  /**
   * Check whether regions are assigned to servers consistent with the explicit
   * hints that are persisted in the META table, if any, printing results to
   * standard out.
   * @throws IOException
   */
  private void verifyPlacement() throws IOException {
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      private String[] PLACEMENTS = {"[Primary]", "[Secondary]", "[Tertiary]"};
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              "favorednodes".getBytes());
          if (regionInfo != null) {
            HRegionInfo info = Writables.getHRegionInfo(regionInfo);
            if (server != null) {
              String serverString = new String(server);
              if (favoredNodes != null) {
                String[] splits = new String(favoredNodes).split(",");
                String placement = "not a favored node <<<<<<<<<<";
                for (int i = 0; i < splits.length; i++) {
                  if (splits[i].equals(serverString)) {
                    placement = PLACEMENTS[i];
                  }
                }
                System.out.println(info.getRegionNameAsString() + " on " +
                    serverString + " " + placement);
              } else {
                System.out.println(info.getRegionNameAsString() + " on " +
                    serverString + " no favored nodes");
              }
            } else {
              System.out.println(info.getRegionNameAsString() +
                  " not assigned to a server");
            }
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };

    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "RegionPlacement < -w | -n | -v | -t | -h > [-r]", opt);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    Options opt = new Options();
    opt.addOption("w", "write", false, "write assignments to META");
    opt.addOption("n", "dry-run", false, "do not write assignments to META");
    opt.addOption("v", "verify", false, "check current placement against META");
    opt.addOption("t", "test", false, "test RandomizedMatrix");
    opt.addOption("h", "help", false, "print usage");
    opt.addOption("r", "enforce-rack", false, "enforce 2-rack policy");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);
      boolean enforceRackPolicy = cmd.hasOption("r") ||
          cmd.hasOption("enforce-rack");
      if (cmd.hasOption("h") || cmd.hasOption("help")) {
        printHelp(opt);
      } else if (cmd.hasOption("t") || cmd.hasOption("test")) {
        RandomizedMatrix.test();
      } else if (cmd.hasOption("v") || cmd.hasOption("verify")) {
        Configuration conf = HBaseConfiguration.create();
        RegionPlacement rp = new RegionPlacement(conf, enforceRackPolicy);
        rp.verifyPlacement();
      } else if (cmd.hasOption("n") || cmd.hasOption("dry-run")) {
        Configuration conf = HBaseConfiguration.create();
        RegionPlacement rp = new RegionPlacement(conf, enforceRackPolicy);
        Map<HRegionInfo, List<HServerInfo>> assignments = rp.placeRegions();
        rp.verifyAssignments(assignments);
      } else if (cmd.hasOption("w") || cmd.hasOption("write")) {
        Configuration conf = HBaseConfiguration.create();
        RegionPlacement rp = new RegionPlacement(conf, enforceRackPolicy);
        Map<HRegionInfo, List<HServerInfo>> assignments = rp.placeRegions();
        rp.verifyAssignments(assignments);
        rp.putFavoredNodes(assignments);
      } else {
        printHelp(opt);
      }
    } catch (ParseException e) {
      printHelp(opt);
    }
  }

  /**
   * Some algorithms for solving the assignment problem may traverse workers or
   * jobs in linear order which may result in skewing the assignments of the
   * first jobs in the matrix toward the last workers in the matrix if the
   * costs are uniform. To avoid this kind of clumping, we can randomize the
   * rows and columns of the cost matrix in a reversible way, such that the
   * solution to the assignment problem can be interpreted in terms of the
   * original untransformed cost matrix. Rows and columns are transformed
   * independently such that the elements contained in any row of the input
   * matrix are the same as the elements in the corresponding output matrix,
   * and each row has its elements transformed in the same way. Similarly for
   * columns.
   */
  private static class RandomizedMatrix {
    private final int rows;
    private final int cols;
    private final int[] rowTransform;
    private final int[] rowInverse;
    private final int[] colTransform;
    private final int[] colInverse;

    /**
     * Create a randomization scheme for a matrix of a given size.
     * @param rows the number of rows in the matrix
     * @param cols the number of columns in the matrix
     */
    public RandomizedMatrix(int rows, int cols) {
      this.rows = rows;
      this.cols = cols;
      Random random = new Random();
      rowTransform = new int[rows];
      rowInverse = new int[rows];
      for (int i = 0; i < rows; i++) {
        rowTransform[i] = i;
      }
      // Shuffle the row indices.
      for (int i = rows - 1; i >= 0; i--) {
        int r = random.nextInt(i + 1);
        int temp = rowTransform[r];
        rowTransform[r] = rowTransform[i];
        rowTransform[i] = temp;
      }
      // Generate the inverse row indices.
      for (int i = 0; i < rows; i++) {
        rowInverse[rowTransform[i]] = i;
      }

      colTransform = new int[cols];
      colInverse = new int[cols];
      for (int i = 0; i < cols; i++) {
        colTransform[i] = i;
      }
      // Shuffle the column indices.
      for (int i = cols - 1; i >= 0; i--) {
        int r = random.nextInt(i + 1);
        int temp = colTransform[r];
        colTransform[r] = colTransform[i];
        colTransform[i] = temp;
      }
      // Generate the inverse column indices.
      for (int i = 0; i < cols; i++) {
        colInverse[colTransform[i]] = i;
      }
    }

    /**
     * Copy a given matrix into a new matrix, transforming each row index and
     * each column index according to the randomization scheme that was created
     * at construction time.
     * @param matrix the cost matrix to transform
     * @return a new matrix with row and column indices transformed
     */
    public float[][] transform(float[][] matrix) {
      float[][] result = new float[rows][cols];
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          result[rowTransform[i]][colTransform[j]] = matrix[i][j];
        }
      }
      return result;
    }

    /**
     * Copy a given matrix into a new matrix, transforming each row index and
     * each column index according to the inverse of the randomization scheme
     * that was created at construction time.
     * @param matrix the cost matrix to be inverted
     * @return a new matrix with row and column indices inverted
     */
    public float[][] invert(float[][] matrix) {
      float[][] result = new float[rows][cols];
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          result[rowInverse[i]][colInverse[j]] = matrix[i][j];
        }
      }
      return result;
    }

    /**
     * Given an array where each element {@code indices[i]} represents the
     * randomized column index corresponding to randomized row index {@code i},
     * create a new array with the corresponding inverted indices.
     * @param indices an array of transformed indices to be inverted
     * @return an array of inverted indices
     */
    public int[] invertIndices(int[] indices) {
      int[] result = new int[indices.length];
      for (int i = 0; i < indices.length; i++) {
        result[rowInverse[i]] = colInverse[indices[i]];
      }
      return result;
    }

    /**
     * Used to test the correctness of this class.
     * TODO Move this to a unit test?
     */
    public static void test() {
      int rows = 100;
      int cols = 100;
      float[][] matrix = new float[rows][cols];
      Random random = new Random();
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          matrix[i][j] = random.nextFloat();
        }
      }

      // Test that inverting a transformed matrix gives the original matrix.
      RandomizedMatrix rm = new RandomizedMatrix(rows, cols);
      float[][] transformed = rm.transform(matrix);
      float[][] invertedTransformed = rm.invert(transformed);
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          if (matrix[i][j] != invertedTransformed[i][j]) {
            throw new RuntimeException();
          }
        }
      }

      // Test that the indices on a transformed matrix can be inverted to give
      // the same values on the original matrix.
      int[] transformedIndices = new int[rows];
      for (int i = 0; i < rows; i++) {
        transformedIndices[i] = random.nextInt(cols);
      }
      int[] invertedTransformedIndices = rm.invertIndices(transformedIndices);
      float[] transformedValues = new float[rows];
      float[] invertedTransformedValues = new float[rows];
      for (int i = 0; i < rows; i++) {
        transformedValues[i] = transformed[i][transformedIndices[i]];
        invertedTransformedValues[i] = matrix[i][invertedTransformedIndices[i]];
      }
      Arrays.sort(transformedValues);
      Arrays.sort(invertedTransformedValues);
      if (!Arrays.equals(transformedValues, invertedTransformedValues)) {
        throw new RuntimeException();
      }

      System.out.println("Test passed");
    }
  }
}

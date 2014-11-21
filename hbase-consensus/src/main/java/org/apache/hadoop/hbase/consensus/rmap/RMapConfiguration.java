package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RMapConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(RMapConfiguration.class);

  private Configuration conf;

  private Map<String, RMap> appliedRMaps;
  private Map<URI, RMap> cachedRMaps;

  public RMapConfiguration(final Configuration conf) {
    this.conf = conf;
    this.appliedRMaps = new HashMap<>();
    this.cachedRMaps = new HashMap<>();
  }

  public static URI getRMapSubscription(final Configuration conf) {
    String[] subscriptionsList =
      conf.get(HConstants.RMAP_SUBSCRIPTION, "").split(",");
    if (subscriptionsList.length >= 1) {
      if (subscriptionsList.length > 1) {
        LOG.warn(String.format("We do not support multiple RMaps. " +
          "Using the first RMap as the correct one: %s", subscriptionsList[0]));
      }
      else if (!subscriptionsList[0].equals("")) {
        try {
          return new URI(subscriptionsList[0]);
        } catch (URISyntaxException e) {
          LOG.warn(String.format("Failed to parse URI for subscription %s: ",
            subscriptionsList[0]), e);
        }
      }
    }
    return null;
  }

  public static RMapReader getRMapReader(final Configuration conf,
          final URI uri) throws RMapException {
    switch (uri.getScheme()) {
      case "file":
        return new LocalReader();
      case "hdfs":
        return new HDFSReader(conf);
      default:
        throw new RMapException("No reader found for RMap: " + uri);
    }
  }

  public synchronized RMap getRMap(URI uri)
      throws IOException, RMapException {
    return getRMap(uri, false);
  }

  public synchronized RMap getRMap(URI uri, boolean reload)
      throws IOException, RMapException {
    try {
      RMapReader reader = getRMapReader(conf, uri);
      URI nonSymbolicURI = reader.resolveSymbolicVersion(uri);
      // Try to get a cached instance of the RMap.
      RMap rmap = cachedRMaps.get(nonSymbolicURI);
      if (reload || rmap == null) {
        // No cached instance was found, read it using the reader.
        RMapJSON encodedRMap = reader.readRMap(nonSymbolicURI);
        rmap = new RMap(encodedRMap.uri,
            new Parser(conf).parseEncodedRMap(encodedRMap.getEncodedRMap()),
            encodedRMap.signature);
        cachedRMaps.put(rmap.uri, rmap);
      }
      return rmap;
    } catch (URISyntaxException e) {
      throw new RMapException("URI syntax invalid for RMap: " + uri, e);
    } catch (JSONException e) {
      throw new RMapException("Failed to decode JSON for RMap: " + uri, e);
    }
  }

  /**
   * Reads and caches the RMap from the given URI and returns its signature.
   *
   * @param uri
   * @return
   */
  public synchronized String readRMap(final URI uri) throws IOException,
          RMapException {
    return getRMap(uri).signature;
  }

  public synchronized String readRMap(URI uri, boolean reload)
      throws IOException, RMapException {
    return getRMap(uri, reload).signature;
  }

  /**
   * Get the list of regions which need to be updated in order to transition to
   * this (version) of the RMap by the given URI.
   *
   * @param uri of the RMap
   * @return a list of regions
   */
  public synchronized Collection<HRegionInfo> getTransitionDelta(final URI uri)
          throws IOException, RMapException {
    RMap nextRMap = getRMap(uri);
    RMap currentRMap = appliedRMaps.get(RMapReader.getSchemeAndPath(uri));

    // The standard Set implementations seem to be using compareTo() for their
    // operations. On the HRegionInfo objects compareTo() and equals() have
    // different properties where equals() is needed here. What follows is a
    // poor mans Set comparison to determine which regions need to be modified
    // to make the RMap transition.
    if (nextRMap != null) {
      HashMap<String, HRegionInfo> delta = new HashMap<>();
      for (HRegionInfo next : nextRMap.regions) {
        delta.put(next.getEncodedName(), next);
      }

      if (currentRMap != null) {
        // Remove all regions already present in the current RMap from the
        // delta. This should use the {@link HRegionInfo.equals} method as it
        // should consider the favored nodes and replicas.
        for (HRegionInfo current : currentRMap.regions) {
          HRegionInfo next = delta.get(current.getEncodedName());
          if (next != null) {
            if (next.equals(current)) {
              delta.remove(next.getEncodedName());
            }
          }
        }
      }

      return delta.values();
    }

    return Collections.emptyList();
  }

  public synchronized void appliedRMap(final URI uri) throws IOException,
          RMapException {
    RMap previous = appliedRMaps.put(RMapReader.getSchemeAndPath(uri),
        getRMap(uri));
    // Purge the earlier version of the RMap from cache.
    if (previous != null) {
      cachedRMaps.remove(previous.uri);
    }
  }

  public synchronized boolean isRMapApplied(final URI uri) {
    RMap active = appliedRMaps.get(RMapReader.getSchemeAndPath(uri));
    if (active != null) {
      return active.uri.equals(uri);
    }
    return false;
  }

  public synchronized RMap getAppliedRMap(String uri) {
    return appliedRMaps.get(uri);
  }

  public synchronized List<HRegionInfo> getRegions(final URI uri)
          throws IOException, RMapException {
    RMap rmap = getRMap(uri);
    if (rmap == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(rmap.regions);
  }

  public synchronized void clearFromRMapCache(URI uri) {
    cachedRMaps.remove(uri);
  }

  /**
   * Replace the content of cached RMap. For testing only!
   *
   * @param uri
   * @param rMap
   */
  public synchronized void cacheCustomRMap(URI uri, RMap rMap) {
    cachedRMaps.put(uri, rMap);
    appliedRMaps.put(uri.toString(), rMap);
  }

  public class RMap {
    public final URI uri;
    public final List<HRegionInfo> regions;
    public final String signature;

    RMap(final URI uri, final List<HRegionInfo> regions,
         final String signature) {
      this.uri = uri;
      this.regions = regions;
      this.signature = signature;
    }

    /**
     * Return the quorum size in the RMap.
     * @return
     */
    public int getQuorumSize() {
      if (regions.size() == 0) {
        return 0;
      }
      return regions.get(0).getQuorumInfo().getQuorumSize();
    }

    /**
     * Return the list of regions that are served by the specified server.
     * @param hServerAddress
     * @return
     */
    public List<HRegionInfo> getRegionsForServer(HServerAddress hServerAddress) {
      List<HRegionInfo> ret = new ArrayList<HRegionInfo>();
      for (HRegionInfo region: regions) {
        if (region.getPeersWithRank().containsKey(hServerAddress)) {
          ret.add(region);
        }
      }
      return ret;
    }

    /**
     * Returns the set of servers that are hosting any of the regions in the RMap.
     * @return
     */
    public Set<HServerAddress> getAllServers() {
      Set<HServerAddress> ret = new HashSet<>();
      for (HRegionInfo region: regions) {
        ret.addAll(region.getPeersWithRank().keySet());
      }
      return ret;
    }

    /**
     * Create a customized RMap for test use only!
     *
     * @param uri
     * @param regions
     * @param signature
     * @return
     */
    public RMap createCustomizedRMap(URI uri,
                                     List<HRegionInfo> regions,
                                     String signature) {
      return new RMapConfiguration.RMap(
          uri == null ? this.uri : uri,
          regions == null ? this.regions : regions,
          signature == null ? this.signature : signature
      );
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof RMap)) {
        return false;
      }
      RMap that = (RMap)obj;
      if (this.regions == null || that.regions == null || this.regions.size() != that.regions.size()) {
        return false;
      }
      Set<HRegionInfo> regionInfos = new TreeSet<>();
      regionInfos.addAll(regions);
      for (HRegionInfo region : that.regions) {
        if (!regionInfos.contains(region)) {
          return false;
        }
        regionInfos.remove(region);
      }
      return regionInfos.isEmpty();
    }
  }

  /**
   * Creates a temporary name for an RMap, based on the date and time.
   * @return
   */
  public static String createRMapName() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HHmmss");
    return "rmap.json." + format.format(System.currentTimeMillis());
  }

  /**
   * View information about an RMap. Currently only prints its signature.
   * @param args
   */
  public static void main(String[] args) throws ParseException,
    URISyntaxException, RMapException, IOException {
    Options options = new Options();
    options.addOption("r", "rmap", true, "Name of the rmap");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (!cmd.hasOption("r")) {
      System.out.println("Please specify the rmap with -r");
      return;
    }

    String rmapUriStr = cmd.getOptionValue("r");
    RMapConfiguration conf = new RMapConfiguration(new Configuration());
    String rmapStr = conf.readRMap(new URI(rmapUriStr));
    LOG.debug("RMap Signature: " + rmapStr);
  }
}

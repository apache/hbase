/**
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;

/**
 * Class to track information about regions availability and to calculate day availability and week availability.
 *
 * Only information for latest MAX_LOG_TIME_DIF milliseconds is stored.
 * It stores information for each region in zookeeper in the following format:
 * /hbase/regionchecker/data/ - directory where data about every region is stored (info about some region is
 *                              stored in file with region hash as file name)
 * /hbase/regionchecker/previous/ - directory where info about latest region's fail is stored, or 0 if the region
 *                              is assigned (info about some region is stored in file with region hash as file name)
 *
 * data is stored in the following format:
 * "ms duration\n"
 * ms - start time in millisecods (got from EnvironmentEdgeManager.currentTimeMillis())
 * duration  - duration of being unavailable - in milliseconds.
 *
 * @author ibra
 */
public class RegionChecker {
  protected static final Log LOG = LogFactory.getLog(RegionChecker.class);

  private ZooKeeperWrapper zkWrapper;
  private HMaster master;

  private static final long WEEK_TIME_DIF = 7L * 24L * 60L * 60L * 1000L;
  private static final long DAY_TIME_DIF = 24L * 60L * 60L * 1000L;
  private static final long MAX_LOG_TIME_DIF = 7L * 24L * 60L * 60L * 1000L;
  private final String ZNodeName = "regionchecker";

  /**
   * is regionCheckerEnabled - got from knob
   */
  private final boolean regionCheckerEnabled;

  public boolean isEnabled()
  {
    return regionCheckerEnabled;
  }

  public RegionChecker(final HMaster master) throws SocketException {
    Configuration conf = master.getConfiguration();
    this.regionCheckerEnabled = conf.getBoolean(HConstants.REGION_CHECKER_ENABLED, HConstants.DEFAULT_REGION_CHECKER_ENABLED);
    this.master = master;
    if (this.regionCheckerEnabled) {
      this.zkWrapper = ZooKeeperWrapper.getInstance(conf, master.getZKWrapperName());
    }
  }

  /**
   * When region becomes closed this method is called to store information of each region's latest
   * fail
   * @param regionHash
   */
  synchronized public void becameClosed(final HRegionInfo rInfo) {
    this.becameClosed(rInfo, EnvironmentEdgeManager.currentTimeMillis());
  }

  /**
   * When region becomes closed this method is called to store information of each region's latest
   * fail
   * @param regionHash
   */
  synchronized public void becameClosed(final HRegionInfo rInfo, long createdTime) {
    if (!this.regionCheckerEnabled) {
      return;
    }

    final String region = rInfo.getRegionNameAsString();
    String previous = this.getPrevious(region);
    if (previous.equals("") || previous.equals("0")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("region '" + region + ":\t becameClosed");
      }

      this.setPrevious(region, Long.toString(createdTime));
    } else {
      LOG.warn("region '" + region + "' becameClosed came second: previous is already set to '" + previous + "' and now is '" + Long.toString(createdTime) + "'");
    }
  }

  /**
   * When region becomes opened this method is called to store information of each region's
   * unassigment interval
   * @param regionHash
   */
  synchronized public void becameOpened(final HRegionInfo rInfo) {
    if (!this.regionCheckerEnabled) {
      return;
    }

    final String region = rInfo.getRegionNameAsString();
    String previous = this.getPrevious(region);
    if (!previous.equals("") && !previous.equals("0")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("region '" + region + ":\t becameOpened");
      }

      this.setPrevious(region, "0");
      long currTime = EnvironmentEdgeManager.currentTimeMillis();
      long prevTime = Long.parseLong(previous);

      String info = prevTime + " " + (currTime - prevTime) + "\n";

      if(LOG.isDebugEnabled()) {
        LOG.debug("region '" + region + ":\t" + info);
      }

      String znodeData = this.getData(region);
      String strData = "";
      StringTokenizer in = new StringTokenizer(znodeData);
      while (in.hasMoreTokens()) {
        long ms = Long.parseLong(in.nextToken());
        long duration = Long.parseLong(in.nextToken());

        // we store only information of the latest MAX_LOG_TIME_DIF milliseconds
        if (currTime - ms <= MAX_LOG_TIME_DIF) {
          strData = znodeData.substring(znodeData.indexOf(ms + " " + duration));
          break;
        }
      }

      this.setData(region, strData + info);
    } else {
      LOG.warn("region '" + region + "' was called to became Opened without being previously called to became Closed");
    }
  }

  /**
   * Prints to log: cluster's last day availability cluster's last week availability each region's
   * last day availability each region's last week availability
   */
  private void printAvailabilityInfoToLog() {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    LOG.debug("getLastDayAvailability =\t" + this.getLastDayAvailability());
    LOG.debug("getLastWeekAvailability =\t" + this.getLastWeekAvailability());

    Map<String, RegionAvailabilityInfo> detailedDayInfo = this.getDetailedLastDayAvailability();
    LOG.debug("detailedDayInfo:\n");
    for (String key : detailedDayInfo.keySet()) {
      LOG.debug("\t" + "[" + key + "]" + " - " + detailedDayInfo.get(key));
    }

    Map<String, RegionAvailabilityInfo> detailedWeekInfo = this.getDetailedLastWeekAvailability();
    LOG.debug("detailedWeekInfo:\n");
    for (String key : detailedWeekInfo.keySet()) {
      LOG.debug("\t" + "[" + key + "]" + " - " + detailedWeekInfo.get(key));
    }
  }

  /**
   * @return double - cluster's availability for last day
   */
  public double getLastDayAvailability() {
    return this.getAvailability(DAY_TIME_DIF);
  }

  /**
   * @return double - cluster's availability for last week
   */
  public double getLastWeekAvailability() {
    return this.getAvailability(WEEK_TIME_DIF);
  }

  /**
   * @return Map<String, RegionAvailabilityInfo> - each pair is <region, its availabilityInfo for last day>
   */
  public Map<String, RegionAvailabilityInfo> getDetailedLastDayAvailability() {
    return this.getDetailedAvailability(DAY_TIME_DIF);
  }

  /**
   * @return Map<String, RegionAvailabilityInfo> - each pair is <region, its availabilityInfo for last week>
   */
  public Map<String, RegionAvailabilityInfo> getDetailedLastWeekAvailability() {
    return this.getDetailedAvailability(WEEK_TIME_DIF);
  }

  /**
   * @param timeDif - method uses information of last timeDif milliseconds to calculate availability
   * @return cluster's availability of last timeDif milliseconds
   */
  private double getAvailability(long timeDif) {
    if (!this.regionCheckerEnabled) {
      return -1.0;
    }

    Map<String, RegionAvailabilityInfo> detailed = getDetailedAvailability(timeDif);

    double res = 0.0;
    for(RegionAvailabilityInfo info : detailed.values()) {
      res += info.getAvailability();
    }

    res += this.master.getRegionManager().getRegionsCount() - detailed.size();
    res /= this.master.getRegionManager().getRegionsCount();

    return res;
  }

  /**
   * @param timeDif - method uses information of last timeDif milliseconds to calculate availability
   * @return each regions's availability in Map<String, RegionAvailabilityInfo> - each pair is <region, its
   *         availabilityInfo for last timeDif milliseconds>
   */
  private Map<String, RegionAvailabilityInfo> getDetailedAvailability(long timeDif) {
    if (!this.regionCheckerEnabled) {
      return new HashMap<String, RegionAvailabilityInfo>();
    }
    long curTime = EnvironmentEdgeManager.currentTimeMillis();

    Map<String, RegionAvailabilityInfo> availabilityMap = new HashMap<String, RegionAvailabilityInfo>();

    Iterable<String> dataNodes = this.zkWrapper.listZnodes(this.joinPath(this.zkWrapper.parentZNode, this.ZNodeName, "data"));
    if (dataNodes != null) {
      for (String node : dataNodes) {
        availabilityMap.put(node, new RegionAvailabilityInfo().addAvailabilityInfoFromData(this.getData(node), curTime, timeDif));
      }
    }

    Iterable<String> prevNodes = this.zkWrapper.listZnodes(this.joinPath(this.zkWrapper.parentZNode, this.ZNodeName, "previous"));
    if (prevNodes != null) {
      for (String node : prevNodes) {
        String previous = this.getPrevious(node);
        // check if file exits and this region is currently unassigned
        if (!previous.equals("") && !previous.equals("0")) {
          if(!availabilityMap.containsKey(node)) {
            availabilityMap.put(node, new RegionAvailabilityInfo());
          }
          availabilityMap.put(node, availabilityMap.get(node).addDuration(Long.parseLong(previous), timeDif));
        }
      }
    }

    return availabilityMap;
  }

  public class RegionAvailabilityInfo
  {
    private long intervalsCount;
    private long lastIntervalStart;
    private long lastIntervalEnd;
    private boolean isCurrentlyAssigned;
    private double availability;

    private RegionAvailabilityInfo()
    {
      isCurrentlyAssigned = true;
      availability = 1.0;
      intervalsCount = 0;
    }

    /**
     * adds unavailability duration (interval [from-NOW]) to AvailabilityInfo
     * @param from - unavailability interval start
     * @param timeDif - time through which availability is calculated
     * @return this
     */
    private RegionAvailabilityInfo addDuration(long from, long timeDif)
    {
      isCurrentlyAssigned = false;
      return addDuration(from, EnvironmentEdgeManager.currentTimeMillis(), timeDif);
    }

    /**
     * adds unavailability duration (interval [from-to]) to AvailabilityInfo
     * @param from - unavailability interval start
     * @param to - unavailability interval end
     * @param timeDif - time through which availability is calculated
     * @return this
     */
    private RegionAvailabilityInfo addDuration(long from, long to, long timeDif)
    {
      availability -= (double) (to-from) / timeDif;
      lastIntervalStart = from;
      lastIntervalEnd = to;
      intervalsCount++;

      return this;
    }

    /**
     * @return last unavailability interval start time
     */
    public long getLastIntervalStart()
    {
      return lastIntervalStart;
    }

    /**
     * @return last unavailability interval end time
     */
    public long getLastIntervalEnd()
    {
      return lastIntervalEnd;
    }

    /**
     * @return availability
     */
    public double getAvailability()
    {
      return availability;
    }

    /**
     * @return if last unavailability interval is still continues
     */
    public boolean isCurrentlyAssigned()
    {
      return isCurrentlyAssigned;
    }

    /**
     * @return duration of last unavailability interval
     */
    public long getDuration()
    {
      return lastIntervalEnd-lastIntervalStart;
    }

    /**
     * @return amount of unavailability intervals
     */
    public long getIntervalsCount()
    {
      return intervalsCount;
    }

    public String getInterval()
    {
      if(intervalsCount == 0)
        return "";
      SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss SSS");
      return (dateFormat.format(new Date(lastIntervalStart)) + "  ->  " +  (isCurrentlyAssigned()?dateFormat.format(new Date(lastIntervalEnd)):"NOW"));
    }

    /**
     * get Availability info stored in znodeData
     * @param znodeData - string that contains data
     * @param curTime - current time in ms
     * @param timeDif - time through which availability is calculated
     * @return AvailabilityInfo object
     */
    private RegionAvailabilityInfo addAvailabilityInfoFromData(String znodeData, long curTime, long timeDif) {
      StringTokenizer in = new StringTokenizer(znodeData);
      while (in.hasMoreTokens()) {
        long ms = Long.parseLong(in.nextToken());
        long duration = Long.parseLong(in.nextToken());
        if (curTime - ms <= timeDif) {
          addDuration(ms, ms+duration, timeDif);
        }
      }
      return this;
    }
  }

  /**
   * @param regionHash
   * @return data stored in this region's previous-file or "" if there is no such file
   */
  private String getPrevious(String region) {
    return this.getZnode("previous", region);
  }

  /**
   * method sets strData to this region's previous-file
   * @param regionHash
   * @param strData
   */
  private void setPrevious(String region, String strData) {
    this.setZnode("previous", region, strData);
  }

  /**
   * @param regionHash
   * @return data stored in this region's data-file or "" if there is no such file
   */
  private String getData(String region) {
    return this.getZnode("data", region);
  }

  /**
   * method sets strData to this region's data-file
   * @param regionHash
   * @param strData
   */
  private void setData(String region, String strData) {
    this.setZnode("data", region, strData);
  }

  /**
   * method returns text stored in folder/regionHash
   * @param folder
   * @param regionHash
   * @return data from folder/regionHash
   * @throws IOException
   */
  private String getZnode(String folder, String region) {
    String path = this.joinPath(this.ZNodeName, folder, region);
    this.ensureExists(path);

    try {
      byte[] bt = this.zkWrapper.readZNode(path, null);
      return bt == null ? "" : new String(bt);
    } catch (IOException e) {
      LOG.error("Exception occured during read from " + path, e);
      return "";
    }
  }

  /**
   * writes strData to folder/regionHash
   * @param folder
   * @param regionHash
   * @param strData
   */
  private void setZnode(String folder, String region, String strData) {
    String path = this.joinPath(this.ZNodeName, folder, region);
    this.ensureExists(path);

    try {
      this.zkWrapper.writeZNode(this.zkWrapper.parentZNode, path, strData);
    } catch (final InterruptedException e) {
      LOG.error("Can't get data from ZNode '" + this.zkWrapper.parentZNode + "->"
          + path + "' after calling ensureExists.", e);
    } catch (final KeeperException e) {
      LOG.error("Can't set data to ZNode '" + this.zkWrapper.parentZNode + "->"
          + path + "' after calling ensureExists.", e);
    }
  }

  /**
   * Make sure this znode exists by creating it if it's missing
   * @param path
   */
  private void ensureExists(String path) {
    path = this.joinPath(this.zkWrapper.parentZNode, path);
    this.zkWrapper.ensureExists(path);
  }

  /**
   * @param args - names of folders, files
   * @return joined path
   */
  private String joinPath(String... args) {
    String res = "";
    if (args.length > 0) {
      res = args[0];
      for (int i = 1; i < args.length; i++) {
        res = this.zkWrapper.getZNode(res, args[i]);
      }
    }
    return res;
  }
}

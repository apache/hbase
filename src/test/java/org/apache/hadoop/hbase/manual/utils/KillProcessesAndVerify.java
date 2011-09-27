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
package org.apache.hadoop.hbase.manual.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.manual.HBaseTest;
import org.apache.hadoop.hbase.util.Bytes;


public class KillProcessesAndVerify extends Thread
{
  private static final Log LOG = LogFactory.getLog(KillProcessesAndVerify.class);
  /* One minute in millis */
  public static final int TIME_INTERVAL_ONE_MIN = 1*(60*1000);
  /* wait between killing an RS/DN - set to 1 hour */
  public int TIME_INTERVAL_BETWEEN_KILLS = 60 * TIME_INTERVAL_ONE_MIN;
  /* percent of time we want the RS killed (as opposed to the DN) */
  public int RS_KILL_PERCENT = 80;
  /* how many keys to verify when the server is killed - 4 minute window */
  public int NUM_KEYS_TO_VERIFY = 25000;
  /* the HTable */
  HTable hTable_ = null;
  /* the cluster name */
  public static String clusterHBasePath_ = null;
  public static String clusterHDFSPath_ = null;

  public static Random random_ = new Random();
  private Runtime runtime_ = null;

  public KillProcessesAndVerify(String clusterHBasePath, String clusterHDFSPath, int timeIntervalBetweenKills, int rSKillPercent, int numKeysToVerify) {
    runtime_ = Runtime.getRuntime();
    hTable_ = HBaseUtils.getHTable(HBaseTest.configList_.get(0), MultiThreadedWriter.tableName_);
    clusterHBasePath_ = clusterHBasePath;
    clusterHDFSPath_ = clusterHDFSPath;
    TIME_INTERVAL_BETWEEN_KILLS = timeIntervalBetweenKills * TIME_INTERVAL_ONE_MIN;
    RS_KILL_PERCENT = rSKillPercent;
    NUM_KEYS_TO_VERIFY = numKeysToVerify;
  }

  public void run() {
    while(true) {
      try
      {
        // wait for the next iteration of kills
        Thread.sleep(TIME_INTERVAL_BETWEEN_KILLS);

        // choose if we are killing an RS or a DN
        boolean operateOnRS = (random_.nextInt(100) < RS_KILL_PERCENT);
        // choose the node we want to kill
        long lastWrittenKey = MultiThreadedWriter.currentKey_.get();
        HRegionLocation hloc = hTable_.getRegionLocation(Bytes.toBytes(lastWrittenKey));
        String nodeName = hloc.getServerAddress().getHostname();
        LOG.debug("Picked type = " + (operateOnRS?"REGIONSERVER":"DATANODE") + ", node = " + nodeName);

        // kill the server
        String killCommand = getKillCommand(nodeName, operateOnRS);
        executeCommand(killCommand);
        LOG.debug("Killed " + (operateOnRS?"REGIONSERVER":"DATANODE") + " on node " + nodeName + ", waiting...");

        if(true) {
          break;
        }

        // wait for a while
        Thread.sleep(TIME_INTERVAL_ONE_MIN/2);

        // start the server
        String startCommand = getStartCommand(nodeName, operateOnRS);
        executeCommand(startCommand);
        LOG.debug("Started " + (operateOnRS?"REGIONSERVER":"DATANODE") + " on node " + nodeName + ", waiting...");

        // wait for a while
        Thread.sleep(TIME_INTERVAL_ONE_MIN);

        // verify the reads that happened in the last 4 minutes - last 25000 keys
        int endIdx = MultiThreadedWriter.insertedKeySet_.size() - 1;
        int startIdx = (endIdx < NUM_KEYS_TO_VERIFY) ? 0 : (endIdx - NUM_KEYS_TO_VERIFY + 1);
        verifyKeys(MultiThreadedWriter.insertedKeySet_, startIdx, endIdx, MultiThreadedWriter.failedKeySet_);
        LOG.debug("Done verifying keys, sleep till next interval");
      }
      catch (IOException e1)
      {
        e1.printStackTrace();
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }
  }

  public String getKillCommand(String nodeName, boolean rsCommand) {
    // construct the remote kill command
    String processName = rsCommand?"HRegionServer":"DataNode";
    String killCmd = "/usr/bin/pgrep -f " + processName + " | /usr/bin/xargs /bin/kill -9";

    // put the ssh call
    String remoteKillCmd = "ssh " + nodeName + " " + killCmd;

    return remoteKillCmd;
  }

  public String getStartCommand(String nodeName, boolean rsCommand) {
    // construct the remote start up command
    String startCmd =
      rsCommand?
      clusterHBasePath_ +"/bin/hbase-daemon.sh start regionserver":
      clusterHDFSPath_ + "/bin/hadoop-daemons.sh --config /usr/local/hadoop/HDFS-" + clusterHDFSPath_ + "/conf start datanode";

    // put the ssh call
    String remoteStartCmd = "ssh " + nodeName + " " + startCmd;

    return remoteStartCmd;
  }

  public void verifyKeys(List<Long> insertedKeys, int verifyStartIdx, int verifyEndIdx, List<Long> failedKeys) {
    Get get = null;

    for (int idx = verifyStartIdx; idx <= verifyEndIdx; idx++) {

      long rowKey = insertedKeys.get(idx);

      // skip any key that has not been inserted
      if(failedKeys.contains(rowKey)) {
        continue;
      }
      // query hbase for the key
      get = new Get(MultiThreadedWriter.HBaseWriter.longToByteArrayKey(rowKey));
      get.addFamily(MultiThreadedWriter.columnFamily_);
      try
      {
        Result result = hTable_.get(get);
      }
      catch (IOException e)
      {
        // if this is hit, the key was not found
        LOG.error("KEY " + rowKey + " was NOT FOUND, it was claimed to be inserted.");
        e.printStackTrace();
      }
    }
  }

  public void executeCommand(String command) throws InterruptedException, IOException {
    LOG.debug("Command : " + command);
    Process p = runtime_.exec(command);
//    p.waitFor();
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

    // read the output from the command
    System.out.println("Here is the standard output of the command:\n");
    String s = null;

    while ((s = stdInput.readLine()) != null) {
      System.out.println(s);
    }

    // read any errors from the attempted command
    System.out.println("Here is the standard error of the command (if any):\n");
    while ((s = stdError.readLine()) != null) {
      System.out.println(s);
    }
  }
}

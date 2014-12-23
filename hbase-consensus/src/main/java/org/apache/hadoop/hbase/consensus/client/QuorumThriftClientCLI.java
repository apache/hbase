package org.apache.hadoop.hbase.consensus.client;

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


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.facebook.swift.service.ThriftClient;
import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.server.ConsensusService;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;


public class QuorumThriftClientCLI {

  public static void main(String[] args) throws Exception {

    Options opts = new Options();
    Option opt;
    opt = new Option("r", "region", true, "The region ID");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("s", "servers", true, "A comma-separated list of address:port");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("t", "timeout", true, "Controls connection, read and write timeouts");
    opts.addOption(opt);

    try {
      CommandLine cmd = new GnuParser().parse(opts, args);

      String serverList = cmd.getOptionValue("servers");
      String regionId = regionId = cmd.getOptionValue("region");
      String [] servers = serverList.split(",");

      int connectionRetry = 3;
      int timeout = Integer.parseInt(cmd.getOptionValue("timeout", "5000"));
      for (String server : servers) {
        server = server.trim();
        System.out.println("Getting QuorumThriftClientAgent for " + server);
        QuorumThriftClientAgent agent = new QuorumThriftClientAgent(
            server, timeout, timeout, timeout, connectionRetry);
        System.out.println("QuorumThriftClientAgent for " + server + " = " + agent);
        PeerStatus status = agent.getPeerStatus(regionId);
        System.out.println("PeerStatus for " + server + " : " + status);
      }
    } catch (ParseException ex) {
      System.err.println("Failed to parse the command line: " + ex);
      ex.printStackTrace();
      printHelp(opts);
      System.exit(1);
    }
  }

  private static void printHelp(Options opts) {
    new HelpFormatter().printHelp(
      "QuorumLoadTestClient -r regionID -s h1:port,...,h3:port", opts
    );
  }
}

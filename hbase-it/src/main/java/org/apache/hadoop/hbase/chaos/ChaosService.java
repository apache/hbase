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

package org.apache.hadoop.hbase.chaos;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;

/**
 * Class used to start/stop Chaos related services (currently chaosagent)
 */
@InterfaceAudience.Private
public class ChaosService {

  private static final Logger LOG = LoggerFactory.getLogger(ChaosService.class.getName());

  public static void execute(String[] args, Configuration conf) {
    LOG.info("arguments : " + Arrays.toString(args));

    try {
      CommandLine cmdline = new GnuParser().parse(getOptions(), args);
      if (cmdline.hasOption(ChaosServiceName.CHAOSAGENT.toString().toLowerCase())) {
        String actionStr = cmdline.getOptionValue(ChaosServiceName.CHAOSAGENT.toString().toLowerCase());
        try {
          ExecutorAction action = ExecutorAction.valueOf(actionStr.toUpperCase());
          if (action == ExecutorAction.START) {
            ChaosServiceStart(conf, ChaosServiceName.CHAOSAGENT);
          } else if (action == ExecutorAction.STOP) {
            ChaosServiceStop();
          }
        } catch (IllegalArgumentException e) {
          LOG.error("action passed: {} Unexpected action. Please provide only start/stop.",
            actionStr, e);
          throw new RuntimeException(e);
        }
      } else {
        LOG.error("Invalid Options");
      }
    } catch (Exception e) {
      LOG.error("Error while starting ChaosService : ", e);
    }
  }

  private static void ChaosServiceStart(Configuration conf, ChaosServiceName serviceName) {
    switch (serviceName) {
      case CHAOSAGENT:
        ChaosAgent.stopChaosAgent.set(false);
        try {
          Thread t = new Thread(new ChaosAgent(conf,
            ChaosUtils.getZKQuorum(conf), ChaosUtils.getHostName()));
          t.start();
          t.join();
        } catch (InterruptedException | UnknownHostException e) {
          LOG.error("Failed while executing next task execution of ChaosAgent on : {}",
            serviceName, e);
        }
        break;
      default:
        LOG.error("Service Name not known : " + serviceName.toString());
    }
  }

  private static void ChaosServiceStop() {
    ChaosAgent.stopChaosAgent.set(true);
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption(new Option("c", ChaosServiceName.CHAOSAGENT.toString().toLowerCase(),
      true, "expecting a start/stop argument"));
    options.addOption(new Option("D", ChaosServiceName.GENERIC.toString(),
      true, "generic D param"));
    LOG.info(Arrays.toString(new Collection[] { options.getOptions() }));
    return options;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    new GenericOptionsParser(conf, args);

    ChoreService choreChaosService = null;
    ScheduledChore authChore = AuthUtil.getAuthChore(conf);

    try {
      if (authChore != null) {
        choreChaosService = new ChoreService(ChaosConstants.CHORE_SERVICE_PREFIX);
        choreChaosService.scheduleChore(authChore);
      }

      execute(args, conf);
    } finally {
      if (authChore != null)
        choreChaosService.shutdown();
    }
  }

  enum ChaosServiceName {
    CHAOSAGENT,
    GENERIC
  }


  enum ExecutorAction {
    START,
    STOP
  }
}

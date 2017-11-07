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

package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeperMain;

/**
 * Tool for running ZookeeperMain from HBase by  reading a ZooKeeper server
 * from HBase XML configuration.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class ZooKeeperMainServer {
  private static final String SERVER_ARG = "-server";

  public String parse(final Configuration c) {
    return ZKConfig.getZKQuorumServersString(c);
  }

  /**
   * ZooKeeper 3.4.6 broke being able to pass commands on command line.
   * See ZOOKEEPER-1897.  This class is a hack to restore this faclity.
   */
  private static class HACK_UNTIL_ZOOKEEPER_1897_ZooKeeperMain extends ZooKeeperMain {
    public HACK_UNTIL_ZOOKEEPER_1897_ZooKeeperMain(String[] args)
    throws IOException, InterruptedException {
      super(args);
      // Make sure we are connected before we proceed. Can take a while on some systems. If we
      // run the command without being connected, we get ConnectionLoss KeeperErrorConnection...
      long startTime = System.currentTimeMillis();
      while (!this.zk.getState().isConnected()) {
        Thread.sleep(1);
        if ((System.currentTimeMillis() - startTime) > 10000) {
          throw new InterruptedException("Failed connect " + this.zk);
        }
      }
    }

    /**
     * Run the command-line args passed.  Calls System.exit when done.
     * @throws KeeperException
     * @throws IOException
     * @throws InterruptedException
     */
    void runCmdLine() throws KeeperException, IOException, InterruptedException {
      processCmd(this.cl);
      System.exit(0);
    }
  }

  /**
   * @param args
   * @return True if argument strings have a '-server' in them.
   */
  private static boolean hasServer(final String args[]) {
    return args.length > 0 && args[0].equals(SERVER_ARG);
  }

  /**
   * @param args
   * @return True if command-line arguments were passed.
   */
  private static boolean hasCommandLineArguments(final String args[]) {
    if (hasServer(args)) {
      if (args.length < 2) throw new IllegalStateException("-server param but no value");
      return args.length > 2;
    }
    return args.length > 0;
  }

  /**
   * Run the tool.
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) throws Exception {
    String [] newArgs = args;
    if (!hasServer(args)) {
      // Add the zk ensemble from configuration if none passed on command-line.
      Configuration conf = HBaseConfiguration.create();
      String hostport = new ZooKeeperMainServer().parse(conf);
      if (hostport != null && hostport.length() > 0) {
        newArgs = new String[args.length + 2];
        System.arraycopy(args, 0, newArgs, 2, args.length);
        newArgs[0] = "-server";
        newArgs[1] = hostport;
      }
    }
    // If command-line arguments, run our hack so they are executed.
    // ZOOKEEPER-1897 was committed to zookeeper-3.4.6 but elsewhere in this class we say
    // 3.4.6 breaks command-processing; TODO.
    if (hasCommandLineArguments(args)) {
      HACK_UNTIL_ZOOKEEPER_1897_ZooKeeperMain zkm =
        new HACK_UNTIL_ZOOKEEPER_1897_ZooKeeperMain(newArgs);
      zkm.runCmdLine();
    } else {
      ZooKeeperMain.main(newArgs);
    }
  }
}

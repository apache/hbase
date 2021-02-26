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
package org.apache.hadoop.hbase.thrift2;

import static org.apache.hadoop.hbase.thrift.Constants.READONLY_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_READONLY_ENABLED;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_READONLY_ENABLED_DEFAULT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.HBaseServiceHandler;
import org.apache.hadoop.hbase.thrift.HbaseHandlerMetricsProxy;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TProcessor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;

/**
 * ThriftServer - this class starts up a Thrift server which implements the HBase API specified in
 * the HbaseClient.thrift IDL file.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Change the name will be an incompatible change, will do it later")
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ThriftServer extends org.apache.hadoop.hbase.thrift.ThriftServer {
  private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);


  public ThriftServer(Configuration conf) {
    super(conf);
  }

  @Override
  protected void printUsageAndExit(Options options, int exitCode)
      throws Shell.ExitCodeException {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
        "To start the Thrift server run 'hbase-daemon.sh start thrift2' or " +
            "'hbase thrift2'\n" +
            "To shutdown the thrift server run 'hbase-daemon.sh stop thrift2' or" +
            " send a kill signal to the thrift server pid",
        true);
    throw new Shell.ExitCodeException(exitCode, "");
  }

  @Override
  protected HBaseServiceHandler createHandler(Configuration conf, UserProvider userProvider)
      throws IOException {
    return new ThriftHBaseServiceHandler(conf, userProvider);
  }

  @Override
  protected ThriftMetrics createThriftMetrics(Configuration conf) {
    return new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.TWO);
  }

  @Override
  protected TProcessor createProcessor() {
    return new THBaseService.Processor<>(HbaseHandlerMetricsProxy
        .newInstance((THBaseService.Iface) hbaseServiceHandler, metrics, conf));
  }

  @Override
  protected void addOptions(Options options) {
    super.addOptions(options);
    options.addOption("ro", READONLY_OPTION, false,
        "Respond only to read method requests [default: false]");
  }

  @Override
  protected void parseCommandLine(CommandLine cmd, Options options) throws Shell.ExitCodeException {
    super.parseCommandLine(cmd, options);
    boolean readOnly = THRIFT_READONLY_ENABLED_DEFAULT;
    if (cmd.hasOption(READONLY_OPTION)) {
      readOnly = true;
    }
    conf.setBoolean(THRIFT_READONLY_ENABLED, readOnly);
  }

  /**
   * Start up the Thrift2 server.
   */
  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    // for now, only time we return is on an argument error.
    final int status = ToolRunner.run(conf, new ThriftServer(conf), args);
    System.exit(status);
  }

}

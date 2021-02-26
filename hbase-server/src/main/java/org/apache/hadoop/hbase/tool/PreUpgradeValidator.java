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
package org.apache.hadoop.hbase.tool;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorValidator;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for validating that cluster can be upgraded from HBase 1.x to 2.0
 * <p>
 * Available validations:
 * <ul>
 * <li>validate-cp: Validates Co-processors compatibility</li>
 * <li>validate-dbe: Check Data Block Encoding for column families</li>
 * <li>validate-hfile: Check for corrupted HFiles</li>
 * </ul>
 * </p>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class PreUpgradeValidator implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(PreUpgradeValidator.class);

  public static final String TOOL_NAME = "pre-upgrade";
  public static final String VALIDATE_CP_NAME = "validate-cp";
  public static final String VALIDATE_DBE_NAME = "validate-dbe";
  public static final String VALIDATE_HFILE = "validate-hfile";

  private Configuration configuration;

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

  private void printUsage() {
    System.out.println("usage: hbase " + TOOL_NAME + " command ...");
    System.out.println("Available commands:");
    System.out.printf(" %-15s Validate co-processors are compatible with HBase%n",
        VALIDATE_CP_NAME);
    System.out.printf(" %-15s Validate DataBlockEncodings are compatible with HBase%n",
        VALIDATE_DBE_NAME);
    System.out.printf(" %-15s Validate HFile contents are readable%n",
        VALIDATE_HFILE);
    System.out.println("For further information, please use command -h");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage();
      return AbstractHBaseTool.EXIT_FAILURE;
    }

    Tool tool;

    switch (args[0]) {
      case VALIDATE_CP_NAME:
        tool = new CoprocessorValidator();
        break;
      case VALIDATE_DBE_NAME:
        tool = new DataBlockEncodingValidator();
        break;
      case VALIDATE_HFILE:
        tool = new HFileContentValidator();
        break;
      case "-h":
        printUsage();
        return AbstractHBaseTool.EXIT_FAILURE;
      default:
        System.err.println("Unknown command: " + args[0]);
        printUsage();
        return AbstractHBaseTool.EXIT_FAILURE;
    }

    tool.setConf(getConf());
    return tool.run(Arrays.copyOfRange(args, 1, args.length));
  }

  public static void main(String[] args) {
    int ret;

    Configuration conf = HBaseConfiguration.create();

    try {
      ret = ToolRunner.run(conf, new PreUpgradeValidator(), args);
    } catch (Exception e) {
      LOG.error("Error running command-line tool", e);
      ret = AbstractHBaseTool.EXIT_FAILURE;
    }

    System.exit(ret);
  }
}

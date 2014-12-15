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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Generate a classpath string containing any jars required by mapreduce jobs. Specify
 * additional values by providing a comma-separated list of paths via -Dtmpjars.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class MapreduceDependencyClasspathTool implements Tool {

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length > 0) {
      System.err.println("Usage: hbase mapredcp [-Dtmpjars=...]");
      System.err.println("  Construct a CLASSPATH containing dependency jars required to run a mapreduce");
      System.err.println("  job. By default, includes any jars detected by TableMapReduceUtils. Provide");
      System.err.println("  additional entries by specifying a comma-separated list in tmpjars.");
      return 0;
    }

    TableMapReduceUtil.addHBaseDependencyJars(getConf());
    System.out.println(TableMapReduceUtil.buildDependencyClasspath(getConf()));
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    // Silence the usual noise. This is probably fragile...
    Logger logger = Logger.getLogger("org.apache.hadoop.hbase");
    if (logger != null) {
      logger.setLevel(Level.WARN);
    }
    System.exit(ToolRunner.run(
      HBaseConfiguration.create(), new MapreduceDependencyClasspathTool(), argv));
  }
}

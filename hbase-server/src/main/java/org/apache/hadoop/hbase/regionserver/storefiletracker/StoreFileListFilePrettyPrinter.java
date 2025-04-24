/*
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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.OptionGroup;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class StoreFileListFilePrettyPrinter extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListFilePrettyPrinter.class);

  private Options options = new Options();

  private final String fileOption = "f";
  private final String columnFamilyOption = "cf";
  private final String regionOption = "r";
  private final String tableNameOption = "t";

  private final String cmdString = "sft";

  private String namespace;
  private String regionName;
  private String columnFamily;
  private String tableName;
  private Path path;
  private PrintStream err = System.err;
  private PrintStream out = System.out;

  public StoreFileListFilePrettyPrinter() {
    super();
    init();
  }

  public StoreFileListFilePrettyPrinter(Configuration conf) {
    super(conf);
    init();
  }

  private void init() {
    OptionGroup files = new OptionGroup();
    options.addOption(new Option(tableNameOption, "table", true,
      "Table to scan. Pass table name; e.g. test_table"));
    options.addOption(new Option(columnFamilyOption, "columnfamily", true,
      "column family to scan. Pass column family name; e.g. f"));
    files.addOption(new Option(regionOption, "region", true,
      "Region to scan. Pass region name; e.g. '3d58e9067bf23e378e68c071f3dd39eb'"));
    files.addOption(new Option(fileOption, "file", true,
      "File to scan. Pass full-path; e.g. /root/hbase-3.0.0-alpha-4-SNAPSHOT/hbase-data/"
        + "data/default/tbl-sft/093fa06bf84b3b631007f951a14b8457/f/.filelist/f2.1655139542249"));
    options.addOptionGroup(files);
  }

  public boolean parseOptions(String[] args) throws ParseException, IOException {
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(cmdString, options, true);
      return false;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(fileOption)) {
      path = new Path(cmd.getOptionValue(fileOption));
    } else {
      regionName = cmd.getOptionValue(regionOption);
      if (StringUtils.isEmpty(regionName)) {
        err.println("Region name is not specified.");
        formatter.printHelp(cmdString, options, true);
        ExitHandler.getInstance().exit(1);
      }
      columnFamily = cmd.getOptionValue(columnFamilyOption);
      if (StringUtils.isEmpty(columnFamily)) {
        err.println("Column family is not specified.");
        formatter.printHelp(cmdString, options, true);
        ExitHandler.getInstance().exit(1);
      }
      String tableNameWtihNS = cmd.getOptionValue(tableNameOption);
      if (StringUtils.isEmpty(tableNameWtihNS)) {
        err.println("Table name is not specified.");
        formatter.printHelp(cmdString, options, true);
        ExitHandler.getInstance().exit(1);
      }
      TableName tn = TableName.valueOf(tableNameWtihNS);
      namespace = tn.getNamespaceAsString();
      tableName = tn.getNameAsString();
    }
    return true;
  }

  public int run(String[] args) {
    if (getConf() == null) {
      throw new RuntimeException("A Configuration instance must be provided.");
    }
    boolean pass = true;
    try {
      CommonFSUtils.setFsDefault(getConf(), CommonFSUtils.getRootDir(getConf()));
      if (!parseOptions(args)) {
        return 1;
      }
    } catch (IOException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    } catch (ParseException ex) {
      LOG.error("Error parsing command-line options", ex);
      return 1;
    }
    FileSystem fs = null;
    if (path != null) {
      try {
        fs = path.getFileSystem(getConf());
        if (fs.isDirectory(path)) {
          err.println("ERROR, wrong path given: " + path);
          return 2;
        }
        return print(fs, path);
      } catch (IOException e) {
        LOG.error("Error reading " + path, e);
        return 2;
      }
    } else {
      try {
        Path root = CommonFSUtils.getRootDir(getConf());
        Path baseDir = new Path(root, HConstants.BASE_NAMESPACE_DIR);
        Path nameSpacePath = new Path(baseDir, namespace);
        Path tablePath = new Path(nameSpacePath, tableName);
        Path regionPath = new Path(tablePath, regionName);
        Path cfPath = new Path(regionPath, columnFamily);
        Path sftPath = new Path(cfPath, StoreFileListFile.TRACK_FILE_DIR);

        fs = FileSystem.newInstance(regionPath.toUri(), getConf());

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(sftPath, false);

        while (iterator.hasNext()) {
          LocatedFileStatus lfs = iterator.next();
          if (
            lfs.isFile()
              && StoreFileListFile.TRACK_FILE_PATTERN.matcher(lfs.getPath().getName()).matches()
          ) {
            out.println("Printing contents for file " + lfs.getPath().toString());
            int ret = print(fs, lfs.getPath());
            if (ret != 0) {
              pass = false;
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Error processing " + e);
        return 2;
      }
    }
    return pass ? 0 : 2;
  }

  private int print(FileSystem fs, Path path) throws IOException {
    try {
      if (!fs.exists(path)) {
        err.println("ERROR, file doesnt exist: " + path);
        return 2;
      }
    } catch (IOException e) {
      err.println("ERROR, reading file: " + path + e);
      return 2;
    }
    StoreFileList storeFile = StoreFileListFile.load(fs, path);
    int end = storeFile.getStoreFileCount();
    for (int i = 0; i < end; i++) {
      out.println(storeFile.getStoreFile(i).getName());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new StoreFileListFilePrettyPrinter(), args);
    ExitHandler.getInstance().exit(ret);
  }
}

/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * The administration program for coprocessor.
 */
public class CoprocessorAdmin {
  private Configuration conf;

  public CoprocessorAdmin(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Checks whether classes defined by loadedClasses can be loaded.
   */
  void checkClasses(Path filesRoot, ArrayList<String> loadedClasses) {
    // TODO Check existing of the classes
  }

  /**
   * Uploads a version of coprocessor to DFS.
   *
   * @param localRoot The path to the root of the coprocessor code to be
   *          uploaded.
   * @param force If specified, remove existing content, if any, before
   *          uploading.
   */
  @SuppressWarnings("unchecked")
  public void upload(Path localRoot, boolean force) throws IOException {
    String root = CoprocessorHost.getCoprocessorDfsRoot(conf);

    System.out.println("Uploading coprocessor in " + localRoot + " to HDFS "
        + "rooted at " + root);

    String name = null;
    int version = -1;
    ArrayList<String> loadedClasses = null;

    Path pJson = new Path(localRoot, CoprocessorHost.CONFIG_JSON);
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    try (InputStream is = lfs.open(pJson)) {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> jConf =
          (Map<String, Object>) mapper.readValue(is, Map.class);

      name = (String) jConf.get(CoprocessorHost.COPROCESSOR_JSON_NAME_FIELD);
      version = (int) jConf.get(CoprocessorHost.COPROCESSOR_JSON_VERSION_FIELD);
      loadedClasses =
          (ArrayList<String>) jConf.get(CoprocessorHost.COPROCESSOR_JSON_LOADED_CLASSES_FIELD);

      System.out.println("Name: " + name);
      System.out.println("Version: " + version);
      System.out.println("Loaded classes: " + loadedClasses);

      checkClasses(localRoot, loadedClasses);
    }

    if (name == null || version < 0 || loadedClasses == null
        || loadedClasses.size() < 1) {
      System.err.println("Invalid configuration!");
      return;
    }

    FileSystem fs = FileSystem.get(conf);
    String cpRoot = CoprocessorHost.getCoprocessorDfsRoot(conf);
    Path pthRoot = new Path(cpRoot);
    if (!fs.exists(pthRoot)) {
      fs.mkdirs(pthRoot);
    }
    Path dstPath =
        new Path(CoprocessorHost.getCoprocessorPath(cpRoot, name, version));
    if (fs.exists(dstPath)) {
      if (!force) {
        FileStatus status = fs.getFileStatus(dstPath);
        if (!status.isDir()) {
          System.err.println("Path " + dstPath + " is an existing file.");
          System.err.println("Use -f option to force update.");
          return;
        }
        if (fs.listStatus(dstPath).length > 0) {
          System.err.println("Path " + dstPath
              + " already exists and is not empty.");
          System.err.println("Use -f option to force update.");
          return;
        }
      }
      fs.delete(dstPath, true);
    }

    System.out.println("Copying folder " + localRoot + " to " + dstPath);
    fs.copyFromLocalFile(false, true, true, localRoot, dstPath);
    System.out.println("All files copied!");
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "CoprocessorAdmin <upload|list|clean> [OPTIONS]\n"
            + "upload local-root-path\n" + "list", opt);
  }

  public static void main(String[] args) throws Exception {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.hdfs").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.io").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.fs").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.util").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.conf").setLevel(Level.ERROR);

    Options opt = new Options();
    opt.addOption("h", "help", false, "print usage");
    opt.addOption("p", "path", true, "Local path to the coprocessor files.");
    opt.addOption("f", "force", false, "Force to load.");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);
      if (cmd.hasOption("h")) {
        printHelp(opt);
        return;
      }

      args = cmd.getArgs();
      if (args.length < 1) {
        System.err.println("Please specify the action to perform.");
        printHelp(opt);
        return;
      }

      String action = args[0].toLowerCase();

      CoprocessorAdmin admin =
          new CoprocessorAdmin(HBaseConfiguration.create());

      switch (action) {
        case "upload": {
          if (args.length < 1) {
            System.err.println("Missing arguments for upload action");
            printHelp(opt);
            return;
          }

          admin.upload(new Path(args[1]), cmd.hasOption("f"));
          break;
        }
        default: {
          System.err.println("Unknown action: " + action);
          System.err.println("Run CoprocessorAdmin -h to see help info.");
        }
      }
    } catch (UnrecognizedOptionException | MissingArgumentException e) {
      System.err.println(e.getMessage());
      System.err.println("Run CoprocessorAdmin -h to see help info.");
    }
  }
}

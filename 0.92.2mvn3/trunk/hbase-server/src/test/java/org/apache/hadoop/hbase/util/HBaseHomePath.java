/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.net.URL;

import org.apache.hadoop.hbase.master.HMaster;

/** Determines HBase home path from either class or jar directory */
public class HBaseHomePath {

  private static final String TARGET_CLASSES = "/target/classes";
  private static final String JAR_SUFFIX = ".jar!";
  private static final String FILE_PREFIX = "file:";

  private HBaseHomePath() {
  }

  public static String getHomePath() {
    String className = HMaster.class.getName();  // This could have been any HBase class.
    String relPathForClass = className.replace(".", "/") + ".class";
    URL url = ClassLoader.getSystemResource(relPathForClass);
    relPathForClass = "/" + relPathForClass;
    if (url == null) {
      throw new RuntimeException("Could not lookup class location for " + className);
    }

    String path = url.getPath();
    if (!path.endsWith(relPathForClass)) {
      throw new RuntimeException("Got invalid path trying to look up class " + className +
          ": " + path);
    }
    path = path.substring(0, path.length() - relPathForClass.length());

    if (path.startsWith(FILE_PREFIX)) {
      path = path.substring(FILE_PREFIX.length());
    }

    if (path.endsWith(TARGET_CLASSES)) {
      path = path.substring(0, path.length() - TARGET_CLASSES.length());
    } else if (path.endsWith(JAR_SUFFIX)) {
      int slashIndex = path.lastIndexOf("/");
      if (slashIndex != -1) {
        throw new RuntimeException("Expected to find slash in jar path " + path);
      }
      path = path.substring(0, slashIndex);
    } else {
      throw new RuntimeException("Cannot identify HBase source directory or installation path " +
          "from " + path);
    }
    return path;
  }

}

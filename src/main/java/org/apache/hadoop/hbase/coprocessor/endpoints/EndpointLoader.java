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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.coprocessor.CoprocessorClassLoader;

/**
 * Static methods for loading endpoints.
 */
public class EndpointLoader {
  private static final Log LOG = LogFactory.getLog(EndpointLoader.class);
  /**
   * Key to configuration of the array endpoint factory classes.
   * Value is a list of entries. Each entry could be either a class name or a
   * pair of project-version in format of project@version.
   */
  public static final String FACTORY_CLASSES_KEY =
      "hbase.endpoint.factory.classes";

  private static final String[] FACTORY_CLASSES_DEFAULT = new String[] {};

  private static final String dfsTmpPrefix = "endpoint."
      + UUID.randomUUID().toString();

  private static final Pattern PAT_PROJECT_VERSION =
      Pattern.compile("[^@]+@[0-9]+");

  /**
   * Returns an entry string for a version of a project.
   */
  public static String genDfsEndpointEntry(String project, int version) {
    return project + "@" + version;
  }

  private static ClassLoader getDfsClassLoader(Configuration conf,
      FileSystem fs, Path projectPath) throws IOException {
    FileStatus[] jars = fs.listStatus(projectPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".jar");
      }
    });

    if (jars.length < 1) {
      throw new IOException("No jars in " + projectPath + " found!");
    }

    // TODO load class based on all jars.
    return CoprocessorClassLoader.getClassLoader(jars[0].getPath(),
        EndpointLoader.class.getClassLoader(), dfsTmpPrefix, conf);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static List<IEndpointFactory> loadProject(Configuration conf,
      FileSystem fs, String dfsRootPath, String project, int version)
      throws IOException {
    Path projectPath = new Path(CoprocessorHost.getCoprocessorPath(dfsRootPath,
        project, version));
    if (!fs.exists(projectPath)) {
      throw new IOException("Folder " + projectPath + " doesn't exist!");
    }
    if (!fs.getFileStatus(projectPath).isDir()) {
      throw new IOException(projectPath + " is not a folder!");
    }

    Path configPath = new Path(projectPath, CoprocessorHost.CONFIG_JSON);
    if (!fs.exists(configPath)) {
      throw new IOException("Cannot find config file: " + configPath);
    }

    Map<String, Object> jConf = FSUtils.readJSONFromFile(fs, configPath);
    String name = (String) jConf.get(
        CoprocessorHost.COPROCESSOR_JSON_NAME_FIELD);
    if (!name.equals(project)) {
      throw new IOException("Loaded name " + name + " is not expected"
          + project);
    }

    int loadedVer =
        (int) jConf.get(CoprocessorHost.COPROCESSOR_JSON_VERSION_FIELD);
    if (loadedVer != version) {
      throw new IOException("Loaded version " + loadedVer + " is not expected"
        + version);
    }

    ArrayList<String> loadedClasses = (ArrayList<String>) jConf.get(
        CoprocessorHost.COPROCESSOR_JSON_LOADED_CLASSES_FIELD);

    ClassLoader cl = getDfsClassLoader(conf, fs, projectPath);
    List<IEndpointFactory> res = new ArrayList<>(loadedClasses.size());
    for (int i = 0; i < loadedClasses.size(); i++) {
      try {
        Class<?> cls = cl.loadClass(loadedClasses.get(i));
        if (!IEndpointFactory.class.isAssignableFrom(cls)) {
          LOG.info("Class " + cls + " cannot be assigned as "
              + IEndpointFactory.class + ", ignored!");
          continue;
        }
        res.add((IEndpointFactory) cls.newInstance());
      } catch (Throwable t) {
        LOG.error("Load class " + loadedClasses.get(i) + " failed!", t);
      }
    }

    return res;
  }

  /**
   * Reload endpoints in an endpoint registry.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void reload(Configuration conf, IEndpointRegistry reg) {
    reg.unregisterAll();

    FileSystem fs = null;
    String dfsRoot = CoprocessorHost.getCoprocessorDfsRoot(conf);

    ClassLoader loader = EndpointLoader.class.getClassLoader();
    String[] classNames =
        conf.getStrings(FACTORY_CLASSES_KEY, FACTORY_CLASSES_DEFAULT);

    for (String className : classNames) {
      try {
        if (PAT_PROJECT_VERSION.matcher(className).matches()) {
          String[] projVer = className.split("@");
          String project = projVer[0];
          int version = Integer.parseInt(projVer[1]);

          if (fs == null) {
            fs = FileSystem.get(conf);
          }
          List<IEndpointFactory> factories = loadProject(conf, fs, dfsRoot,
              project, version);
          for (IEndpointFactory factory : factories) {
            LOG.info("Loading endpoint class " + factory.getClass().getName());
            reg.register(factory.getEndpointInterface(), factory);
          }
        } else {
          LOG.info("Loading endpoint class " + className);
          Class<IEndpointFactory> cFactory =
              (Class<IEndpointFactory>) loader.loadClass(className);

          IEndpointFactory factory = cFactory.newInstance();
          reg.register(factory.getEndpointInterface(), factory);
        }

      } catch (Throwable t) {
        LOG.info("Registering endpoint " + className + " failed", t);
      }
    }
  }
}

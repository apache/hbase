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
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Pair;

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

  @SuppressWarnings({ "rawtypes" })
  private static List<IEndpointFactory> loadEndpointProject(Configuration conf,
      FileSystem fs, String dfsRootPath, String project, int version)
      throws IOException {

    Pair<List<Class<?>>, ClassLoader> classesAndLoader =
        CoprocessorHost.getProjectClassesAndLoader(conf, fs, dfsRootPath, project, version,
        dfsTmpPrefix);

    List<IEndpointFactory> res = new ArrayList<>(
        classesAndLoader.getFirst().size());
    for (Class<?> cls: classesAndLoader.getFirst()) {
      if (!IEndpointFactory.class.isAssignableFrom(cls)) {
        LOG.info("Class " + cls + " cannot be assigned as "
            + IEndpointFactory.class + ", ignored!");
        continue;
      }
      try {
        res.add((IEndpointFactory) cls.newInstance());
      } catch (Throwable t) {
        LOG.info("Creating instance of " + cls + " failed!", t);
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
        if (CoprocessorHost.PAT_PROJECT_VERSION.matcher(className).matches()) {
          String[] projVer = className.split("@");
          String project = projVer[0];
          int version = Integer.parseInt(projVer[1]);

          if (fs == null) {
            fs = FileSystem.get(conf);
          }
          List<IEndpointFactory> factories = loadEndpointProject(conf, fs,
              dfsRoot, project, version);
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

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
package org.apache.hadoop.hbase.thrift;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The classloader allows the searching firstly in some path, if not found
 * search in parent.
 *
 * The class is useful for loading classes whose dependency is incompatible with
 * the current classloader.
 *
 * skipClasses are set to make those classes to be loaded with parent loader
 * and thus can be used by classes loaded by both classloaders. They are
 * commonly classes used in the interface. If the class doesn't exist in the
 * cutting path, it is not necessary to put in skipClasses.
 *
 * Class loaded with different classloader is not assignable to each other, use
 * reflection to call its methods.
 */
public class CuttingClassLoader extends URLClassLoader {
  private static Log LOG = LogFactory.getLog(CuttingClassLoader.class);
  private Set<String> skipClasses = new HashSet<>();

  public CuttingClassLoader(URL[] urls, ClassLoader parent,
      String... skipClasses) {
    super(urls, parent);
    Collections.addAll(this.skipClasses, skipClasses);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    if (skipClasses.contains(name)) {
      // Find in parent
      LOG.debug("Finding " + name + " in parent path!");
      return super.loadClass(name, resolve);
    }

    // Find in loaded classes
    Class<?> clazz = findLoadedClass(name);
    if (clazz != null) {
      LOG.debug("Found " + name + " in loaded class!");
      return clazz;
    }

    try {
      // Find in cutting paths
      LOG.debug("Finding " + name + " in cutting path!");
      return findClass(name);
    } catch (ClassNotFoundException e) {
    }

    // Find in parent
    LOG.debug("Finding " + name + " in parent path!");
    return super.loadClass(name, resolve);
  }
}

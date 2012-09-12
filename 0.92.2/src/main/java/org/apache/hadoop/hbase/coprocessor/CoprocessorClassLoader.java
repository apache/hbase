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
package org.apache.hadoop.hbase.coprocessor;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ClassLoader used to load Coprocessor instances.
 * 
 * This ClassLoader always tries to load classes from the Coprocessor jar first 
 * before delegating to the parent ClassLoader, thus avoiding dependency 
 * conflicts between HBase's classpath and classes in the coprocessor's jar.  
 * Certain classes are exempt from being loaded by this ClassLoader because it 
 * would prevent them from being cast to the equivalent classes in the region 
 * server.  For example, the Coprocessor interface needs to be loaded by the 
 * region server's ClassLoader to prevent a ClassCastException when casting the 
 * coprocessor implementation.
 * 
 * This ClassLoader also handles resource loading.  In most cases this 
 * ClassLoader will attempt to load resources from the coprocessor jar first 
 * before delegating to the parent.  However, like in class loading, 
 * some resources need to be handled differently.  For all of the Hadoop 
 * default configurations (e.g. hbase-default.xml) we will check the parent 
 * ClassLoader first to prevent issues such as failing the HBase default 
 * configuration version check.
 */
public class CoprocessorClassLoader extends URLClassLoader {
  private static final Log LOG = 
      LogFactory.getLog(CoprocessorClassLoader.class);
  
  /**
   * If the class being loaded starts with any of these strings, we will skip
   * trying to load it from the coprocessor jar and instead delegate 
   * directly to the parent ClassLoader.
   */
  private static final String[] CLASS_PREFIX_EXEMPTIONS = new String[] {
    // Java standard library:
    "com.sun.",
    "launcher.",
    "java.",
    "javax.",
    "org.ietf",
    "org.omg",
    "org.w3c",
    "org.xml",
    "sunw.",
    // Hadoop/HBase:
    "org.apache.hadoop",
  };
  
  /**
   * If the resource being loaded matches any of these patterns, we will first 
   * attempt to load the resource with the parent ClassLoader.  Only if the 
   * resource is not found by the parent do we attempt to load it from the 
   * coprocessor jar.
   */
  private static final Pattern[] RESOURCE_LOAD_PARENT_FIRST_PATTERNS = 
      new Pattern[] {
    Pattern.compile("^[^-]+-default\\.xml$")
  };
  
  /**
   * Creates a CoprocessorClassLoader that loads classes from the given paths.
   * @param paths paths from which to load classes.
   * @param parent the parent ClassLoader to set.
   */
  public CoprocessorClassLoader(List<URL> paths, ClassLoader parent) {
    super(paths.toArray(new URL[]{}), parent);
  }
  
  @Override
  synchronized public Class<?> loadClass(String name) 
      throws ClassNotFoundException {
    // Delegate to the parent immediately if this class is exempt
    if (isClassExempt(name)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping exempt class " + name + 
            " - delegating directly to parent");
      }
      return super.loadClass(name);
    }
    
    // Check whether the class has already been loaded:
    Class<?> clasz = findLoadedClass(name);
    if (clasz != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Class " + name + " already loaded");
      }
    }
    else {
      try {
        // Try to find this class using the URLs passed to this ClassLoader, 
        // which includes the coprocessor jar
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finding class: " + name);
        }
        clasz = findClass(name);
      } catch (ClassNotFoundException e) {
        // Class not found using this ClassLoader, so delegate to parent
        if (LOG.isDebugEnabled()) {
          LOG.debug("Class " + name + " not found - delegating to parent");
        }
        try {
          clasz = super.loadClass(name);
        } catch (ClassNotFoundException e2) {
          // Class not found in this ClassLoader or in the parent ClassLoader
          // Log some debug output before rethrowing ClassNotFoundException
          if (LOG.isDebugEnabled()) {
            LOG.debug("Class " + name + " not found in parent loader");
          }
          throw e2;
        }
      }
    }
    
    return clasz;
  }
  
  @Override
  synchronized public URL getResource(String name) {
    URL resource = null;
    boolean parentLoaded = false;
    
    // Delegate to the parent first if necessary
    if (loadResourceUsingParentFirst(name)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking parent first for resource " + name);
      }
      resource = super.getResource(name);
      parentLoaded = true;
    }
    
    if (resource == null) {
      // Try to find the resource in the coprocessor jar
      resource = findResource(name);
      if ((resource == null) && !parentLoaded) {
        // Not found in the coprocessor jar and we haven't attempted to load 
        // the resource in the parent yet; fall back to the parent
        resource = super.getResource(name);
      }
    }

    return resource;
  }
  
  /**
   * Determines whether the given class should be exempt from being loaded 
   * by this ClassLoader.
   * @param name the name of the class to test.
   * @return true if the class should *not* be loaded by this ClassLoader; 
   * false otherwise.
   */
  protected boolean isClassExempt(String name) {
    for (String exemptPrefix : CLASS_PREFIX_EXEMPTIONS) {
      if (name.startsWith(exemptPrefix)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Determines whether we should attempt to load the given resource using the  
   * parent first before attempting to load the resource using this ClassLoader.
   * @param name the name of the resource to test.
   * @return true if we should attempt to load the resource using the parent 
   * first; false if we should attempt to load the resource using this 
   * ClassLoader first.
   */
  protected boolean loadResourceUsingParentFirst(String name) {
    for (Pattern resourcePattern : RESOURCE_LOAD_PARENT_FIRST_PATTERNS) {
      if (resourcePattern.matcher(name).matches()) {
        return true;
      }
    }
    return false;
  }
}

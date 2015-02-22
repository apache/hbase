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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

/**
 * ClassLoader used to load classes for Coprocessor instances.
 * <p>
 * This ClassLoader always tries to load classes from the specified coprocessor
 * jar first actually using URLClassLoader logic before delegating to the parent
 * ClassLoader, thus avoiding dependency conflicts between HBase's classpath and
 * classes in the coprocessor jar.
 * <p>
 * Certain classes are exempt from being loaded by this ClassLoader because it
 * would prevent them from being cast to the equivalent classes in the region
 * server.  For example, the Coprocessor interface needs to be loaded by the
 * region server's ClassLoader to prevent a ClassCastException when casting the
 * coprocessor implementation.
 * <p>
 * A HDFS path can be used to specify the coprocessor jar. In this case, the jar
 * will be copied to local at first under some folder under ${hbase.local.dir}/jars/tmp/.
 * The local copy will be removed automatically when the HBase server instance is
 * stopped.
 * <p>
 * This ClassLoader also handles resource loading.  In most cases this
 * ClassLoader will attempt to load resources from the coprocessor jar first
 * before delegating to the parent.  However, like in class loading,
 * some resources need to be handled differently.  For all of the Hadoop
 * default configurations (e.g. hbase-default.xml) we will check the parent
 * ClassLoader first to prevent issues such as failing the HBase default
 * configuration version check.
 */
@InterfaceAudience.Private
public class CoprocessorClassLoader extends ClassLoaderBase {
  private static final Log LOG = LogFactory.getLog(CoprocessorClassLoader.class);

  // A temporary place ${hbase.local.dir}/jars/tmp/ to store the local
  // copy of the jar file and the libraries contained in the jar.
  private static final String TMP_JARS_DIR = File.separator
     + "jars" + File.separator + "tmp" + File.separator;

  /**
   * External class loaders cache keyed by external jar path.
   * ClassLoader instance is stored as a weak-reference
   * to allow GC'ing when it is not used
   * (@see HBASE-7205)
   */
  private static final ConcurrentMap<Path, CoprocessorClassLoader> classLoadersCache =
    new MapMaker().concurrencyLevel(3).weakValues().makeMap();

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
    // logging
    "org.apache.commons.logging",
    "org.apache.log4j",
    "com.hadoop",
    // Hadoop/HBase/ZK:
    "org.apache.hadoop",
    "org.apache.zookeeper",
  };

  /**
   * If the resource being loaded matches any of these patterns, we will first
   * attempt to load the resource with the parent ClassLoader.  Only if the
   * resource is not found by the parent do we attempt to load it from the coprocessor jar.
   */
  private static final Pattern[] RESOURCE_LOAD_PARENT_FIRST_PATTERNS =
      new Pattern[] {
    Pattern.compile("^[^-]+-default\\.xml$")
  };

  private static final Pattern libJarPattern = Pattern.compile("[/]?lib/([^/]+\\.jar)");

  /**
   * A locker used to synchronize class loader initialization per coprocessor jar file
   */
  private static final KeyLocker<String> locker = new KeyLocker<String>();

  /**
   * A set used to synchronized parent path clean up.  Generally, there
   * should be only one parent path, but using a set so that we can support more.
   */
  static final HashSet<String> parentDirLockSet = new HashSet<String>();

  /**
   * Creates a JarClassLoader that loads classes from the given paths.
   */
  private CoprocessorClassLoader(ClassLoader parent) {
    super(parent);
  }

  private void init(Path path, String pathPrefix,
      Configuration conf) throws IOException {
    // Copy the jar to the local filesystem
    String parentDirStr =
      conf.get(LOCAL_DIR_KEY, DEFAULT_LOCAL_DIR) + TMP_JARS_DIR;
    synchronized (parentDirLockSet) {
      if (!parentDirLockSet.contains(parentDirStr)) {
        Path parentDir = new Path(parentDirStr);
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(parentDir, true); // it's ok if the dir doesn't exist now
        parentDirLockSet.add(parentDirStr);
        if (!fs.mkdirs(parentDir) && !fs.getFileStatus(parentDir).isDirectory()) {
          throw new RuntimeException("Failed to create local dir " + parentDirStr
            + ", CoprocessorClassLoader failed to init");
        }
      }
    }

    FileSystem fs = path.getFileSystem(conf);
    File dst = new File(parentDirStr, "." + pathPrefix + "."
      + path.getName() + "." + System.currentTimeMillis() + ".jar");
    fs.copyToLocalFile(path, new Path(dst.toString()));
    dst.deleteOnExit();

    addURL(dst.getCanonicalFile().toURI().toURL());

    JarFile jarFile = new JarFile(dst.toString());
    try {
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        Matcher m = libJarPattern.matcher(entry.getName());
        if (m.matches()) {
          File file = new File(parentDirStr, "." + pathPrefix + "."
            + path.getName() + "." + System.currentTimeMillis() + "." + m.group(1));
          IOUtils.copyBytes(jarFile.getInputStream(entry),
            new FileOutputStream(file), conf, true);
          file.deleteOnExit();
          addURL(file.toURI().toURL());
        }
      }
    } finally {
      jarFile.close();
    }
  }

  // This method is used in unit test
  public static CoprocessorClassLoader getIfCached(final Path path) {
    Preconditions.checkNotNull(path, "The jar path is null!");
    return classLoadersCache.get(path);
  }

  // This method is used in unit test
  public static Collection<? extends ClassLoader> getAllCached() {
    return classLoadersCache.values();
  }

  // This method is used in unit test
  public static void clearCache() {
    classLoadersCache.clear();
  }

  /**
   * Get a CoprocessorClassLoader for a coprocessor jar path from cache.
   * If not in cache, create one.
   *
   * @param path the path to the coprocessor jar file to load classes from
   * @param parent the parent class loader for exempted classes
   * @param pathPrefix a prefix used in temp path name to store the jar file locally
   * @param conf the configuration used to create the class loader, if needed
   * @return a CoprocessorClassLoader for the coprocessor jar path
   * @throws IOException
   */
  public static CoprocessorClassLoader getClassLoader(final Path path,
      final ClassLoader parent, final String pathPrefix,
      final Configuration conf) throws IOException {
    CoprocessorClassLoader cl = getIfCached(path);
    String pathStr = path.toString();
    if (cl != null) {
      LOG.debug("Found classloader "+ cl + " for "+ pathStr);
      return cl;
    }

    if (!pathStr.endsWith(".jar")) {
      throw new IOException(pathStr + ": not a jar file?");
    }

    Lock lock = locker.acquireLock(pathStr);
    try {
      cl = getIfCached(path);
      if (cl != null) {
        LOG.debug("Found classloader "+ cl + " for "+ pathStr);
        return cl;
      }

      cl = AccessController.doPrivileged(
          new PrivilegedAction<CoprocessorClassLoader>() {
        @Override
        public CoprocessorClassLoader run() {
          return new CoprocessorClassLoader(parent);
        }
      });

      cl.init(path, pathPrefix, conf);

      // Cache class loader as a weak value, will be GC'ed when no reference left
      CoprocessorClassLoader prev = classLoadersCache.putIfAbsent(path, cl);
      if (prev != null) {
        // Lost update race, use already added class loader
        LOG.warn("THIS SHOULD NOT HAPPEN, a class loader"
          +" is already cached for " + pathStr);
        cl = prev;
      }
      return cl;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Class<?> loadClass(String name)
      throws ClassNotFoundException {
    // Delegate to the parent immediately if this class is exempt
    if (isClassExempt(name)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping exempt class " + name +
            " - delegating directly to parent");
      }
      return parent.loadClass(name);
    }

    synchronized (getClassLoadingLock(name)) {
      // Check whether the class has already been loaded:
      Class<?> clasz = findLoadedClass(name);
      if (clasz != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Class " + name + " already loaded");
        }
      }
      else {
        try {
          // Try to find this class using the URLs passed to this ClassLoader
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
            clasz = parent.loadClass(name);
          } catch (ClassNotFoundException e2) {
            // Class not found in this ClassLoader or in the parent ClassLoader
            // Log some debug output before re-throwing ClassNotFoundException
            if (LOG.isDebugEnabled()) {
              LOG.debug("Class " + name + " not found in parent loader");
            }
            throw e2;
          }
        }
      }
      return clasz;
    }
  }

  @Override
  public URL getResource(String name) {
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
      synchronized (getClassLoadingLock(name)) {
        // Try to find the resource in this jar
        resource = findResource(name);
        if ((resource == null) && !parentLoaded) {
          // Not found in this jar and we haven't attempted to load
          // the resource in the parent yet; fall back to the parent
          resource = super.getResource(name);
        }
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

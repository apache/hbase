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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a class loader that can load classes dynamically from new
 * jar files under a configured folder. The paths to the jar files are
 * converted to URLs, and URLClassLoader logic is actually used to load
 * classes. This class loader always uses its parent class loader
 * to load a class at first. Only if its parent class loader
 * can not load a class, we will try to load it using the logic here.
 * <p>
 * The configured folder can be a HDFS path. In this case, the jar files
 * under that folder will be copied to local at first under ${hbase.local.dir}/jars/.
 * The local copy will be updated if the remote copy is updated, according to its
 * last modified timestamp.
 * <p>
 * We can't unload a class already loaded. So we will use the existing
 * jar files we already know to load any class which can't be loaded
 * using the parent class loader. If we still can't load the class from
 * the existing jar files, we will check if any new jar file is added,
 * if so, we will load the new jar file and try to load the class again.
 * If still failed, a class not found exception will be thrown.
 * <p>
 * Be careful in uploading new jar files and make sure all classes
 * are consistent, otherwise, we may not be able to load your
 * classes properly.
 */
@InterfaceAudience.Private
public class DynamicClassLoader extends ClassLoaderBase {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicClassLoader.class);

  // Dynamic jars are put under ${hbase.local.dir}/jars/
  private static final String DYNAMIC_JARS_DIR = File.separator
    + "jars" + File.separator;

  private static final String DYNAMIC_JARS_DIR_KEY = "hbase.dynamic.jars.dir";

  private static final String DYNAMIC_JARS_OPTIONAL_CONF_KEY = "hbase.use.dynamic.jars";
  private static final boolean DYNAMIC_JARS_OPTIONAL_DEFAULT = true;

  // The user-provided value for using the DynamicClassLoader
  private final boolean userConfigUseDynamicJars;
  // The current state of whether to use the DynamicClassLoader
  private final boolean useDynamicJars;

  private File localDir;

  // FileSystem of the remote path, set only if remoteDir != null
  private FileSystem remoteDirFs;
  private Path remoteDir;

  // Last modified time of local jars
  private HashMap<String, Long> jarModifiedTime;

  /**
   * Creates a DynamicClassLoader that can load classes dynamically
   * from jar files under a specific folder.
   *
   * @param conf the configuration for the cluster.
   * @param parent the parent ClassLoader to set.
   */
  public DynamicClassLoader(final Configuration conf, final ClassLoader parent) {
    super(parent);

    // Save off the user's original configuration value for the DynamicClassLoader
    userConfigUseDynamicJars = conf.getBoolean(
        DYNAMIC_JARS_OPTIONAL_CONF_KEY, DYNAMIC_JARS_OPTIONAL_DEFAULT);

    boolean dynamicJarsEnabled = userConfigUseDynamicJars;
    if (dynamicJarsEnabled) {
      try {
        initTempDir(conf);
        dynamicJarsEnabled = true;
      } catch (Exception e) {
        LOG.error("Disabling the DynamicClassLoader as it failed to initialize its temp directory."
            + " Check your configuration and filesystem permissions. Custom coprocessor code may"
            + " not be loaded as a result of this failure.", e);
        dynamicJarsEnabled = false;
      }
    }
    useDynamicJars = dynamicJarsEnabled;
  }

  // FindBugs: Making synchronized to avoid IS2_INCONSISTENT_SYNC complaints about
  // remoteDirFs and jarModifiedTime being part synchronized protected.
  private synchronized void initTempDir(final Configuration conf) {
    jarModifiedTime = new HashMap<>();
    String localDirPath = conf.get(
      LOCAL_DIR_KEY, DEFAULT_LOCAL_DIR) + DYNAMIC_JARS_DIR;
    localDir = new File(localDirPath);
    if (!localDir.mkdirs() && !localDir.isDirectory()) {
      throw new RuntimeException("Failed to create local dir " + localDir.getPath()
        + ", DynamicClassLoader failed to init");
    }

    String remotePath = conf.get(DYNAMIC_JARS_DIR_KEY);
    if (remotePath == null || remotePath.equals(localDirPath)) {
      remoteDir = null;  // ignore if it is the same as the local path
    } else {
      remoteDir = new Path(remotePath);
      try {
        remoteDirFs = remoteDir.getFileSystem(conf);
      } catch (IOException ioe) {
        LOG.warn("Failed to identify the fs of dir "
          + remoteDir + ", ignored", ioe);
        remoteDir = null;
      }
    }
  }

  @Override
  public Class<?> loadClass(String name)
      throws ClassNotFoundException {
    try {
      return parent.loadClass(name);
    } catch (ClassNotFoundException e) {
      if (useDynamicJars) {
        LOG.debug("Class {} not found - using dynamical class loader", name);
        return tryRefreshClass(name);
      } else if (userConfigUseDynamicJars) {
        // If the user tried to enable the DCL, then warn again.
        LOG.debug("Not checking DynamicClassLoader for missing class because it is disabled."
            + " See the log for previous errors.");
      }
      throw e;
    }
  }

  private Class<?> tryRefreshClass(String name) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      // Check whether the class has already been loaded:
      Class<?> clasz = findLoadedClass(name);

      if (clasz != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Class {} already loaded", name);
        }
      } else {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Finding class: {}", name);
          }

          clasz = findClass(name);
        } catch (ClassNotFoundException cnfe) {
          // Load new jar files if any
          if (LOG.isDebugEnabled()) {
            LOG.debug("Loading new jar files, if any");
          }

          loadNewJars();

          if (LOG.isDebugEnabled()) {
            LOG.debug("Finding class again: {}", name);
          }

          clasz = findClass(name);
        }
      }

      return clasz;
    }
  }

  private synchronized void loadNewJars() {
    // Refresh local jar file lists
    File[] files = localDir == null ? null : localDir.listFiles();
    if (files != null) {
      for (File file : files) {
        String fileName = file.getName();
        if (jarModifiedTime.containsKey(fileName)) {
          continue;
        }
        if (file.isFile() && fileName.endsWith(".jar")) {
          jarModifiedTime.put(fileName, file.lastModified());
          try {
            URL url = file.toURI().toURL();
            addURL(url);
          } catch (MalformedURLException mue) {
            // This should not happen, just log it
            LOG.warn("Failed to load new jar " + fileName, mue);
          }
        }
      }
    }

    // Check remote files
    FileStatus[] statuses = null;
    if (remoteDir != null) {
      try {
        statuses = remoteDirFs.listStatus(remoteDir);
      } catch (IOException ioe) {
        LOG.warn("Failed to check remote dir status " + remoteDir, ioe);
      }
    }
    if (statuses == null || statuses.length == 0) {
      return; // no remote files at all
    }

    for (FileStatus status: statuses) {
      if (status.isDirectory()) {
        continue; // No recursive lookup
      }

      Path path = status.getPath();
      String fileName = path.getName();
      if (!fileName.endsWith(".jar")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignored non-jar file {}", fileName);
        }
        continue; // Ignore non-jar files
      }
      Long cachedLastModificationTime = jarModifiedTime.get(fileName);
      if (cachedLastModificationTime != null) {
        long lastModified = status.getModificationTime();
        if (lastModified < cachedLastModificationTime) {
          // There could be some race, for example, someone uploads
          // a new one right in the middle the old one is copied to
          // local. We can check the size as well. But it is still
          // not guaranteed. This should be rare. Most likely,
          // we already have the latest one.
          // If you are unlucky to hit this race issue, you have
          // to touch the remote jar to update its last modified time
          continue;
        }
      }
      try {
        // Copy it to local
        File dst = new File(localDir, fileName);
        remoteDirFs.copyToLocalFile(path, new Path(dst.getPath()));
        jarModifiedTime.put(fileName, dst.lastModified());
        URL url = dst.toURI().toURL();
        addURL(url);
      } catch (IOException ioe) {
        LOG.warn("Failed to load new jar " + fileName, ioe);
      }
    }
  }
}

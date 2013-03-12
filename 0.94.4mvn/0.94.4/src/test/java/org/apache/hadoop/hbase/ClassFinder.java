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

package org.apache.hadoop.hbase;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A class that finds a set of classes that are locally accessible
 * (from .class or .jar files), and satisfy the conditions that are
 * imposed by name and class filters provided by the user.
 */
public class ClassFinder {
  private static final Log LOG = LogFactory.getLog(ClassFinder.class);
  private static String CLASS_EXT = ".class";

  private FileNameFilter fileNameFilter;
  private ClassFilter classFilter;
  private FileFilter fileFilter;

  public static interface FileNameFilter {
    public boolean isCandidateFile(String fileName, String absFilePath);
  };

  public static interface ClassFilter {
    public boolean isCandidateClass(Class<?> c);
  };

  public ClassFinder(FileNameFilter fileNameFilter, ClassFilter classFilter) {
    this.classFilter = classFilter;
    this.fileNameFilter = fileNameFilter;
    this.fileFilter = new FileFilterWithName(fileNameFilter);
  }

  /**
   * Finds the classes in current package (of ClassFinder) and nested packages.
   * @param proceedOnExceptions whether to ignore exceptions encountered for
   *        individual jars/files/classes, and proceed looking for others.
   */
  public Set<Class<?>> findClasses(boolean proceedOnExceptions)
    throws ClassNotFoundException, IOException, LinkageError {
    return findClasses(this.getClass().getPackage().getName(), proceedOnExceptions);
  }

  /**
   * Finds the classes in a package and nested packages.
   * @param packageName package names
   * @param proceedOnExceptions whether to ignore exceptions encountered for
   *        individual jars/files/classes, and proceed looking for others.
   */
  public Set<Class<?>> findClasses(String packageName, boolean proceedOnExceptions)
    throws ClassNotFoundException, IOException, LinkageError {
    final String path = packageName.replace('.', '/');
    final Pattern jarResourceRe = Pattern.compile("^file:(.+\\.jar)!/" + path + "$");

    Enumeration<URL> resources = ClassLoader.getSystemClassLoader().getResources(path);
    List<File> dirs = new ArrayList<File>();
    List<String> jars = new ArrayList<String>();

    while (resources.hasMoreElements()) {
      URL resource = resources.nextElement();
      String resourcePath = resource.getFile();
      Matcher matcher = jarResourceRe.matcher(resourcePath);
      if (matcher.find()) {
        jars.add(matcher.group(1));
      } else {
        dirs.add(new File(resource.getFile()));
      }
    }

    Set<Class<?>> classes = new HashSet<Class<?>>();
    for (File directory : dirs) {
      classes.addAll(findClassesFromFiles(directory, packageName, proceedOnExceptions));
    }
    for (String jarFileName : jars) {
      classes.addAll(findClassesFromJar(jarFileName, packageName, proceedOnExceptions));
    }
    return classes;
  }

  private Set<Class<?>> findClassesFromJar(String jarFileName,
      String packageName, boolean proceedOnExceptions)
    throws IOException, ClassNotFoundException, LinkageError {
    JarInputStream jarFile = null;
    try {
      jarFile = new JarInputStream(new FileInputStream(jarFileName));
    } catch (IOException ioEx) {
      if (!proceedOnExceptions) {
        throw ioEx;
      }
      LOG.error("Failed to look for classes in " + jarFileName + ": " + ioEx);
    }

    Set<Class<?>> classes = new HashSet<Class<?>>();
    JarEntry entry = null;
    while (true) {
      try {
        entry = jarFile.getNextJarEntry();
      } catch (IOException ioEx) {
        if (!proceedOnExceptions) {
          throw ioEx;
        }
        LOG.error("Failed to get next entry from " + jarFileName + ": " + ioEx);
        break;
      }
      if (entry == null) {
        break; // loop termination condition
      }

      String className = entry.getName();
      if (!className.endsWith(CLASS_EXT)) {
        continue;
      }
      int ix = className.lastIndexOf('/');
      String fileName = (ix >= 0) ? className.substring(ix + 1) : className;
      if (!this.fileNameFilter.isCandidateFile(fileName, className)) {
        continue;
      }
      className = className
          .substring(0, className.length() - CLASS_EXT.length()).replace('/', '.');
      if (!className.startsWith(packageName)) {
        continue;
      }
      Class<?> c = makeClass(className, proceedOnExceptions);
      if (c != null) {
        if (!classes.add(c)) {
          LOG.error("Ignoring duplicate class " + className);
        }
      }
    }
    return classes;
  }

  private Set<Class<?>> findClassesFromFiles(File baseDirectory, String packageName,
      boolean proceedOnExceptions) throws ClassNotFoundException, LinkageError {
    Set<Class<?>> classes = new HashSet<Class<?>>();
    if (!baseDirectory.exists()) {
      LOG.error("Failed to find " + baseDirectory.getAbsolutePath());
      return classes;
    }

    File[] files = baseDirectory.listFiles(this.fileFilter);
    if (files == null) {
      LOG.error("Failed to get files from " + baseDirectory.getAbsolutePath());
      return classes;
    }

    for (File file : files) {
      final String fileName = file.getName();
      if (file.isDirectory()) {
        classes.addAll(findClassesFromFiles(file, packageName + "." + fileName,
            proceedOnExceptions));
      } else {
        String className = packageName + '.'
            + fileName.substring(0, fileName.length() - CLASS_EXT.length());
        Class<?> c = makeClass(className, proceedOnExceptions);
        if (c != null) {
          if (!classes.add(c)) {
            LOG.error("Ignoring duplicate class " + className);
          }
        }
      }
    }
    return classes;
  }

  private Class<?> makeClass(String className, boolean proceedOnExceptions)
    throws ClassNotFoundException, LinkageError {
    try {
      Class<?> c = Class.forName(className, false, this.getClass().getClassLoader());
      return classFilter.isCandidateClass(c) ? c : null;
    } catch (ClassNotFoundException classNotFoundEx) {
      if (!proceedOnExceptions) {
        throw classNotFoundEx;
      }
      LOG.error("Failed to instantiate or check " + className + ": " + classNotFoundEx);
    } catch (LinkageError linkageEx) {
      if (!proceedOnExceptions) {
        throw linkageEx;
      }
      LOG.error("Failed to instantiate or check " + className + ": " + linkageEx);
    }
    return null;
  }

  private class FileFilterWithName implements FileFilter {
    private FileNameFilter nameFilter;

    public FileFilterWithName(FileNameFilter nameFilter) {
      this.nameFilter = nameFilter;
    }

    @Override
    public boolean accept(File file) {
      return file.isDirectory()
          || (file.getName().endsWith(CLASS_EXT)
              && nameFilter.isCandidateFile(file.getName(), file.getAbsolutePath()));
    }
  };
};

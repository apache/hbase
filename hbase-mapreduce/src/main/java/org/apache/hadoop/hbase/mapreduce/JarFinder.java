/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Finds the Jar for a class. If the class is in a directory in the
 * classpath, it creates a Jar on the fly with the contents of the directory
 * and returns the path to that Jar. If a Jar is created, it is created in
 * the system temporary directory.
 *
 * This file was forked from hadoop/common/branches/branch-2@1377176.
 */
@InterfaceAudience.Private
public final class JarFinder {

  private static void copyToZipStream(File file, ZipEntry entry,
                              ZipOutputStream zos) throws IOException {
    InputStream is = new FileInputStream(file);
    try {
      zos.putNextEntry(entry);
      byte[] arr = new byte[4096];
      int read = is.read(arr);
      while (read > -1) {
        zos.write(arr, 0, read);
        read = is.read(arr);
      }
    } finally {
      try {
        is.close();
      } finally {
        zos.closeEntry();
      }
    }
  }

  public static void jarDir(File dir, String relativePath, ZipOutputStream zos)
    throws IOException {
    Preconditions.checkNotNull(relativePath, "relativePath");
    Preconditions.checkNotNull(zos, "zos");

    // by JAR spec, if there is a manifest, it must be the first entry in the
    // ZIP.
    File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
    ZipEntry manifestEntry = new ZipEntry(JarFile.MANIFEST_NAME);
    if (!manifestFile.exists()) {
      zos.putNextEntry(manifestEntry);
      new Manifest().write(new BufferedOutputStream(zos));
      zos.closeEntry();
    } else {
      copyToZipStream(manifestFile, manifestEntry, zos);
    }
    zos.closeEntry();
    zipDir(dir, relativePath, zos, true);
    zos.close();
  }

  private static void zipDir(File dir, String relativePath, ZipOutputStream zos,
                             boolean start) throws IOException {
    String[] dirList = dir.list();
    if (dirList == null) {
      return;
    }
    for (String aDirList : dirList) {
      File f = new File(dir, aDirList);
      if (!f.isHidden()) {
        if (f.isDirectory()) {
          if (!start) {
            ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
            zos.putNextEntry(dirEntry);
            zos.closeEntry();
          }
          String filePath = f.getPath();
          File file = new File(filePath);
          zipDir(file, relativePath + f.getName() + "/", zos, false);
        }
        else {
          String path = relativePath + f.getName();
          if (!path.equals(JarFile.MANIFEST_NAME)) {
            ZipEntry anEntry = new ZipEntry(path);
            copyToZipStream(f, anEntry, zos);
          }
        }
      }
    }
  }

  private static void createJar(File dir, File jarFile) throws IOException {
    Preconditions.checkNotNull(dir, "dir");
    Preconditions.checkNotNull(jarFile, "jarFile");
    File jarDir = jarFile.getParentFile();
    if (!jarDir.exists()) {
      if (!jarDir.mkdirs()) {
        throw new IOException(MessageFormat.format("could not create dir [{0}]",
                                                   jarDir));
      }
    }
    try (FileOutputStream fos = new FileOutputStream(jarFile);
         JarOutputStream jos = new JarOutputStream(fos)) {
      jarDir(dir, "", jos);
    }
  }

  /**
   * Returns the full path to the Jar containing the class. It always return a
   * JAR.
   *
   * @param klass class.
   *
   * @return path to the Jar containing the class.
   */
  public static String getJar(Class klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";
      try {
        for (Enumeration itr = loader.getResources(class_file);
             itr.hasMoreElements(); ) {
          URL url = (URL) itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }
          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          }
          else if ("file".equals(url.getProtocol())) {
            String klassName = klass.getName();
            klassName = klassName.replace(".", "/") + ".class";
            path = path.substring(0, path.length() - klassName.length());
            File baseDir = new File(path);
            File testDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
            testDir = testDir.getAbsoluteFile();
            if (!testDir.exists()) {
              testDir.mkdirs();
            }
            File tempJar = File.createTempFile("hadoop-", "", testDir);
            tempJar = new File(tempJar.getAbsolutePath() + ".jar");
            tempJar.deleteOnExit();
            createJar(baseDir, tempJar);
            return tempJar.getAbsolutePath();
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private JarFinder() {}
}

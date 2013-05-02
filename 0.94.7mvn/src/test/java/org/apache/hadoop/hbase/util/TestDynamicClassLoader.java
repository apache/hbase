/*
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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test TestDynamicClassLoader
 */
@Category(SmallTests.class)
public class TestDynamicClassLoader {
  private static final Log LOG = LogFactory.getLog(TestDynamicClassLoader.class);

  private static final Configuration conf = HBaseConfiguration.create();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static {
    conf.set("hbase.dynamic.jars.dir", TEST_UTIL.getDataTestDir().toString());
  }

  // generate jar file
  private boolean createJarArchive(File archiveFile, File[] tobeJared) {
    try {
      byte buffer[] = new byte[4096];
      // Open archive file
      FileOutputStream stream = new FileOutputStream(archiveFile);
      JarOutputStream out = new JarOutputStream(stream, new Manifest());

      for (int i = 0; i < tobeJared.length; i++) {
        if (tobeJared[i] == null || !tobeJared[i].exists()
            || tobeJared[i].isDirectory()) {
          continue;
        }

        // Add archive entry
        JarEntry jarAdd = new JarEntry(tobeJared[i].getName());
        jarAdd.setTime(tobeJared[i].lastModified());
        out.putNextEntry(jarAdd);

        // Write file to archive
        FileInputStream in = new FileInputStream(tobeJared[i]);
        while (true) {
          int nRead = in.read(buffer, 0, buffer.length);
          if (nRead <= 0)
            break;
          out.write(buffer, 0, nRead);
        }
        in.close();
      }
      out.close();
      stream.close();
      LOG.info("Adding classes to jar file completed");
      return true;
    } catch (Exception ex) {
      LOG.error("Error: " + ex.getMessage());
      return false;
    }
  }

  private File buildJar(
      String className, String folder) throws Exception {
    String javaCode = "public class " + className + " {}";
    Path srcDir = new Path(TEST_UTIL.getDataTestDir(), "src");
    File srcDirPath = new File(srcDir.toString());
    srcDirPath.mkdirs();
    File sourceCodeFile = new File(srcDir.toString(), className + ".java");
    BufferedWriter bw = new BufferedWriter(new FileWriter(sourceCodeFile));
    bw.write(javaCode);
    bw.close();

    // compile it by JavaCompiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    ArrayList<String> srcFileNames = new ArrayList<String>();
    srcFileNames.add(sourceCodeFile.toString());
    StandardJavaFileManager fm = compiler.getStandardFileManager(null, null,
      null);
    Iterable<? extends JavaFileObject> cu =
      fm.getJavaFileObjects(sourceCodeFile);
    List<String> options = new ArrayList<String>();
    options.add("-classpath");
    // only add hbase classes to classpath. This is a little bit tricky: assume
    // the classpath is {hbaseSrc}/target/classes.
    String currentDir = new File(".").getAbsolutePath();
    String classpath =
        currentDir + File.separator + "target"+ File.separator + "classes" +
        System.getProperty("path.separator") + System.getProperty("java.class.path");
    options.add(classpath);
    LOG.debug("Setting classpath to: "+classpath);

    JavaCompiler.CompilationTask task = compiler.getTask(null, fm, null,
      options, null, cu);
    assertTrue("Compile file " + sourceCodeFile + " failed.", task.call());

    // build a jar file by the classes files
    String jarFileName = className + ".jar";
    File jarFile = new File(folder, jarFileName);
    if (!createJarArchive(jarFile,
        new File[]{new File(srcDir.toString(), className + ".class")})){
      assertTrue("Build jar file failed.", false);
    }
    return jarFile;
  }

  @Test
  public void testLoadClassFromLocalPath() throws Exception {
    ClassLoader parent = TestDynamicClassLoader.class.getClassLoader();
    DynamicClassLoader classLoader = new DynamicClassLoader(conf, parent);

    String className = "TestLoadClassFromLocalPath";
    try {
      classLoader.loadClass(className);
      fail("Should not be able to load class " + className);
    } catch (ClassNotFoundException cnfe) {
      // expected, move on
    }

    try {
      buildJar(className, localDirPath());
      classLoader.loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Should be able to load class " + className, cnfe);
      fail(cnfe.getMessage());
    } finally {
      deleteClass(className);
    }
  }

  @Test
  public void testLoadClassFromAnotherPath() throws Exception {
    ClassLoader parent = TestDynamicClassLoader.class.getClassLoader();
    DynamicClassLoader classLoader = new DynamicClassLoader(conf, parent);

    String className = "TestLoadClassFromAnotherPath";
    try {
      classLoader.loadClass(className);
      fail("Should not be able to load class " + className);
    } catch (ClassNotFoundException cnfe) {
      // expected, move on
    }

    try {
      buildJar(className, TEST_UTIL.getDataTestDir().toString());
      classLoader.loadClass(className);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Should be able to load class " + className, cnfe);
      fail(cnfe.getMessage());
    } finally {
      deleteClass(className);
    }
  }

  private String localDirPath() {
    return conf.get("hbase.local.dir") + File.separator
      + "dynamic" + File.separator + "jars" + File.separator;
  }

  private void deleteClass(String className) throws Exception {
    String jarFileName = className + ".jar";
    File file = new File(TEST_UTIL.getDataTestDir().toString(), jarFileName);
    file.deleteOnExit();

    file = new File(conf.get("hbase.dynamic.jars.dir"), jarFileName);
    file.deleteOnExit();

    file = new File(localDirPath(), jarFileName);
    file.deleteOnExit();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some utilities to help class loader testing
 */
public final class ClassLoaderTestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderTestHelper.class);

  private static final int BUFFER_SIZE = 4096;

  private ClassLoaderTestHelper() {
  }

  /**
   * Jar a list of files into a jar archive.
   *
   * @param archiveFile the target jar archive
   * @param tobeJared a list of files to be jared
   * @return true if a jar archive is build, false otherwise
   */
  private static boolean createJarArchive(File archiveFile, File[] tobeJared) {
    try {
      byte[] buffer = new byte[BUFFER_SIZE];
      // Open archive file
      FileOutputStream stream = new FileOutputStream(archiveFile);
      JarOutputStream out = new JarOutputStream(stream, new Manifest());

      for (File file : tobeJared) {
        if (file == null || !file.exists() || file.isDirectory()) {
          continue;
        }

        // Add archive entry
        JarEntry jarAdd = new JarEntry(file.getName());
        jarAdd.setTime(file.lastModified());
        out.putNextEntry(jarAdd);

        // Write file to archive
        FileInputStream in = new FileInputStream(file);
        while (true) {
          int nRead = in.read(buffer, 0, buffer.length);
          if (nRead <= 0) {
            break;
          }

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

  /**
   * Create a test jar for testing purpose for a given class
   * name with specified code string: save the class to a file,
   * compile it, and jar it up. If the code string passed in is
   * null, a bare empty class will be created and used.
   *
   * @param testDir the folder under which to store the test class and jar
   * @param className the test class name
   * @param code the optional test class code, which can be null.
   *    If null, a bare empty class will be used
   * @return the test jar file generated
   */
  public static File buildJar(String testDir,
      String className, String code) throws Exception {
    return buildJar(testDir, className, code, testDir);
  }

  /**
   * Create a test jar for testing purpose for a given class
   * name with specified code string.
   *
   * @param testDir the folder under which to store the test class
   * @param className the test class name
   * @param code the optional test class code, which can be null.
   *    If null, an empty class will be used
   * @param folder the folder under which to store the generated jar
   * @return the test jar file generated
   */
  public static File buildJar(String testDir,
      String className, String code, String folder) throws Exception {
    String javaCode = code != null ? code : "public class " + className + " {}";
    Path srcDir = new Path(testDir, "src");
    File srcDirPath = new File(srcDir.toString());
    srcDirPath.mkdirs();
    File sourceCodeFile = new File(srcDir.toString(), className + ".java");
    BufferedWriter bw = Files.newBufferedWriter(sourceCodeFile.toPath(), StandardCharsets.UTF_8);
    bw.write(javaCode);
    bw.close();

    // compile it by JavaCompiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    ArrayList<String> srcFileNames = new ArrayList<>(1);
    srcFileNames.add(sourceCodeFile.toString());
    StandardJavaFileManager fm = compiler.getStandardFileManager(null, null,
      null);
    Iterable<? extends JavaFileObject> cu =
      fm.getJavaFileObjects(sourceCodeFile);
    List<String> options = new ArrayList<>(2);
    options.add("-classpath");
    // only add hbase classes to classpath. This is a little bit tricky: assume
    // the classpath is {hbaseSrc}/target/classes.
    String currentDir = new File(".").getAbsolutePath();
    String classpath = currentDir + File.separator + "target"+ File.separator
      + "classes" + System.getProperty("path.separator")
      + System.getProperty("java.class.path") + System.getProperty("path.separator")
      + System.getProperty("surefire.test.class.path");

    options.add(classpath);
    LOG.debug("Setting classpath to: " + classpath);

    JavaCompiler.CompilationTask task = compiler.getTask(null, fm, null,
      options, null, cu);
    assertTrue("Compile file " + sourceCodeFile + " failed.", task.call());

    // build a jar file by the classes files
    String jarFileName = className + ".jar";
    File jarFile = new File(folder, jarFileName);
    jarFile.getParentFile().mkdirs();
    if (!createJarArchive(jarFile,
        new File[]{new File(srcDir.toString(), className + ".class")})){
      fail("Build jar file failed.");
    }
    return jarFile;
  }

  /**
   * Add a list of jar files to another jar file under a specific folder.
   * It is used to generated coprocessor jar files which can be loaded by
   * the coprocessor class loader.  It is for testing usage only so we
   * don't be so careful about stream closing in case any exception.
   *
   * @param targetJar the target jar file
   * @param libPrefix the folder where to put inner jar files
   * @param srcJars the source inner jar files to be added
   * @throws Exception if anything doesn't work as expected
   */
  public static void addJarFilesToJar(File targetJar,
      String libPrefix, File... srcJars) throws Exception {
    FileOutputStream stream = new FileOutputStream(targetJar);
    JarOutputStream out = new JarOutputStream(stream, new Manifest());
    byte[] buffer = new byte[BUFFER_SIZE];

    for (File jarFile: srcJars) {
      // Add archive entry
      JarEntry jarAdd = new JarEntry(libPrefix + jarFile.getName());
      jarAdd.setTime(jarFile.lastModified());
      out.putNextEntry(jarAdd);

      // Write file to archive
      FileInputStream in = new FileInputStream(jarFile);
      while (true) {
        int nRead = in.read(buffer, 0, buffer.length);
        if (nRead <= 0) {
          break;
        }

        out.write(buffer, 0, nRead);
      }
      in.close();
    }
    out.close();
    stream.close();
    LOG.info("Adding jar file to outer jar file completed");
  }

  public static String localDirPath(Configuration conf) {
    return conf.get(ClassLoaderBase.LOCAL_DIR_KEY)
      + File.separator + "jars" + File.separator;
  }
}

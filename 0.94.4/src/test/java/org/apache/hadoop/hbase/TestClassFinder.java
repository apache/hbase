/**
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.*;
import javax.tools.*;

import org.apache.hadoop.hbase.SmallTests;

import org.junit.experimental.categories.Category;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

@Category(SmallTests.class)
public class TestClassFinder {
  private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private static final String BASEPKG = "tfcpkg";
  // Use unique jar/class/package names in each test case with the help
  // of these global counters; we are mucking with ClassLoader in this test
  // and we don't want individual test cases to conflict via it.
  private static AtomicLong testCounter = new AtomicLong(0);
  private static AtomicLong jarCounter = new AtomicLong(0);

  private static String basePath = null;

  // Default name/class filters for testing.
  private static final ClassFinder.FileNameFilter trueNameFilter =
      new ClassFinder.FileNameFilter() {
    @Override
    public boolean isCandidateFile(String fileName, String absFilePath) {
      return true;
    }
  };
  private static final ClassFinder.ClassFilter trueClassFilter =
      new ClassFinder.ClassFilter() {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      return true;
    }
  };

  @BeforeClass
  public static void createTestDir() throws IOException {
    basePath = testUtil.getDataTestDir(TestClassFinder.class.getSimpleName()).toString();
    if (!basePath.endsWith("/")) {
      basePath += "/";
    }
    // Make sure we get a brand new directory.
    File testDir = new File(basePath);
    if (testDir.exists()) {
      deleteTestDir();
    }
    assertTrue(testDir.mkdirs());
  }

  @AfterClass
  public static void deleteTestDir() throws IOException {
    testUtil.cleanupTestDir(TestClassFinder.class.getSimpleName());
  }

  @Test
  public void testClassFinderCanFindClassesInJars() throws Exception {
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, ".nested", "c2");
    FileAndPath c3 = compileTestClass(counter, "", "c3");
    packageAndLoadJar(c1, c3);
    packageAndLoadJar(c2);

    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(3, allClasses.size());
  }

  @Test
  public void testClassFinderHandlesConflicts() throws Exception {
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, "", "c2");
    packageAndLoadJar(c1, c2);
    packageAndLoadJar(c1);

    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(2, allClasses.size());
  }

  @Test
  public void testClassFinderHandlesNestedPackages() throws Exception {
    final String NESTED = ".nested";
    final String CLASSNAME1 = "c2";
    final String CLASSNAME2 = "c3";
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, NESTED, CLASSNAME1);
    FileAndPath c3 = compileTestClass(counter, NESTED, CLASSNAME2);
    packageAndLoadJar(c1, c2);
    packageAndLoadJar(c3);

    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> nestedClasses = allClassesFinder.findClasses(
        makePackageName(NESTED, counter), false);
    assertEquals(2, nestedClasses.size());
    Class<?> nestedClass1 = makeClass(NESTED, CLASSNAME1, counter);
    assertTrue(nestedClasses.contains(nestedClass1));
    Class<?> nestedClass2 = makeClass(NESTED, CLASSNAME2, counter);
    assertTrue(nestedClasses.contains(nestedClass2));
  }

  @Test
  public void testClassFinderFiltersByNameInJar() throws Exception {
    final String CLASSNAME = "c1";
    final String CLASSNAMEEXCPREFIX = "c2";
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", CLASSNAME);
    FileAndPath c2 = compileTestClass(counter, "", CLASSNAMEEXCPREFIX + "1");
    FileAndPath c3 = compileTestClass(counter, "", CLASSNAMEEXCPREFIX + "2");
    packageAndLoadJar(c1, c2, c3);

    ClassFinder.FileNameFilter notExcNameFilter = new ClassFinder.FileNameFilter() {
      @Override
      public boolean isCandidateFile(String fileName, String absFilePath) {
        return !fileName.startsWith(CLASSNAMEEXCPREFIX);
      }
    };
    ClassFinder incClassesFinder = new ClassFinder(notExcNameFilter, trueClassFilter);
    Set<Class<?>> incClasses = incClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(1, incClasses.size());
    Class<?> incClass = makeClass("", CLASSNAME, counter);
    assertTrue(incClasses.contains(incClass));
  }

  @Test
  public void testClassFinderFiltersByClassInJar() throws Exception {
    final String CLASSNAME = "c1";
    final String CLASSNAMEEXCPREFIX = "c2";
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", CLASSNAME);
    FileAndPath c2 = compileTestClass(counter, "", CLASSNAMEEXCPREFIX + "1");
    FileAndPath c3 = compileTestClass(counter, "", CLASSNAMEEXCPREFIX + "2");
    packageAndLoadJar(c1, c2, c3);

    final ClassFinder.ClassFilter notExcClassFilter = new ClassFinder.ClassFilter() {
      @Override
      public boolean isCandidateClass(Class<?> c) {
        return !c.getSimpleName().startsWith(CLASSNAMEEXCPREFIX);
      }
    };
    ClassFinder incClassesFinder = new ClassFinder(trueNameFilter, notExcClassFilter);
    Set<Class<?>> incClasses = incClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(1, incClasses.size());
    Class<?> incClass = makeClass("", CLASSNAME, counter);
    assertTrue(incClasses.contains(incClass));
  }

  @Test
  public void testClassFinderCanFindClassesInDirs() throws Exception {
    // Well, technically, we are not guaranteed that the classes will
    // be in dirs, but during normal build they would be.
    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(
        this.getClass().getPackage().getName(), false);
    assertTrue(allClasses.contains(this.getClass()));
    assertTrue(allClasses.contains(ClassFinder.class));
  }

  @Test
  public void testClassFinderFiltersByNameInDirs() throws Exception {
    final String thisName = this.getClass().getSimpleName();
    ClassFinder.FileNameFilter notThisFilter = new ClassFinder.FileNameFilter() {
      @Override
      public boolean isCandidateFile(String fileName, String absFilePath) {
        return !fileName.equals(thisName + ".class");
      }
    };
    String thisPackage = this.getClass().getPackage().getName();
    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(thisPackage, false);
    ClassFinder notThisClassFinder = new ClassFinder(notThisFilter, trueClassFilter);
    Set<Class<?>> notAllClasses = notThisClassFinder.findClasses(thisPackage, false);
    assertFalse(notAllClasses.contains(this.getClass()));
    assertEquals(allClasses.size() - 1, notAllClasses.size());
  }

  @Test
  public void testClassFinderFiltersByClassInDirs() throws Exception {
    ClassFinder.ClassFilter notThisFilter = new ClassFinder.ClassFilter() {
      @Override
      public boolean isCandidateClass(Class<?> c) {
        return c != TestClassFinder.class;
      }
    };
    String thisPackage = this.getClass().getPackage().getName();
    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(thisPackage, false);
    ClassFinder notThisClassFinder = new ClassFinder(trueNameFilter, notThisFilter);
    Set<Class<?>> notAllClasses = notThisClassFinder.findClasses(thisPackage, false);
    assertFalse(notAllClasses.contains(this.getClass()));
    assertEquals(allClasses.size() - 1, notAllClasses.size());
  }

  @Test
  public void testClassFinderDefaultsToOwnPackage() throws Exception {
    // Correct handling of nested packages is tested elsewhere, so here we just assume
    // pkgClasses is the correct answer that we don't have to check.
    ClassFinder allClassesFinder = new ClassFinder(trueNameFilter, trueClassFilter);
    Set<Class<?>> pkgClasses = allClassesFinder.findClasses(
        ClassFinder.class.getPackage().getName(), false);
    Set<Class<?>> defaultClasses = allClassesFinder.findClasses(false);
    assertArrayEquals(pkgClasses.toArray(), defaultClasses.toArray());
  }

  private static class FileAndPath {
    String path;
    File file;
    public FileAndPath(String path, File file) {
      this.file = file;
      this.path = path;
    }
  }

  private static Class<?> makeClass(String nestedPkgSuffix,
      String className, long counter) throws ClassNotFoundException {
    return Class.forName(
        makePackageName(nestedPkgSuffix, counter) + "." + className + counter);
  }

  private static String makePackageName(String nestedSuffix, long counter) {
    return BASEPKG + counter + nestedSuffix;
  }

  /**
   * Compiles the test class with bogus code into a .class file.
   * Unfortunately it's very tedious.
   * @param counter Unique test counter.
   * @param packageNameSuffix Package name suffix (e.g. ".suffix") for nesting, or "".
   * @return The resulting .class file and the location in jar it is supposed to go to.
   */
  private static FileAndPath compileTestClass(long counter,
      String packageNameSuffix, String classNamePrefix) throws Exception {
    classNamePrefix = classNamePrefix + counter;
    String packageName = makePackageName(packageNameSuffix, counter);
    String javaPath = basePath + classNamePrefix + ".java";
    String classPath = basePath + classNamePrefix + ".class";
    PrintStream source = new PrintStream(javaPath);
    source.println("package " + packageName + ";");
    source.println("public class " + classNamePrefix
        + " { public static void main(String[] args) { } };");
    source.close();
    JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
    int result = jc.run(null, null, null, javaPath);
    assertEquals(0, result);
    File classFile = new File(classPath);
    assertTrue(classFile.exists());
    return new FileAndPath(packageName.replace('.', '/') + '/', classFile);
  }

  /**
   * Makes a jar out of some class files. Unfortunately it's very tedious.
   * @param filesInJar Files created via compileTestClass.
   */
  private static void packageAndLoadJar(FileAndPath... filesInJar) throws Exception {
    // First, write the bogus jar file.
    String path = basePath + "jar" + jarCounter.incrementAndGet() + ".jar";
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    FileOutputStream fos = new FileOutputStream(path);
    JarOutputStream jarOutputStream = new JarOutputStream(fos, manifest);
    // Directory entries for all packages have to be added explicitly for
    // resources to be findable via ClassLoader. Directory entries must end
    // with "/"; the initial one is expected to, also.
    Set<String> pathsInJar = new HashSet<String>();
    for (FileAndPath fileAndPath : filesInJar) {
      String pathToAdd = fileAndPath.path;
      while (pathsInJar.add(pathToAdd)) {
        int ix = pathToAdd.lastIndexOf('/', pathToAdd.length() - 2);
        if (ix < 0) {
          break;
        }
        pathToAdd = pathToAdd.substring(0, ix);
      }
    }
    for (String pathInJar : pathsInJar) {
      jarOutputStream.putNextEntry(new JarEntry(pathInJar));
      jarOutputStream.closeEntry();
    }
    for (FileAndPath fileAndPath : filesInJar) {
      File file = fileAndPath.file;
      jarOutputStream.putNextEntry(
          new JarEntry(fileAndPath.path + file.getName()));
      byte[] allBytes = new byte[(int)file.length()];
      FileInputStream fis = new FileInputStream(file);
      fis.read(allBytes);
      fis.close();
      jarOutputStream.write(allBytes);
      jarOutputStream.closeEntry();
    }
    jarOutputStream.close();
    fos.close();

    // Add the file to classpath.
    File jarFile = new File(path);
    assertTrue(jarFile.exists());
    URLClassLoader urlClassLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class
        .getDeclaredMethod("addURL", new Class[] { URL.class });
    method.setAccessible(true);
    method.invoke(urlClassLoader, new Object[] { jarFile.toURI().toURL() });
  }
};

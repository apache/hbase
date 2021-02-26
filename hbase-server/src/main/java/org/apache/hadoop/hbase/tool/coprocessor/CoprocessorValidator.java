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

package org.apache.hadoop.hbase.tool.coprocessor;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.tool.PreUpgradeValidator;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorViolation.Severity;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class CoprocessorValidator extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory
      .getLogger(CoprocessorValidator.class);

  private CoprocessorMethods branch1;
  private CoprocessorMethods current;

  private final List<String> jars;
  private final List<Pattern> tablePatterns;
  private final List<String> classes;
  private boolean config;

  private boolean dieOnWarnings;

  public CoprocessorValidator() {
    branch1 = new Branch1CoprocessorMethods();
    current = new CurrentCoprocessorMethods();

    jars = new ArrayList<>();
    tablePatterns = new ArrayList<>();
    classes = new ArrayList<>();
  }

  /**
   * This classloader implementation calls {@link #resolveClass(Class)}
   * method for every loaded class. It means that some extra validation will
   * take place <a
   * href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-12.html#jls-12.3">
   * according to JLS</a>.
   */
  private static final class ResolverUrlClassLoader extends URLClassLoader {
    private ResolverUrlClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      return loadClass(name, true);
    }
  }

  private ResolverUrlClassLoader createClassLoader(URL[] urls) {
    return createClassLoader(urls, getClass().getClassLoader());
  }

  private ResolverUrlClassLoader createClassLoader(URL[] urls, ClassLoader parent) {
    return AccessController.doPrivileged(new PrivilegedAction<ResolverUrlClassLoader>() {
      @Override
      public ResolverUrlClassLoader run() {
        return new ResolverUrlClassLoader(urls, parent);
      }
    });
  }

  private ResolverUrlClassLoader createClassLoader(ClassLoader parent,
      org.apache.hadoop.fs.Path path) throws IOException {
    Path tempPath = Files.createTempFile("hbase-coprocessor-", ".jar");
    org.apache.hadoop.fs.Path destination = new org.apache.hadoop.fs.Path(tempPath.toString());

    LOG.debug("Copying coprocessor jar '{}' to '{}'.", path, tempPath);

    FileSystem fileSystem = FileSystem.get(getConf());
    fileSystem.copyToLocalFile(path, destination);

    URL url = tempPath.toUri().toURL();

    return createClassLoader(new URL[] { url }, parent);
  }

  private void validate(ClassLoader classLoader, String className,
      List<CoprocessorViolation> violations) {
    LOG.debug("Validating class '{}'.", className);

    try {
      Class<?> clazz = classLoader.loadClass(className);

      for (Method method : clazz.getDeclaredMethods()) {
        LOG.trace("Validating method '{}'.", method);

        if (branch1.hasMethod(method) && !current.hasMethod(method)) {
          CoprocessorViolation violation = new CoprocessorViolation(
              className, Severity.WARNING, "method '" + method +
              "' was removed from new coprocessor API, so it won't be called by HBase");
          violations.add(violation);
        }
      }
    } catch (ClassNotFoundException e) {
      CoprocessorViolation violation = new CoprocessorViolation(
          className, Severity.ERROR, "no such class", e);
      violations.add(violation);
    } catch (RuntimeException | Error e) {
      CoprocessorViolation violation = new CoprocessorViolation(
          className, Severity.ERROR, "could not validate class", e);
      violations.add(violation);
    }
  }

  public void validateClasses(ClassLoader classLoader, List<String> classNames,
      List<CoprocessorViolation> violations) {
    for (String className : classNames) {
      validate(classLoader, className, violations);
    }
  }

  public void validateClasses(ClassLoader classLoader, String[] classNames,
      List<CoprocessorViolation> violations) {
    validateClasses(classLoader, Arrays.asList(classNames), violations);
  }

  @InterfaceAudience.Private
  protected void validateTables(ClassLoader classLoader, Admin admin,
      Pattern pattern, List<CoprocessorViolation> violations) throws IOException {
    List<TableDescriptor> tableDescriptors = admin.listTableDescriptors(pattern);

    for (TableDescriptor tableDescriptor : tableDescriptors) {
      LOG.debug("Validating table {}", tableDescriptor.getTableName());

      Collection<CoprocessorDescriptor> coprocessorDescriptors =
          tableDescriptor.getCoprocessorDescriptors();

      for (CoprocessorDescriptor coprocessorDescriptor : coprocessorDescriptors) {
        String className = coprocessorDescriptor.getClassName();
        Optional<String> jarPath = coprocessorDescriptor.getJarPath();

        if (jarPath.isPresent()) {
          org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(jarPath.get());
          try (ResolverUrlClassLoader cpClassLoader = createClassLoader(classLoader, path)) {
            validate(cpClassLoader, className, violations);
          } catch (IOException e) {
            CoprocessorViolation violation = new CoprocessorViolation(
                className, Severity.ERROR,
                "could not validate jar file '" + path + "'", e);
            violations.add(violation);
          }
        } else {
          validate(classLoader, className, violations);
        }
      }
    }
  }

  private void validateTables(ClassLoader classLoader, Pattern pattern,
      List<CoprocessorViolation> violations) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(getConf());
        Admin admin = connection.getAdmin()) {
      validateTables(classLoader, admin, pattern, violations);
    }
  }

  @Override
  protected void printUsage() {
    String header = "hbase " + PreUpgradeValidator.TOOL_NAME + " " +
        PreUpgradeValidator.VALIDATE_CP_NAME +
        " [-jar ...] [-class ... | -table ... | -config]";
    printUsage(header, "Options:", "");
  }

  @Override
  protected void addOptions() {
    addOptNoArg("e", "Treat warnings as errors.");
    addOptWithArg("jar", "Jar file/directory of the coprocessor.");
    addOptWithArg("table", "Table coprocessor(s) to check.");
    addOptWithArg("class", "Coprocessor class(es) to check.");
    addOptNoArg("config", "Obtain coprocessor class(es) from configuration.");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    String[] jars = cmd.getOptionValues("jar");
    if (jars != null) {
      Collections.addAll(this.jars, jars);
    }

    String[] tables = cmd.getOptionValues("table");
    if (tables != null) {
      Arrays.stream(tables).map(Pattern::compile).forEach(tablePatterns::add);
    }

    String[] classes = cmd.getOptionValues("class");
    if (classes != null) {
      Collections.addAll(this.classes, classes);
    }

    config = cmd.hasOption("config");
    dieOnWarnings = cmd.hasOption("e");
  }

  private List<URL> buildClasspath(List<String> jars) throws IOException {
    List<URL> urls = new ArrayList<>();

    for (String jar : jars) {
      Path jarPath = Paths.get(jar);
      if (Files.isDirectory(jarPath)) {
        try (Stream<Path> stream = Files.list(jarPath)) {
          List<Path> files = stream
              .filter((path) -> Files.isRegularFile(path))
              .collect(Collectors.toList());

          for (Path file : files) {
            URL url = file.toUri().toURL();
            urls.add(url);
          }
        }
      } else {
        URL url = jarPath.toUri().toURL();
        urls.add(url);
      }
    }

    return urls;
  }

  @Override
  protected int doWork() throws Exception {
    if (tablePatterns.isEmpty() && classes.isEmpty() && !config) {
      LOG.error("Please give at least one -table, -class or -config parameter.");
      printUsage();
      return EXIT_FAILURE;
    }

    List<URL> urlList = buildClasspath(jars);
    URL[] urls = urlList.toArray(new URL[urlList.size()]);

    LOG.debug("Classpath: {}", urlList);

    List<CoprocessorViolation> violations = new ArrayList<>();

    try (ResolverUrlClassLoader classLoader = createClassLoader(urls)) {
      for (Pattern tablePattern : tablePatterns) {
        validateTables(classLoader, tablePattern, violations);
      }

      validateClasses(classLoader, classes, violations);

      if (config) {
        String[] masterCoprocessors =
            getConf().getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
        if (masterCoprocessors != null) {
          validateClasses(classLoader, masterCoprocessors, violations);
        }

        String[] regionCoprocessors =
            getConf().getStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
        if (regionCoprocessors != null) {
          validateClasses(classLoader, regionCoprocessors, violations);
        }
      }
    }

    boolean error = false;

    for (CoprocessorViolation violation : violations) {
      String className = violation.getClassName();
      String message = violation.getMessage();
      Throwable throwable = violation.getThrowable();

      switch (violation.getSeverity()) {
        case WARNING:
          if (throwable == null) {
            LOG.warn("Warning in class '{}': {}.", className, message);
          } else {
            LOG.warn("Warning in class '{}': {}.", className, message, throwable);
          }

          if (dieOnWarnings) {
            error = true;
          }

          break;
        case ERROR:
          if (throwable == null) {
            LOG.error("Error in class '{}': {}.", className, message);
          } else {
            LOG.error("Error in class '{}': {}.", className, message, throwable);
          }

          error = true;

          break;
      }
    }

    return (error) ? EXIT_FAILURE : EXIT_SUCCESS;
  }
}

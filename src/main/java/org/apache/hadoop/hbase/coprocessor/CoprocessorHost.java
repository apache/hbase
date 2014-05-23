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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.ipc.ThriftClientInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.coprocessor.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.coprocessor.SortedCopyOnWriteSet;

/**
 * Provides the common setup framework and runtime services for coprocessor
 * invocation from HBase services.
 * @param <E> the specific environment extension that a concrete implementation
 * provides
 */
public abstract class CoprocessorHost<E extends CoprocessorEnvironment> {
  public static final String REGION_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.region.classes";
  public static final String REGIONSERVER_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.regionserver.classes";
  public static final String USER_REGION_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.user.region.classes";
  public static final String MASTER_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.master.classes";
  public static final String REGION_COPROCESSOR_REMOVE_CONF_KEY =
      "hbase.coprocessor.remove.classes";
  public static final String WAL_COPROCESSOR_CONF_KEY =
    "hbase.coprocessor.wal.classes";
  public static final String ABORT_ON_ERROR_KEY = "hbase.coprocessor.abortonerror";
  public static final boolean DEFAULT_ABORT_ON_ERROR = true;
  public static final String USER_REGION_COPROCESSOR_FROM_HDFS_KEY = "hbase.coprocessor.jars.and.classes";

  /**
   * Key in the configuration to the root of coprocessors on DFS
   */
  public static final String COPROCESSOR_DFS_ROOT_KEY =
      "hbase.coprocessor.dfs.root";
  /**
   * Default values of the root of coprocessors on DFS.
   */
  public static final String COPROCESSOR_DFS_ROOT_DEF = "/coprocessors";

  private static final Log LOG = LogFactory.getLog(CoprocessorHost.class);
  protected ThriftClientInterface tcInter;
  /** Ordered set of currently loaded coprocessors with lock */
  protected SortedSet<E> coprocessors =
      new SortedCopyOnWriteSet<E>(new EnvironmentPriorityComparator());
  protected Configuration conf;
  // unique file prefix to use for local copies of jars when classloading
  protected String pathPrefix;
  protected AtomicInteger loadSequence = new AtomicInteger();
  /**
   * The field name for "loaded classes"
   */
  public static final String COPROCESSOR_JSON_LOADED_CLASSES_FIELD =
      "loaded_classes";
  /**
   * The field name for "version"
   */
  public static final String COPROCESSOR_JSON_VERSION_FIELD = "version";
  /**
   * The field name for "name"
   */
  public static final String COPROCESSOR_JSON_NAME_FIELD = "name";
  /**
   * The file name of the configuration json file within the coprocessor folder.
   */
  public static final String CONFIG_JSON = "config.json";

  public CoprocessorHost() {
    this.pathPrefix = UUID.randomUUID().toString();
  }

  /**
   * Not to be confused with the per-object _coprocessors_ (above),
   * coprocessorNames is static and stores the set of all coprocessors ever
   * loaded by any thread in this JVM. It is strictly additive: coprocessors are
   * added to coprocessorNames, by loadInstance() but are never removed, since
   * the intention is to preserve a history of all loaded coprocessors for
   * diagnosis in case of server crash (HBASE-4014).
   */
  private static Set<String> everLoadedCoprocessorNames =
      Collections.synchronizedSet(new HashSet<String>());
  public static Set<String> getEverLoadedCoprocessors() {
      return everLoadedCoprocessorNames;
  }

  /**
   * Used to create a parameter to the HServerLoad constructor so that
   * HServerLoad can provide information about the coprocessors loaded by this
   * regionserver.
   * (HBASE-4070: Improve region server metrics to report loaded coprocessors
   * to master).
   */
  public Set<String> getCoprocessors() {
    Set<String> returnValue = new TreeSet<String>();
    for(CoprocessorEnvironment e: coprocessors) {
      returnValue.add(e.getInstance().getClass().getSimpleName());
    }
    return returnValue;
  }

  /**
   * Load system coprocessors. Read the class names from configuration.
   * Called by constructor.
   */
  protected void loadSystemCoprocessors(Configuration conf, String confKey) {
    // load default coprocessors from configure file
    String[] defaultCPClasses = conf.getStrings(confKey);
    if (defaultCPClasses == null || defaultCPClasses.length == 0) {
      LOG.error("We could not load coprocessors since there are no coprocessors specified in the configuration");
      return;
    }

    int priority = Coprocessor.PRIORITY_SYSTEM;
    List<E> configured = new ArrayList<E>();
    for (String className : defaultCPClasses) {
      className = className.trim();
      // TODO: check if we want to reenable this behavior later!
      // if (findCoprocessor(className) != null) {
      // System.out.println("coprocessor found with name: " + className);
      // continue;
      // }
      ClassLoader cl = this.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
      try {
        Class<?> implClass = cl.loadClass(className);
        configured.add(loadInstance(implClass, Coprocessor.PRIORITY_SYSTEM, conf, confKey));
        LOG.info("System coprocessor " + className + " was loaded " +
            "successfully with priority (" + priority++ + ").");
      } catch (Throwable t) {
        LOG.error("Coprocessor " + className + "could not be loaded", t);
      }
    }
    coprocessors.addAll(configured);
  }

  /**
   * Generally used when we do online configuration change for the loaded
   * coprocessors
   *
   * @param conf
   * @param confKey
   */
  protected void reloadSysCoprocessorsOnConfigChange(Configuration conf,
      String confKey) {
    // remove whatever is loaded already with that conf key
    removeLoadedCoprocWithKey(confKey);
    loadSystemCoprocessors(conf, confKey);
  }

  /**
   * Will remove loaded coprocessors with specified key
   * @param confKey
   */
  private void removeLoadedCoprocWithKey(String confKey) {
    for (Iterator<E> iterator = coprocessors.iterator(); iterator.hasNext();) {
      E currentCoproc = iterator.next();
      if (currentCoproc.getConfKeyForLoading().equals(confKey)) {
        iterator.remove();
      }
    }
  }

  /**
   * Read coprocessors that should be loaded from configuration. In the
   * configuration we should have string of tuples of [table, jar, class] for
   * each usage of coprocesssor
   *
   * @param conf
   */
  public Map<String, List<Pair<String, String>>> readCoprocessorsFromConf(
      Configuration conf) {
    Map<String, List<Pair<String, String>>> coprocConfigMap = new HashMap<>();
    String fromConfig = conf.get(USER_REGION_COPROCESSOR_FROM_HDFS_KEY);
    if (fromConfig == null || fromConfig.isEmpty()) {
      return coprocConfigMap;
    }
    String[] tuples = fromConfig.split(";");
    for (String tuple : tuples) {
      tuple = tuple.trim();
      String[] str = tuple.split(",");
      // str[0] is table, str[1] is path for the jar, str[2] is name of the
      // class
      if (str.length != 3) {
        LOG.error("Configuration is not in expected format: " + str);
        return null;
      }
      String table = str[0].trim();
      List<Pair<String, String>> inMap = coprocConfigMap.get(table);
      if (inMap == null) {
        inMap = new ArrayList<>();
        coprocConfigMap.put(table, inMap);
      }
      inMap.add(new Pair<>(str[1].trim(), str[2].trim()));
    }
    return coprocConfigMap;
  }

  /**
   * Checks if the specified path for the coprocessor jar is in expected format
   * TODO: make this more advanced - currently is very hardoced and dummy
   *
   * @param path
   *          - Absolute and complete path where the coprocessor jar resides
   *          Expected path is everything in this format:
   *          coprocessors/project/version(integer)/jarfile
   * @return
   */
  public static boolean checkIfCorrectPath(String path) {
    String[] firstSplit = path.split(":");
    String[] parts = firstSplit[2].split("/");
    // port is included here
    if (parts.length != 5) {
      return false;
    }
    if (!parts[1].equals("coprocessors")) {
      return false;
    }
    return true;
  }

  /**
   * Used to load coprocessors whose jar is specified via hdfs path. All
   * existing coprocessors will be unloaded (if they appear again in the new
   * configuration then they will be re-added).
   *
   * @param config
   * @throws IOException
   */
  protected void reloadCoprocessorsFromHdfs(String table,
      List<Pair<String, String>> toLoad, Configuration config)
      throws IOException {
    // remove whatever is loaded first
    removeLoadedCoprocWithKey(USER_REGION_COPROCESSOR_FROM_HDFS_KEY);
    List<E> newCoprocessors = new ArrayList<>();

    LOG.info("Loading coprocessor for table: " + table);
    for (Pair<String, String> pair : toLoad) {
      LOG.info("Jar: " + pair.getFirst() + " class: " + pair.getSecond());
      E coproc = load(new Path(pair.getFirst()), pair.getSecond(),
          Coprocessor.PRIORITY_USER, config,
          USER_REGION_COPROCESSOR_FROM_HDFS_KEY);
      newCoprocessors.add(coproc);
    }
    if (newCoprocessors.isEmpty()) {
      LOG.info("NO region observers were added from hdfs!");
    } else {
      LOG.info(newCoprocessors.size()
          + " region observers were added from hdfs");
    }
    this.coprocessors.addAll(newCoprocessors);
  }

  /**
   * Load a coprocessor implementation into the host
   * @param path path to implementation jar
   * @param className the main class name
   * @param priority chaining priority
   * @param conf configuration for coprocessor
   * @throws java.io.IOException Exception
   */
  public E load(Path path, String className, int priority,
      Configuration conf, String confKey) throws IOException {
    Class<?> implClass = null;
    LOG.debug("Loading coprocessor class " + className + " with path " +
        path + " and priority " + priority);

    ClassLoader cl = null;
    if (path == null) {
      try {
        implClass = getClass().getClassLoader().loadClass(className);
      } catch (ClassNotFoundException e) {
        throw new IOException("No jar path specified for " + className);
      }
    } else {
      cl = CoprocessorClassLoader.getClassLoader(
        path, getClass().getClassLoader(), pathPrefix, conf);
      try {
        implClass = cl.loadClass(className);
      } catch (ClassNotFoundException e) {
        throw new IOException("Cannot load external coprocessor class " + className, e);
      }
    }
    //load custom code for coprocessor
    try (ContextResetter ctxResetter = new ContextResetter(cl)) {
//       switch temporarily to the thread classloader for custom CP
      E cpInstance = loadInstance(implClass, priority, conf, confKey);
      return cpInstance;
    } catch (Exception e) {
      String msg = new StringBuilder()
          .append("Cannot load external coprocessor class ").append(className)
          .toString();
      throw new IOException(msg, e);
    }
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public void load(Class<?> implClass, int priority, Configuration conf, String confKey)
      throws IOException {
    E env = loadInstance(implClass, priority, conf, confKey);
    coprocessors.add(env);
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public E loadInstance(Class<?> implClass, int priority, Configuration conf, String keyFromConf)
      throws IOException {
    if (!Coprocessor.class.isAssignableFrom(implClass)) {
      throw new IOException("Configured class " + implClass.getName() + " must implement "
          + Coprocessor.class.getName() + " interface ");
    }
    // create the instance
    Coprocessor impl = null;
    try {
      impl = (Coprocessor)implClass.newInstance();
    } catch (InstantiationException  | IllegalAccessException e) {
      throw new IOException(e);
    }
    // create the environment
    E env = createEnvironment(implClass, impl, priority, loadSequence.incrementAndGet(), conf, keyFromConf);
    if (env instanceof Environment) {
      ((Environment)env).startup();
    }
    // HBASE-4014: maintain list of loaded coprocessors for later crash analysis
    // if server (master or regionserver) aborts.
    everLoadedCoprocessorNames.add(implClass.getName());
    return env;
  }

  /**
   * Called when a new Coprocessor class is loaded
   */
  public abstract E createEnvironment(Class<?> implClass, Coprocessor instance,
      int priority, int sequence, Configuration conf, String keyForLoading);

  public void shutdown(CoprocessorEnvironment e) {
    if (e instanceof Environment) {
      ((Environment)e).shutdown();
    } else {
      LOG.warn("Shutdown called on unknown environment: "+
          e.getClass().getName());
    }
  }

  /**
   * Find a coprocessor implementation by class name
   * @param className the class name
   * @return the coprocessor, or null if not found
   */
  public Coprocessor findCoprocessor(String className) {
    for (E env: coprocessors) {
      if (env.getInstance().getClass().getName().equals(className) ||
          env.getInstance().getClass().getSimpleName().equals(className)) {
        return env.getInstance();
      }
    }
    return null;
  }

  /**
   * Find a coprocessor environment by class name
   * @param className the class name
   * @return the coprocessor, or null if not found
   */
  public CoprocessorEnvironment findCoprocessorEnvironment(String className) {
    for (E env: coprocessors) {
      if (env.getInstance().getClass().getName().equals(className) ||
          env.getInstance().getClass().getSimpleName().equals(className)) {
        return env;
      }
    }
    return null;
  }

  /**
   * Retrieves the set of classloaders used to instantiate Coprocessor classes defined in external
   * jar files.
   * @return A set of ClassLoader instances
   */
  Set<ClassLoader> getExternalClassLoaders() {
    Set<ClassLoader> externalClassLoaders = new HashSet<ClassLoader>();
    final ClassLoader systemClassLoader = this.getClass().getClassLoader();
    for (E env : coprocessors) {
      ClassLoader cl = env.getInstance().getClass().getClassLoader();
      if (cl != systemClassLoader ){
        //do not include system classloader
        externalClassLoaders.add(cl);
      }
    }
    return externalClassLoaders;
  }

  /**
   * Environment priority comparator.
   * Coprocessors are chained in sorted order.
   */
  static class EnvironmentPriorityComparator
      implements Comparator<CoprocessorEnvironment> {
    @Override
    public int compare(final CoprocessorEnvironment env1,
        final CoprocessorEnvironment env2) {
      if (env1.getPriority() < env2.getPriority()) {
        return -1;
      } else if (env1.getPriority() > env2.getPriority()) {
        return 1;
      }
      if (env1.getLoadSequence() < env2.getLoadSequence()) {
        return -1;
      } else if (env1.getLoadSequence() > env2.getLoadSequence()) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Encapsulation of the environment of each coprocessor
   */
  public static class Environment implements CoprocessorEnvironment {

    /** The coprocessor */
    public Coprocessor impl;
    /** Chaining priority */
    protected int priority = Coprocessor.PRIORITY_USER;
    /** Current coprocessor state */
    Coprocessor.State state = Coprocessor.State.UNINSTALLED;
    /** Accounting for tables opened by the coprocessor */
    protected List<HTableInterface> openTables =
      Collections.synchronizedList(new ArrayList<HTableInterface>());
    private int seq;
    private Configuration conf;
    private String keyForLoading;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public Environment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf, String keyForLoading) {
      this.impl = impl;
      this.priority = priority;
      this.state = Coprocessor.State.INSTALLED;
      this.seq = seq;
      this.conf = conf;
      this.keyForLoading = keyForLoading;
    }

    /** Initialize the environment */
    public void startup() throws IOException {
      if (state == Coprocessor.State.INSTALLED
          || state == Coprocessor.State.STOPPED) {
        state = Coprocessor.State.STARTING;
        try (ContextResetter ctxResetter = new ContextResetter(
            this.getClassLoader())) {
          impl.start();
          state = Coprocessor.State.ACTIVE;
        } catch (Exception e) {
          LOG.error("Setting class loader failed", e);
        }
      } else {
        LOG.warn("Not starting coprocessor " + impl.getClass().getName()
            + " because not inactive (state=" + state.toString() + ")");
      }
    }

    /** Clean up the environment */
    protected void shutdown() {
      if (state == Coprocessor.State.ACTIVE) {
        state = Coprocessor.State.STOPPING;
        try (ContextResetter ctxResetter = new ContextResetter(
            this.getClassLoader())) {
          impl.stop();
          state = Coprocessor.State.STOPPED;
        } catch (Exception e) {
          LOG.error("Error stopping coprocessor " + impl.getClass().getName(),
              e);
        }
      } else {
        LOG.warn("Not stopping coprocessor " + impl.getClass().getName()
            + " because not active (state=" + state.toString() + ")");
      }
      // clean up any table references
      for (HTableInterface table : openTables) {
        try {
          table.close();
          ;
        } catch (IOException e) {
          // nothing can be done here
          LOG.warn(
              "Failed to close " + Bytes.toStringBinary(table.getTableName()),
              e);
        }
      }
    }

    @Override
    public Coprocessor getInstance() {
      return impl;
    }

    @Override
    public ClassLoader getClassLoader() {
      return impl.getClass().getClassLoader();
    }

    @Override
    public int getPriority() {
      return priority;
    }

    @Override
    public int getLoadSequence() {
      return seq;
    }

    /** @return the coprocessor environment version */
    @Override
    public int getVersion() {
      return Coprocessor.VERSION;
    }

    /** @return the HBase release */
    @Override
    public String getHBaseVersion() {
      return VersionInfo.getVersion();
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public String getConfKeyForLoading() {
      return keyForLoading;
    }

  }


  /**
   * This is used by coprocessor hooks which are declared to throw IOException
   * (or its subtypes). For such hooks, we should handle throwable objects
   * depending on the Throwable's type. Those which are instances of
   * IOException should be passed on to the client. This is in conformance with
   * the HBase idiom regarding IOException: that it represents a circumstance
   * that should be passed along to the client for its own handling. For
   * example, a coprocessor that implements access controls would throw a
   * subclass of IOException, such as AccessDeniedException, in its preGet()
   * method to prevent an unauthorized client's performing a Get on a particular
   * table.
   * @param env Coprocessor Environment
   * @param e Throwable object thrown by coprocessor.
   * @exception IOException Exception
   */
  protected void handleCoprocessorThrowable(final CoprocessorEnvironment env, final Throwable e)
      throws IOException {
    if (e instanceof IOException) {
      throw (IOException)e;
    }
    // If we got here, e is not an IOException. A loaded coprocessor has a
    // fatal bug, and the server (master or regionserver) should remove the
    // faulty coprocessor from its set of active coprocessors. Setting
    // 'hbase.coprocessor.abortonerror' to true will cause abortServer(),
    // which may be useful in development and testing environments where
    // 'failing fast' for error analysis is desired.
    if (env.getConfiguration().getBoolean(ABORT_ON_ERROR_KEY, DEFAULT_ABORT_ON_ERROR)) {
      // server is configured to abort.
     // TODO: see if we want to abort regionserver here
    } else {
      LOG.error("Removing coprocessor '" + env.toString() + "' from " +
          "environment because it threw:  " + e,e);
      coprocessors.remove(env);
      try {
        shutdown(env);
      } catch (Exception x) {
        LOG.error("Uncaught exception when shutting down coprocessor '"
            + env.toString() + "'", x);
      }
      throw new DoNotRetryIOException("Coprocessor: '" + env.toString() +
          "' threw: '" + e + "' and has been removed from the active " +
          "coprocessor set.", e);
    }
  }

  /**
   * Used just to set the contextClassLoader on the current thread in case of
   * exception - code in {@link #close()} will be executed
   *
   */
  static class ContextResetter implements AutoCloseable {
    final ClassLoader currentLoader;

    public ContextResetter(ClassLoader cl) {
      this.currentLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
    }

    @Override
    public void close() throws Exception {
      Thread.currentThread().setContextClassLoader(currentLoader);
    }
  }

  /**
   * Returns the root of coprocessors on DFS.
   */
  public static String getCoprocessorDfsRoot(Configuration conf) {
    return conf.get(COPROCESSOR_DFS_ROOT_KEY, COPROCESSOR_DFS_ROOT_DEF);
  }

  /**
   * Returns the path to a version of a coprocessor.
   *
   * @param root the root to all coprocessors on DFS
   * @param name the name of the coprocessor.
   * @param version the version
   */
  public static String getCoprocessorPath(String root, String name, int version) {
    return root + Path.SEPARATOR + name + Path.SEPARATOR + version;
  }
}

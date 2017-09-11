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
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableWrapper;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.SortedList;
import org.apache.hadoop.hbase.util.VersionInfo;

/**
 * Provides the common setup framework and runtime services for coprocessor
 * invocation from HBase services.
 * @param <E> the specific environment extension that a concrete implementation
 * provides
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public abstract class CoprocessorHost<E extends CoprocessorEnvironment> {
  public static final String REGION_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.region.classes";
  public static final String REGIONSERVER_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.regionserver.classes";
  public static final String USER_REGION_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.user.region.classes";
  public static final String MASTER_COPROCESSOR_CONF_KEY =
      "hbase.coprocessor.master.classes";
  public static final String WAL_COPROCESSOR_CONF_KEY =
    "hbase.coprocessor.wal.classes";
  public static final String ABORT_ON_ERROR_KEY = "hbase.coprocessor.abortonerror";
  public static final boolean DEFAULT_ABORT_ON_ERROR = true;
  public static final String COPROCESSORS_ENABLED_CONF_KEY = "hbase.coprocessor.enabled";
  public static final boolean DEFAULT_COPROCESSORS_ENABLED = true;
  public static final String USER_COPROCESSORS_ENABLED_CONF_KEY =
    "hbase.coprocessor.user.enabled";
  public static final boolean DEFAULT_USER_COPROCESSORS_ENABLED = true;

  private static final Log LOG = LogFactory.getLog(CoprocessorHost.class);
  protected Abortable abortable;
  /** Ordered set of loaded coprocessors with lock */
  protected SortedList<E> coprocessors = new SortedList<>(new EnvironmentPriorityComparator());
  protected Configuration conf;
  // unique file prefix to use for local copies of jars when classloading
  protected String pathPrefix;
  protected AtomicInteger loadSequence = new AtomicInteger();

  public CoprocessorHost(Abortable abortable) {
    this.abortable = abortable;
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
  private static Set<String> coprocessorNames =
      Collections.synchronizedSet(new HashSet<String>());

  public static Set<String> getLoadedCoprocessors() {
    synchronized (coprocessorNames) {
      return new HashSet(coprocessorNames);
    }
  }

  /**
   * Used to create a parameter to the HServerLoad constructor so that
   * HServerLoad can provide information about the coprocessors loaded by this
   * regionserver.
   * (HBASE-4070: Improve region server metrics to report loaded coprocessors
   * to master).
   */
  public Set<String> getCoprocessors() {
    Set<String> returnValue = new TreeSet<>();
    for (CoprocessorEnvironment e: coprocessors) {
      returnValue.add(e.getInstance().getClass().getSimpleName());
    }
    return returnValue;
  }

  /**
   * Load system coprocessors once only. Read the class names from configuration.
   * Called by constructor.
   */
  protected void loadSystemCoprocessors(Configuration conf, String confKey) {
    boolean coprocessorsEnabled = conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_COPROCESSORS_ENABLED);
    if (!coprocessorsEnabled) {
      return;
    }

    Class<?> implClass = null;

    // load default coprocessors from configure file
    String[] defaultCPClasses = conf.getStrings(confKey);
    if (defaultCPClasses == null || defaultCPClasses.length == 0)
      return;

    int priority = Coprocessor.PRIORITY_SYSTEM;
    for (String className : defaultCPClasses) {
      className = className.trim();
      if (findCoprocessor(className) != null) {
        // If already loaded will just continue
        LOG.warn("Attempted duplicate loading of " + className + "; skipped");
        continue;
      }
      ClassLoader cl = this.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
        // Add coprocessors as we go to guard against case where a coprocessor is specified twice
        // in the configuration
        this.coprocessors.add(loadInstance(implClass, priority, conf));
        LOG.info("System coprocessor " + className + " was loaded " +
            "successfully with priority (" + priority + ").");
        ++priority;
      } catch (Throwable t) {
        // We always abort if system coprocessors cannot be loaded
        abortServer(className, t);
      }
    }
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
      Configuration conf) throws IOException {
    String[] includedClassPrefixes = null;
    if (conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY) != null){
      String prefixes = conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY);
      includedClassPrefixes = prefixes.split(";");
    }
    return load(path, className, priority, conf, includedClassPrefixes);
  }

  /**
   * Load a coprocessor implementation into the host
   * @param path path to implementation jar
   * @param className the main class name
   * @param priority chaining priority
   * @param conf configuration for coprocessor
   * @param includedClassPrefixes class name prefixes to include
   * @throws java.io.IOException Exception
   */
  public E load(Path path, String className, int priority,
      Configuration conf, String[] includedClassPrefixes) throws IOException {
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
        implClass = ((CoprocessorClassLoader)cl).loadClass(className, includedClassPrefixes);
      } catch (ClassNotFoundException e) {
        throw new IOException("Cannot load external coprocessor class " + className, e);
      }
    }

    //load custom code for coprocessor
    Thread currentThread = Thread.currentThread();
    ClassLoader hostClassLoader = currentThread.getContextClassLoader();
    try{
      // switch temporarily to the thread classloader for custom CP
      currentThread.setContextClassLoader(cl);
      E cpInstance = loadInstance(implClass, priority, conf);
      return cpInstance;
    } finally {
      // restore the fresh (host) classloader
      currentThread.setContextClassLoader(hostClassLoader);
    }
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public void load(Class<?> implClass, int priority, Configuration conf)
      throws IOException {
    E env = loadInstance(implClass, priority, conf);
    coprocessors.add(env);
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public E loadInstance(Class<?> implClass, int priority, Configuration conf)
      throws IOException {
    if (!Coprocessor.class.isAssignableFrom(implClass)) {
      throw new IOException("Configured class " + implClass.getName() + " must implement "
          + Coprocessor.class.getName() + " interface ");
    }

    // create the instance
    Coprocessor impl;
    Object o = null;
    try {
      o = implClass.newInstance();
      impl = (Coprocessor)o;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
    // create the environment
    E env = createEnvironment(implClass, impl, priority, loadSequence.incrementAndGet(), conf);
    if (env instanceof Environment) {
      ((Environment)env).startup();
    }
    // HBASE-4014: maintain list of loaded coprocessors for later crash analysis
    // if server (master or regionserver) aborts.
    coprocessorNames.add(implClass.getName());
    return env;
  }

  /**
   * Called when a new Coprocessor class is loaded
   */
  public abstract E createEnvironment(Class<?> implClass, Coprocessor instance,
      int priority, int sequence, Configuration conf);

  public void shutdown(CoprocessorEnvironment e) {
    if (e instanceof Environment) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stop coprocessor " + e.getInstance().getClass().getName());
      }
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
   * Find list of coprocessors that extend/implement the given class/interface
   * @param cls the class/interface to look for
   * @return the list of coprocessors, or null if not found
   */
  public <T extends Coprocessor> List<T> findCoprocessors(Class<T> cls) {
    ArrayList<T> ret = new ArrayList<>();

    for (E env: coprocessors) {
      Coprocessor cp = env.getInstance();

      if(cp != null) {
        if (cls.isAssignableFrom(cp.getClass())) {
          ret.add((T)cp);
        }
      }
    }
    return ret;
  }

  /**
   * Find list of CoprocessorEnvironment that extend/implement the given class/interface
   * @param cls the class/interface to look for
   * @return the list of CoprocessorEnvironment, or null if not found
   */
  public List<CoprocessorEnvironment> findCoprocessorEnvironment(Class<?> cls) {
    ArrayList<CoprocessorEnvironment> ret = new ArrayList<>();

    for (E env: coprocessors) {
      Coprocessor cp = env.getInstance();

      if(cp != null) {
        if (cls.isAssignableFrom(cp.getClass())) {
          ret.add(env);
        }
      }
    }
    return ret;
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
    Set<ClassLoader> externalClassLoaders = new HashSet<>();
    final ClassLoader systemClassLoader = this.getClass().getClassLoader();
    for (E env : coprocessors) {
      ClassLoader cl = env.getInstance().getClass().getClassLoader();
      if (cl != systemClassLoader){
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
    protected List<Table> openTables =
      Collections.synchronizedList(new ArrayList<Table>());
    private int seq;
    private Configuration conf;
    private ClassLoader classLoader;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public Environment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf) {
      this.impl = impl;
      this.classLoader = impl.getClass().getClassLoader();
      this.priority = priority;
      this.state = Coprocessor.State.INSTALLED;
      this.seq = seq;
      this.conf = conf;
    }

    /** Initialize the environment */
    public void startup() throws IOException {
      if (state == Coprocessor.State.INSTALLED ||
          state == Coprocessor.State.STOPPED) {
        state = Coprocessor.State.STARTING;
        Thread currentThread = Thread.currentThread();
        ClassLoader hostClassLoader = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(this.getClassLoader());
          impl.start(this);
          state = Coprocessor.State.ACTIVE;
        } finally {
          currentThread.setContextClassLoader(hostClassLoader);
        }
      } else {
        LOG.warn("Not starting coprocessor "+impl.getClass().getName()+
            " because not inactive (state="+state.toString()+")");
      }
    }

    /** Clean up the environment */
    protected void shutdown() {
      if (state == Coprocessor.State.ACTIVE) {
        state = Coprocessor.State.STOPPING;
        Thread currentThread = Thread.currentThread();
        ClassLoader hostClassLoader = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(this.getClassLoader());
          impl.stop(this);
          state = Coprocessor.State.STOPPED;
        } catch (IOException ioe) {
          LOG.error("Error stopping coprocessor "+impl.getClass().getName(), ioe);
        } finally {
          currentThread.setContextClassLoader(hostClassLoader);
        }
      } else {
        LOG.warn("Not stopping coprocessor "+impl.getClass().getName()+
            " because not active (state="+state.toString()+")");
      }
      synchronized (openTables) {
        // clean up any table references
        for (Table table: openTables) {
          try {
            ((HTableWrapper)table).internalClose();
          } catch (IOException e) {
            // nothing can be done here
            LOG.warn("Failed to close " +
                table.getName(), e);
          }
        }
      }
    }

    @Override
    public Coprocessor getInstance() {
      return impl;
    }

    @Override
    public ClassLoader getClassLoader() {
      return classLoader;
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

    /**
     * Open a table from within the Coprocessor environment
     * @param tableName the table name
     * @return an interface for manipulating the table
     * @exception java.io.IOException Exception
     */
    @Override
    public Table getTable(TableName tableName) throws IOException {
      return this.getTable(tableName, null);
    }

    /**
     * Open a table from within the Coprocessor environment
     * @param tableName the table name
     * @return an interface for manipulating the table
     * @exception java.io.IOException Exception
     */
    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
      return HTableWrapper.createWrapper(openTables, tableName, this, pool);
    }
  }

  protected void abortServer(final CoprocessorEnvironment environment, final Throwable e) {
    abortServer(environment.getInstance().getClass().getName(), e);
  }

  protected void abortServer(final String coprocessorName, final Throwable e) {
    String message = "The coprocessor " + coprocessorName + " threw " + e.toString();
    LOG.error(message, e);
    if (abortable != null) {
      abortable.abort(message, e);
    } else {
      LOG.warn("No available Abortable, process was not aborted");
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
  // Note to devs: Class comments of all observers ({@link MasterObserver}, {@link WALObserver},
  // etc) mention this nuance of our exception handling so that coprocessor can throw appropriate
  // exceptions depending on situation. If any changes are made to this logic, make sure to
  // update all classes' comments.
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
      abortServer(env, e);
    } else {
      // If available, pull a table name out of the environment
      if(env instanceof RegionCoprocessorEnvironment) {
        String tableName = ((RegionCoprocessorEnvironment)env).getRegionInfo().getTable().getNameAsString();
        LOG.error("Removing coprocessor '" + env.toString() + "' from table '"+ tableName + "'", e);
      } else {
        LOG.error("Removing coprocessor '" + env.toString() + "' from " +
                "environment",e);
      }

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
   * Used to gracefully handle fallback to deprecated methods when we
   * evolve coprocessor APIs.
   *
   * When a particular Coprocessor API is updated to change methods, hosts can support fallback
   * to the deprecated API by using this method to determine if an instance implements the new API.
   * In the event that said support is partial, then in the face of a runtime issue that prevents
   * proper operation {@link #legacyWarning(Class, String)} should be used to let operators know.
   *
   * For examples of this in action, see the implementation of
   * <ul>
   *   <li>{@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost}
   *   <li>{@link org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost}
   * </ul>
   *
   * @param clazz Coprocessor you wish to evaluate
   * @param methodName the name of the non-deprecated method version
   * @param parameterTypes the Class of the non-deprecated method's arguments in the order they are
   *     declared.
   */
  @InterfaceAudience.Private
  protected static boolean useLegacyMethod(final Class<? extends Coprocessor> clazz,
      final String methodName, final Class<?>... parameterTypes) {
    boolean useLegacy;
    // Use reflection to see if they implement the non-deprecated version
    try {
      clazz.getDeclaredMethod(methodName, parameterTypes);
      LOG.debug("Found an implementation of '" + methodName + "' that uses updated method " +
          "signature. Skipping legacy support for invocations in '" + clazz +"'.");
      useLegacy = false;
    } catch (NoSuchMethodException exception) {
      useLegacy = true;
    } catch (SecurityException exception) {
      LOG.warn("The Security Manager denied our attempt to detect if the coprocessor '" + clazz +
          "' requires legacy support; assuming it does. If you get later errors about legacy " +
          "coprocessor use, consider updating your security policy to allow access to the package" +
          " and declared members of your implementation.");
      LOG.debug("Details of Security Manager rejection.", exception);
      useLegacy = true;
    }
    return useLegacy;
  }

  /**
   * Used to limit legacy handling to once per Coprocessor class per classloader.
   */
  private static final Set<Class<? extends Coprocessor>> legacyWarning =
      new ConcurrentSkipListSet<>(
          new Comparator<Class<? extends Coprocessor>>() {
            @Override
            public int compare(Class<? extends Coprocessor> c1, Class<? extends Coprocessor> c2) {
              if (c1.equals(c2)) {
                return 0;
              }
              return c1.getName().compareTo(c2.getName());
            }
          });

  /**
   * limits the amount of logging to once per coprocessor class.
   * Used in concert with {@link #useLegacyMethod(Class, String, Class[])} when a runtime issue
   * prevents properly supporting the legacy version of a coprocessor API.
   * Since coprocessors can be in tight loops this serves to limit the amount of log spam we create.
   */
  @InterfaceAudience.Private
  protected void legacyWarning(final Class<? extends Coprocessor> clazz, final String message) {
    if(legacyWarning.add(clazz)) {
      LOG.error("You have a legacy coprocessor loaded and there are events we can't map to the " +
          " deprecated API. Your coprocessor will not see these events.  Please update '" + clazz +
          "'. Details of the problem: " + message);
    }
  }
}

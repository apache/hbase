/*
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.SortedList;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;

/**
 * Provides the common setup framework and runtime services for coprocessor
 * invocation from HBase services.
 * @param <C> type of specific coprocessor this host will handle
 * @param <E> type of specific coprocessor environment this host requires.
 * provides
 */
@InterfaceAudience.Private
public abstract class CoprocessorHost<C extends Coprocessor, E extends CoprocessorEnvironment<C>> {
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
  public static final String SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR =
      "hbase.skip.load.duplicate.table.coprocessor";
  public static final boolean DEFAULT_SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR = false;

  private static final Logger LOG = LoggerFactory.getLogger(CoprocessorHost.class);
  protected Abortable abortable;
  /** Ordered set of loaded coprocessors with lock */
  protected final SortedList<E> coprocEnvironments =
      new SortedList<>(new EnvironmentPriorityComparator());
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
   * added to coprocessorNames, by checkAndLoadInstance() but are never removed, since
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
    for (E e: coprocEnvironments) {
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

    Class<?> implClass;

    // load default coprocessors from configure file
    String[] defaultCPClasses = conf.getStrings(confKey);
    if (defaultCPClasses == null || defaultCPClasses.length == 0)
      return;

    int currentSystemPriority = Coprocessor.PRIORITY_SYSTEM;
    for (String className : defaultCPClasses) {
      // After HBASE-23710 and HBASE-26714 when configuring for system coprocessor, we accept
      // an optional format of className|priority|path
      String[] classNameToken = className.split("\\|");
      boolean hasPriorityOverride = false;
      boolean hasPath = false;
      className = classNameToken[0];
      int overridePriority = Coprocessor.PRIORITY_SYSTEM;
      Path path = null;
      if (classNameToken.length > 1 && !Strings.isNullOrEmpty(classNameToken[1])) {
        overridePriority = Integer.parseInt(classNameToken[1]);
        hasPriorityOverride = true;
      }
      if (classNameToken.length > 2 && !Strings.isNullOrEmpty(classNameToken[2])) {
        path = new Path(classNameToken[2].trim());
        hasPath = true;
      }
      className = className.trim();
      if (findCoprocessor(className) != null) {
        // If already loaded will just continue
        LOG.warn("Attempted duplicate loading of " + className + "; skipped");
        continue;
      }
      ClassLoader cl = this.getClass().getClassLoader();
      try {
        // override the class loader if a path for the system coprocessor is provided.
        if (hasPath) {
          cl = CoprocessorClassLoader.getClassLoader(path, this.getClass().getClassLoader(),
            pathPrefix, conf);
        }
        Thread.currentThread().setContextClassLoader(cl);
        implClass = cl.loadClass(className);
        int coprocPriority = hasPriorityOverride ? overridePriority : currentSystemPriority;
        // Add coprocessors as we go to guard against case where a coprocessor is specified twice
        // in the configuration
        E env = checkAndLoadInstance(implClass, coprocPriority, conf);
        if (env != null) {
          this.coprocEnvironments.add(env);
          LOG.info("System coprocessor {} loaded, priority={}.", className, coprocPriority);
          if (!hasPriorityOverride) {
            ++currentSystemPriority;
          }
        }
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
    Class<?> implClass;
    LOG.debug("Loading coprocessor class " + className + " with path " +
        path + " and priority " + priority);

    boolean skipLoadDuplicateCoprocessor = conf.getBoolean(SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR,
      DEFAULT_SKIP_LOAD_DUPLICATE_TABLE_COPROCESSOR);
    if (skipLoadDuplicateCoprocessor && findCoprocessor(className) != null) {
      // If already loaded will just continue
      LOG.warn("Attempted duplicate loading of {}; skipped", className);
      return null;
    }

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
      E cpInstance = checkAndLoadInstance(implClass, priority, conf);
      return cpInstance;
    } finally {
      // restore the fresh (host) classloader
      currentThread.setContextClassLoader(hostClassLoader);
    }
  }

  public void load(Class<? extends C> implClass, int priority, Configuration conf)
      throws IOException {
    E env = checkAndLoadInstance(implClass, priority, conf);
    coprocEnvironments.add(env);
  }

  /**
   * @param implClass Implementation class
   * @param priority priority
   * @param conf configuration
   * @throws java.io.IOException Exception
   */
  public E checkAndLoadInstance(Class<?> implClass, int priority, Configuration conf)
      throws IOException {
    // create the instance
    C impl;
    try {
      impl = checkAndGetInstance(implClass);
      if (impl == null) {
        LOG.error("Cannot load coprocessor " + implClass.getSimpleName());
        return null;
      }
    } catch (InstantiationException|IllegalAccessException e) {
      throw new IOException(e);
    }
    // create the environment
    E env = createEnvironment(impl, priority, loadSequence.incrementAndGet(), conf);
    assert env instanceof BaseEnvironment;
    ((BaseEnvironment<C>) env).startup();
    // HBASE-4014: maintain list of loaded coprocessors for later crash analysis
    // if server (master or regionserver) aborts.
    coprocessorNames.add(implClass.getName());
    return env;
  }

  /**
   * Called when a new Coprocessor class is loaded
   */
  public abstract E createEnvironment(C instance, int priority, int sequence, Configuration conf);

  /**
   * Called when a new Coprocessor class needs to be loaded. Checks if type of the given class
   * is what the corresponding host implementation expects. If it is of correct type, returns an
   * instance of the coprocessor to be loaded. If not, returns null.
   * If an exception occurs when trying to create instance of a coprocessor, it's passed up and
   * eventually results into server aborting.
   */
  public abstract C checkAndGetInstance(Class<?> implClass)
      throws InstantiationException, IllegalAccessException;

  public void shutdown(E e) {
    assert e instanceof BaseEnvironment;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stop coprocessor " + e.getInstance().getClass().getName());
    }
    ((BaseEnvironment<C>) e).shutdown();
  }

  /**
   * Find coprocessors by full class name or simple name.
   */
  public C findCoprocessor(String className) {
    for (E env: coprocEnvironments) {
      if (env.getInstance().getClass().getName().equals(className) ||
          env.getInstance().getClass().getSimpleName().equals(className)) {
        return env.getInstance();
      }
    }
    return null;
  }

  public <T extends C> T findCoprocessor(Class<T> cls) {
    for (E env: coprocEnvironments) {
      if (cls.isAssignableFrom(env.getInstance().getClass())) {
        return (T) env.getInstance();
      }
    }
    return null;
  }

  /**
   * Find list of coprocessors that extend/implement the given class/interface
   * @param cls the class/interface to look for
   * @return the list of coprocessors, or null if not found
   */
  public <T extends C> List<T> findCoprocessors(Class<T> cls) {
    ArrayList<T> ret = new ArrayList<>();

    for (E env: coprocEnvironments) {
      C cp = env.getInstance();

      if(cp != null) {
        if (cls.isAssignableFrom(cp.getClass())) {
          ret.add((T)cp);
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
  public E findCoprocessorEnvironment(String className) {
    for (E env: coprocEnvironments) {
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
    for (E env : coprocEnvironments) {
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
  static class EnvironmentPriorityComparator implements Comparator<CoprocessorEnvironment> {
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

  protected void abortServer(final E environment, final Throwable e) {
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
  protected void handleCoprocessorThrowable(final E env, final Throwable e) throws IOException {
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

      coprocEnvironments.remove(env);
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
   * Implementations defined function to get an observer of type {@code O} from a coprocessor of
   * type {@code C}. Concrete implementations of CoprocessorHost define one getter for each
   * observer they can handle. For e.g. RegionCoprocessorHost will use 3 getters, one for
   * each of RegionObserver, EndpointObserver and BulkLoadObserver.
   * These getters are used by {@code ObserverOperation} to get appropriate observer from the
   * coprocessor.
   */
  @FunctionalInterface
  public interface ObserverGetter<C, O> extends Function<C, Optional<O>> {}

  private abstract class ObserverOperation<O> extends ObserverContextImpl<E> {
    ObserverGetter<C, O> observerGetter;

    ObserverOperation(ObserverGetter<C, O> observerGetter) {
      this(observerGetter, null);
    }

    ObserverOperation(ObserverGetter<C, O> observerGetter, User user) {
      this(observerGetter, user, false);
    }

    ObserverOperation(ObserverGetter<C, O> observerGetter, boolean bypassable) {
      this(observerGetter, null, bypassable);
    }

    ObserverOperation(ObserverGetter<C, O> observerGetter, User user, boolean bypassable) {
      super(user != null? user: RpcServer.getRequestUser().orElse(null), bypassable);
      this.observerGetter = observerGetter;
    }

    abstract void callObserver() throws IOException;
    protected void postEnvCall() {}
  }

  // Can't derive ObserverOperation from ObserverOperationWithResult (R = Void) because then all
  // ObserverCaller implementations will have to have a return statement.
  // O = observer, E = environment, C = coprocessor, R=result type
  public abstract class ObserverOperationWithoutResult<O> extends ObserverOperation<O> {
    protected abstract void call(O observer) throws IOException;

    public ObserverOperationWithoutResult(ObserverGetter<C, O> observerGetter) {
      super(observerGetter);
    }

    public ObserverOperationWithoutResult(ObserverGetter<C, O> observerGetter, User user) {
      super(observerGetter, user);
    }

    public ObserverOperationWithoutResult(ObserverGetter<C, O> observerGetter, User user,
        boolean bypassable) {
      super(observerGetter, user, bypassable);
    }

    /**
     * In case of coprocessors which have many kinds of observers (for eg, {@link RegionCoprocessor}
     * has BulkLoadObserver, RegionObserver, etc), some implementations may not need all
     * observers, in which case they will return null for that observer's getter.
     * We simply ignore such cases.
     */
    @Override
    void callObserver() throws IOException {
      Optional<O> observer = observerGetter.apply(getEnvironment().getInstance());
      if (observer.isPresent()) {
        call(observer.get());
      }
    }
  }

  public abstract class ObserverOperationWithResult<O, R> extends ObserverOperation<O> {
    protected abstract R call(O observer) throws IOException;

    private R result;

    public ObserverOperationWithResult(ObserverGetter<C, O> observerGetter, R result) {
      this(observerGetter, result, false);
    }

    public ObserverOperationWithResult(ObserverGetter<C, O> observerGetter, R result,
        boolean bypassable) {
      this(observerGetter, result, null, bypassable);
    }

    public ObserverOperationWithResult(ObserverGetter<C, O> observerGetter, R result,
        User user) {
      this(observerGetter, result, user, false);
    }

    private ObserverOperationWithResult(ObserverGetter<C, O> observerGetter, R result, User user,
        boolean bypassable) {
      super(observerGetter, user, bypassable);
      this.result = result;
    }

    protected R getResult() {
      return this.result;
    }

    @Override
    void callObserver() throws IOException {
      Optional<O> observer = observerGetter.apply(getEnvironment().getInstance());
      if (observer.isPresent()) {
        result = call(observer.get());
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Functions to execute observer hooks and handle results (if any)
  //////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Do not call with an observerOperation that is null! Have the caller check.
   */
  protected <O, R> R execOperationWithResult(
      final ObserverOperationWithResult<O, R> observerOperation) throws IOException {
    boolean bypass = execOperation(observerOperation);
    R result = observerOperation.getResult();
    return bypass == observerOperation.isBypassable()? result: null;
  }

  /**
   * @return True if we are to bypass (Can only be <code>true</code> if
   * ObserverOperation#isBypassable().
   */
  protected <O> boolean execOperation(final ObserverOperation<O> observerOperation)
      throws IOException {
    boolean bypass = false;
    if (observerOperation == null) {
      return bypass;
    }
    List<E> envs = coprocEnvironments.get();
    for (E env : envs) {
      observerOperation.prepare(env);
      Thread currentThread = Thread.currentThread();
      ClassLoader cl = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(env.getClassLoader());
        observerOperation.callObserver();
      } catch (Throwable e) {
        handleCoprocessorThrowable(env, e);
      } finally {
        currentThread.setContextClassLoader(cl);
      }
      // Internal to shouldBypass, it checks if obeserverOperation#isBypassable().
      bypass |= observerOperation.shouldBypass();
      observerOperation.postEnvCall();
      if (bypass) {
        // If CP says bypass, skip out w/o calling any following CPs; they might ruin our response.
        // In hbase1, this used to be called 'complete'. In hbase2, we unite bypass and 'complete'.
        break;
      }
    }
    return bypass;
  }

  /**
   * Coprocessor classes can be configured in any order, based on that priority is set and
   * chained in a sorted order. Should be used preStop*() hooks i.e. when master/regionserver is
   * going down. This function first calls coprocessor methods (using ObserverOperation.call())
   * and then shutdowns the environment in postEnvCall(). <br>
   * Need to execute all coprocessor methods first then postEnvCall(), otherwise some coprocessors
   * may remain shutdown if any exception occurs during next coprocessor execution which prevent
   * master/regionserver stop or cluster shutdown. (Refer:
   * <a href="https://issues.apache.org/jira/browse/HBASE-16663">HBASE-16663</a>
   * @return true if bypaas coprocessor execution, false if not.
   * @throws IOException
   */
  protected <O> boolean execShutdown(final ObserverOperation<O> observerOperation)
      throws IOException {
    if (observerOperation == null) return false;
    boolean bypass = false;
    List<E> envs = coprocEnvironments.get();
    // Iterate the coprocessors and execute ObserverOperation's call()
    for (E env : envs) {
      observerOperation.prepare(env);
      Thread currentThread = Thread.currentThread();
      ClassLoader cl = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(env.getClassLoader());
        observerOperation.callObserver();
      } catch (Throwable e) {
        handleCoprocessorThrowable(env, e);
      } finally {
        currentThread.setContextClassLoader(cl);
      }
      bypass |= observerOperation.shouldBypass();
    }

    // Iterate the coprocessors and execute ObserverOperation's postEnvCall()
    for (E env : envs) {
      observerOperation.prepare(env);
      observerOperation.postEnvCall();
    }
    return bypass;
  }
}

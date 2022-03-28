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
package org.apache.hadoop.hbase.zookeeper;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A zookeeper that can handle 'recoverable' errors.
 * To handle recoverable errors, developers need to realize that there are two
 * classes of requests: idempotent and non-idempotent requests. Read requests
 * and unconditional sets and deletes are examples of idempotent requests, they
 * can be reissued with the same results.
 * (Although, the delete may throw a NoNodeException on reissue its effect on
 * the ZooKeeper state is the same.) Non-idempotent requests need special
 * handling, application and library writers need to keep in mind that they may
 * need to encode information in the data or name of znodes to detect
 * retries. A simple example is a create that uses a sequence flag.
 * If a process issues a create("/x-", ..., SEQUENCE) and gets a connection
 * loss exception, that process will reissue another
 * create("/x-", ..., SEQUENCE) and get back x-111. When the process does a
 * getChildren("/"), it sees x-1,x-30,x-109,x-110,x-111, now it could be
 * that x-109 was the result of the previous create, so the process actually
 * owns both x-109 and x-111. An easy way around this is to use "x-process id-"
 * when doing the create. If the process is using an id of 352, before reissuing
 * the create it will do a getChildren("/") and see "x-222-1", "x-542-30",
 * "x-352-109", x-333-110". The process will know that the original create
 * succeeded an the znode it created is "x-352-109".
 * @see "https://cwiki.apache.org/confluence/display/HADOOP2/ZooKeeper+ErrorHandling"
 */
@InterfaceAudience.Private
public class RecoverableZooKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(RecoverableZooKeeper.class);
  // the actual ZooKeeper client instance
  private ZooKeeper zk;
  private final RetryCounterFactory retryCounterFactory;
  // An identifier of this process in the cluster
  private final String identifier;
  private final byte[] id;
  private final Watcher watcher;
  private final int sessionTimeout;
  private final String quorumServers;
  private final int maxMultiSize;

  /**
   * See {@link #connect(Configuration, String, Watcher, String)}
   */
  public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher)
    throws IOException {
    String ensemble = ZKConfig.getZKQuorumServersString(conf);
    return connect(conf, ensemble, watcher);
  }

  /**
   * See {@link #connect(Configuration, String, Watcher, String)}
   */
  public static RecoverableZooKeeper connect(Configuration conf, String ensemble,
    Watcher watcher)
    throws IOException {
    return connect(conf, ensemble, watcher, null);
  }

  /**
   * Creates a new connection to ZooKeeper, pulling settings and ensemble config
   * from the specified configuration object using methods from {@link ZKConfig}.
   *
   * Sets the connection status monitoring watcher to the specified watcher.
   *
   * @param conf configuration to pull ensemble and other settings from
   * @param watcher watcher to monitor connection changes
   * @param ensemble ZooKeeper servers quorum string
   * @param identifier value used to identify this client instance.
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static RecoverableZooKeeper connect(Configuration conf, String ensemble,
    Watcher watcher, final String identifier)
    throws IOException {
    if(ensemble == null) {
      throw new IOException("Unable to determine ZooKeeper ensemble");
    }
    int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
      HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} opening connection to ZooKeeper ensemble={}", identifier, ensemble);
    }
    int retry = conf.getInt("zookeeper.recovery.retry", 3);
    int retryIntervalMillis =
      conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    int maxSleepTime = conf.getInt("zookeeper.recovery.retry.maxsleeptime", 60000);
    int multiMaxSize = conf.getInt("zookeeper.multi.max.size", 1024*1024);
    return new RecoverableZooKeeper(ensemble, timeout, watcher,
      retry, retryIntervalMillis, maxSleepTime, identifier, multiMaxSize);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DE_MIGHT_IGNORE",
      justification="None. Its always been this way.")
  public RecoverableZooKeeper(String quorumServers, int sessionTimeout,
      Watcher watcher, int maxRetries, int retryIntervalMillis, int maxSleepTime, String identifier,
      int maxMultiSize)
    throws IOException {
    // TODO: Add support for zk 'chroot'; we don't add it to the quorumServers String as we should.
    this.retryCounterFactory =
      new RetryCounterFactory(maxRetries+1, retryIntervalMillis, maxSleepTime);

    if (identifier == null || identifier.length() == 0) {
      // the identifier = processID@hostName
      identifier = ManagementFactory.getRuntimeMXBean().getName();
    }
    LOG.info("Process identifier={} connecting to ZooKeeper ensemble={}", identifier,
      quorumServers);
    this.identifier = identifier;
    this.id = Bytes.toBytes(identifier);

    this.watcher = watcher;
    this.sessionTimeout = sessionTimeout;
    this.quorumServers = quorumServers;
    this.maxMultiSize = maxMultiSize;

    try {
      checkZk();
    } catch (Exception x) {
      /* ignore */
    }
  }

  /**
   * Returns the maximum size (in bytes) that should be included in any single multi() call.
   *
   * NB: This is an approximation, so there may be variance in the msg actually sent over the
   * wire. Please be sure to set this approximately, with respect to your ZK server configuration
   * for jute.maxbuffer.
   */
  public int getMaxMultiSizeLimit() {
    return maxMultiSize;
  }

  /**
   * Try to create a ZooKeeper connection. Turns any exception encountered into a
   * KeeperException.OperationTimeoutException so it can retried.
   * @return The created ZooKeeper connection object
   * @throws KeeperException if a ZooKeeper operation fails
   */
  protected synchronized ZooKeeper checkZk() throws KeeperException {
    if (this.zk == null) {
      try {
        this.zk = new ZooKeeper(quorumServers, sessionTimeout, watcher);
      } catch (IOException ex) {
        LOG.warn("Unable to create ZooKeeper Connection", ex);
        throw new KeeperException.OperationTimeoutException();
      }
    }
    return zk;
  }

  public synchronized void reconnectAfterExpiration()
        throws IOException, KeeperException, InterruptedException {
    if (zk != null) {
      LOG.info("Closing dead ZooKeeper connection, session" +
        " was: 0x"+Long.toHexString(zk.getSessionId()));
      zk.close();
      // reset the ZooKeeper connection
      zk = null;
    }
    checkZk();
    LOG.info("Recreated a ZooKeeper, session" +
      " is: 0x"+Long.toHexString(zk.getSessionId()));
  }

  /**
   * delete is an idempotent operation. Retry before throwing exception.
   * This function will not throw NoNodeException if the path does not
   * exist.
   */
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.delete").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      boolean isRetry = false; // False for first attempt, true for all retries.
      while (true) {
        try {
          checkZk().delete(path, version);
          return;
        } catch (KeeperException e) {
          switch (e.code()) {
            case NONODE:
              if (isRetry) {
                LOG.debug("Node " + path + " already deleted. Assuming a " +
                    "previous attempt succeeded.");
                return;
              }
              LOG.debug("Node {} already deleted, retry={}", path, isRetry);
              throw e;

            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "delete");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
        isRetry = true;
      }
    } finally {
      span.end();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throwing exception
   * @return A Stat instance
   */
  public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
    return exists(path, watcher, null);
  }

  private Stat exists(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.exists").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          Stat nodeStat;
          if (watch == null) {
            nodeStat = checkZk().exists(path, watcher);
          } else {
            nodeStat = checkZk().exists(path, watch);
          }
          return nodeStat;
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "exists");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * exists is an idempotent operation. Retry before throwing exception
   * @return A Stat instance
   */
  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    return exists(path, null, watch);
  }

  private void retryOrThrow(RetryCounter retryCounter, KeeperException e,
      String opName) throws KeeperException {
    if (!retryCounter.shouldRetry()) {
      LOG.error("ZooKeeper {} failed after {} attempts", opName, retryCounter.getMaxAttempts());
      throw e;
    }
    LOG.debug("Retry, connectivity issue (JVM Pause?); quorum={},exception{}=", quorumServers, e);
  }

  /**
   * getChildren is an idempotent operation. Retry before throwing exception
   * @return List of children znodes
   */
  public List<String> getChildren(String path, Watcher watcher)
    throws KeeperException, InterruptedException {
    return getChildren(path, watcher, null);
  }

  private List<String> getChildren(String path, Watcher watcher, Boolean watch)
    throws InterruptedException, KeeperException {
    Span span =
      TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.getChildren").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          List<String> children;
          if (watch == null) {
            children = checkZk().getChildren(path, watcher);
          } else {
            children = checkZk().getChildren(path, watch);
          }
          return children;
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "getChildren");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * getChildren is an idempotent operation. Retry before throwing exception
   * @return List of children znodes
   */
  public List<String> getChildren(String path, boolean watch)
    throws KeeperException, InterruptedException {
    return getChildren(path, null, watch);
  }

  /**
   * getData is an idempotent operation. Retry before throwing exception
   * @return Data
   */
  public byte[] getData(String path, Watcher watcher, Stat stat)
    throws KeeperException, InterruptedException {
    return getData(path, watcher, null, stat);
  }

  private byte[] getData(String path, Watcher watcher, Boolean watch, Stat stat)
    throws InterruptedException, KeeperException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.getData").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          byte[] revData;
          if (watch == null) {
            revData = checkZk().getData(path, watcher, stat);
          } else {
            revData = checkZk().getData(path, watch, stat);
          }
          return ZKMetadata.removeMetaData(revData);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "getData");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * getData is an idempotent operation. Retry before throwing exception
   * @return Data
   */
  public byte[] getData(String path, boolean watch, Stat stat)
    throws KeeperException, InterruptedException {
    return getData(path, null, watch, stat);
  }

  /**
   * setData is NOT an idempotent operation. Retry may cause BadVersion Exception
   * Adding an identifier field into the data to check whether
   * badversion is caused by the result of previous correctly setData
   * @return Stat instance
   */
  public Stat setData(String path, byte[] data, int version)
    throws KeeperException, InterruptedException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.setData").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      byte[] newData = ZKMetadata.appendMetaData(id, data);
      boolean isRetry = false;
      while (true) {
        try {
          return checkZk().setData(path, newData, version);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "setData");
              break;
            case BADVERSION:
              if (isRetry) {
                // try to verify whether the previous setData success or not
                try{
                  Stat stat = new Stat();
                  byte[] revData = checkZk().getData(path, false, stat);
                  if(Bytes.compareTo(revData, newData) == 0) {
                    // the bad version is caused by previous successful setData
                    return stat;
                  }
                } catch(KeeperException keeperException){
                  // the ZK is not reliable at this moment. just throwing exception
                  throw keeperException;
                }
              }
            // throw other exceptions and verified bad version exceptions
            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
        isRetry = true;
      }
    } finally {
      span.end();
    }
  }

  /**
   * getAcl is an idempotent operation. Retry before throwing exception
   * @return list of ACLs
   */
  public List<ACL> getAcl(String path, Stat stat) throws KeeperException, InterruptedException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.getAcl").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          return checkZk().getACL(path, stat);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "getAcl");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * setAcl is an idempotent operation. Retry before throwing exception
   * @return list of ACLs
   */
  public Stat setAcl(String path, List<ACL> acls, int version)
    throws KeeperException, InterruptedException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.setAcl").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      while (true) {
        try {
          return checkZk().setACL(path, acls, version);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
              retryOrThrow(retryCounter, e, "setAcl");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  /**
   * <p>
   * NONSEQUENTIAL create is idempotent operation.
   * Retry before throwing exceptions.
   * But this function will not throw the NodeExist exception back to the
   * application.
   * </p>
   * <p>
   * But SEQUENTIAL is NOT idempotent operation. It is necessary to add
   * identifier to the path to verify, whether the previous one is successful
   * or not.
   * </p>
   *
   * @return Path
   */
  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.create").startSpan();
    try (Scope scope = span.makeCurrent()) {
      byte[] newData = ZKMetadata.appendMetaData(id, data);
      switch (createMode) {
        case EPHEMERAL:
        case PERSISTENT:
          return createNonSequential(path, newData, acl, createMode);

        case EPHEMERAL_SEQUENTIAL:
        case PERSISTENT_SEQUENTIAL:
          return createSequential(path, newData, acl, createMode);

        default:
          throw new IllegalArgumentException("Unrecognized CreateMode: " +
              createMode);
      }
    } finally {
      span.end();
    }
  }

  private String createNonSequential(String path, byte[] data, List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean isRetry = false; // False for first attempt, true for all retries.
    while (true) {
      try {
        return checkZk().create(path, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case NODEEXISTS:
            if (isRetry) {
              // If the connection was lost, there is still a possibility that
              // we have successfully created the node at our previous attempt,
              // so we read the node and compare.
              byte[] currentData = checkZk().getData(path, false, null);
              if (currentData != null &&
                  Bytes.compareTo(currentData, data) == 0) {
                // We successfully created a non-sequential node
                return path;
              }
              LOG.error("Node " + path + " already exists with " +
                  Bytes.toStringBinary(currentData) + ", could not write " +
                  Bytes.toStringBinary(data));
              throw e;
            }
            LOG.trace("Node {} already exists", path);
            throw e;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
          case REQUESTTIMEOUT:
            retryOrThrow(retryCounter, e, "create");
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      isRetry = true;
    }
  }

  private String createSequential(String path, byte[] data,
      List<ACL> acl, CreateMode createMode)
    throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean first = true;
    String newPath = path+this.identifier;
    while (true) {
      try {
        if (!first) {
          // Check if we succeeded on a previous attempt
          String previousResult = findPreviousSequentialNode(newPath);
          if (previousResult != null) {
            return previousResult;
          }
        }
        first = false;
        return checkZk().create(newPath, data, acl, createMode);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
          case REQUESTTIMEOUT:
            retryOrThrow(retryCounter, e, "create");
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
    }
  }
  /**
   * Convert Iterable of {@link org.apache.zookeeper.Op} we got into the ZooKeeper.Op
   * instances to actually pass to multi (need to do this in order to appendMetaData).
   */
  private Iterable<Op> prepareZKMulti(Iterable<Op> ops) throws UnsupportedOperationException {
    if(ops == null) {
      return null;
    }

    List<Op> preparedOps = new LinkedList<>();
    for (Op op : ops) {
      if (op.getType() == ZooDefs.OpCode.create) {
        CreateRequest create = (CreateRequest)op.toRequestRecord();
        preparedOps.add(Op.create(create.getPath(), ZKMetadata.appendMetaData(id, create.getData()),
          create.getAcl(), create.getFlags()));
      } else if (op.getType() == ZooDefs.OpCode.delete) {
        // no need to appendMetaData for delete
        preparedOps.add(op);
      } else if (op.getType() == ZooDefs.OpCode.setData) {
        SetDataRequest setData = (SetDataRequest)op.toRequestRecord();
        preparedOps.add(Op.setData(setData.getPath(),
                ZKMetadata.appendMetaData(id, setData.getData()), setData.getVersion()));
      } else {
        throw new UnsupportedOperationException("Unexpected ZKOp type: " + op.getClass().getName());
      }
    }
    return preparedOps;
  }

  /**
   * Run multiple operations in a transactional manner. Retry before throwing exception
   */
  public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
    Span span = TraceUtil.getGlobalTracer().spanBuilder("RecoverableZookeeper.multi").startSpan();
    try (Scope scope = span.makeCurrent()) {
      RetryCounter retryCounter = retryCounterFactory.create();
      Iterable<Op> multiOps = prepareZKMulti(ops);
      while (true) {
        try {
          return checkZk().multi(multiOps);
        } catch (KeeperException e) {
          switch (e.code()) {
            case CONNECTIONLOSS:
            case OPERATIONTIMEOUT:
            case REQUESTTIMEOUT:
              retryOrThrow(retryCounter, e, "multi");
              break;

            default:
              throw e;
          }
        }
        retryCounter.sleepUntilNextRetry();
      }
    } finally {
      span.end();
    }
  }

  private String findPreviousSequentialNode(String path)
    throws KeeperException, InterruptedException {
    int lastSlashIdx = path.lastIndexOf('/');
    assert(lastSlashIdx != -1);
    String parent = path.substring(0, lastSlashIdx);
    String nodePrefix = path.substring(lastSlashIdx+1);
    List<String> nodes = checkZk().getChildren(parent, false);
    List<String> matching = filterByPrefix(nodes, nodePrefix);
    for (String node : matching) {
      String nodePath = parent + "/" + node;
      Stat stat = checkZk().exists(nodePath, false);
      if (stat != null) {
        return nodePath;
      }
    }
    return null;
  }

  public synchronized long getSessionId() {
    return zk == null ? -1 : zk.getSessionId();
  }

  public synchronized void close() throws InterruptedException {
    if (zk != null) {
      zk.close();
    }
  }

  public synchronized States getState() {
    return zk == null ? null : zk.getState();
  }

  public synchronized ZooKeeper getZooKeeper() {
    return zk;
  }

  public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) throws KeeperException {
    checkZk().sync(path, cb, ctx);
  }

  /**
   * Filters the given node list by the given prefixes.
   * This method is all-inclusive--if any element in the node list starts
   * with any of the given prefixes, then it is included in the result.
   *
   * @param nodes the nodes to filter
   * @param prefixes the prefixes to include in the result
   * @return list of every element that starts with one of the prefixes
   */
  private static List<String> filterByPrefix(List<String> nodes,
      String... prefixes) {
    List<String> lockChildren = new ArrayList<>();
    for (String child : nodes){
      for (String prefix : prefixes){
        if (child.startsWith(prefix)){
          lockChildren.add(child);
          break;
        }
      }
    }
    return lockChildren;
  }

  public String getIdentifier() {
    return identifier;
  }
}

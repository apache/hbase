/*
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
package org.apache.hadoop.hbase.security.access;

import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that AccessController implements every security-relevant method declared in the five
 * observer interfaces it claims to implement: MasterObserver, RegionObserver, RegionServerObserver,
 * EndpointObserver, BulkLoadObserver.
 * <p>
 * If a new hook is added to any of these interfaces and AccessController does not override it, the
 * default no-op implementation will silently skip the permission check — a potential privilege
 * escalation. This test catches that at build time.
 * <p>
 * Skipped methods are determined by two mechanisms:
 * <ul>
 * <li><b>Pattern rules</b> — methods matching these patterns are always safe to skip:
 * <ul>
 * <li>{@code post*} — post-operation notifications; the operation has already been authorized and
 * executed.</li>
 * <li>{@code pre*Action} — procedure-level action hooks; authorization happens at the RPC layer in
 * the corresponding {@code pre*} hook.</li>
 * </ul>
 * </li>
 * <li><b>Explicit whitelist</b> — methods that don't match the above rules but are still safe to
 * skip (internal lifecycle hooks, deprecated overloads with default delegation, read-only queries).
 * Each entry has a justification comment.</li>
 * </ul>
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestAccessControllerObserverCoverage {

  /**
   * Explicit whitelist for methods that don't match the pattern rules but are intentionally not
   * overridden in AccessController.
   * <p>
   * Use simple method name (covers all overloads) or full signature key
   * "methodName(ParamType1,ParamType2,..." using simple class names.
   */
  private static final Set<String> WHITELIST = new HashSet<>(Arrays.asList(

    // --- Internal lifecycle hooks (not client-facing RPCs) ---
    // Store file / WAL internal hooks
    "preStoreFileReaderOpen", "preStoreScannerOpen", "preCommitStoreFile", "preReplayWALs",
    "preWALRestore",
    // Master internal
    "preMasterStoreFlush",
    // Lifecycle markers (not triggered by client RPC)
    "preMasterInitialization", "preCreateTableRegionsInfos",
    // --- Read-only query hooks (no mutation, no authorization needed) ---
    "preGetClusterMetrics", "preGetTableNames", "preListNamespaceDescriptors", "preListNamespaces",
    // RSGroup related methods, access control is implemented in RSGroupAdminEndpoint
    "preMoveServers", "preMoveServersAndTables", "preMoveTables", "preRemoveServers",

    // --- Deprecated overloads: interface default delegates to non-deprecated ---
    // prePut(3-arg) delegates to prePut(4-arg Durability)
    "prePut(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Put,org.apache.hadoop.hbase.wal.WALEdit)",
    // preDelete(3-arg) delegates to preDelete(4-arg Durability)
    "preDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Delete,org.apache.hadoop.hbase.wal.WALEdit)",
    // preAppend(3-arg) delegates to preAppend(2-arg deprecated)
    "preAppend(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Append,org.apache.hadoop.hbase.wal.WALEdit)",
    // preAppendAfterRowLock(2-arg) delegates to preAppendAfterRowLock(1-arg deprecated)
    "preAppendAfterRowLock(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Append)",
    // preIncrement(3-arg) delegates to preIncrement(2-arg deprecated)
    "preIncrement(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Increment,org.apache.hadoop.hbase.wal.WALEdit)",
    // preIncrementAfterRowLock(2-arg) delegates to preIncrementAfterRowLock(1-arg deprecated)
    "preIncrementAfterRowLock(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.Increment)",
    // preCheckAndMutate delegates to preCheckAndPut/preCheckAndDelete
    "preCheckAndMutate(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.CheckAndMutate,org.apache.hadoop.hbase.client.CheckAndMutateResult)",
    // preCheckAndMutateAfterRowLock delegates to
    // preCheckAndPutAfterRowLock/preCheckAndDeleteAfterRowLock
    "preCheckAndMutateAfterRowLock(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.CheckAndMutate,org.apache.hadoop.hbase.client.CheckAndMutateResult)",
    // Filter-based CheckAnd* are deprecated; framework uses byte[] overloads
    // Note: Class.getName() returns "[B" for byte[], not "byte[]"
    "preCheckAndPut(org.apache.hadoop.hbase.coprocessor.ObserverContext,[B,org.apache.hadoop.hbase.filter.Filter,org.apache.hadoop.hbase.client.Put,boolean)",
    "preCheckAndPutAfterRowLock(org.apache.hadoop.hbase.coprocessor.ObserverContext,[B,org.apache.hadoop.hbase.filter.Filter,org.apache.hadoop.hbase.client.Put,boolean)",
    "preCheckAndDelete(org.apache.hadoop.hbase.coprocessor.ObserverContext,[B,org.apache.hadoop.hbase.filter.Filter,org.apache.hadoop.hbase.client.Delete,boolean)",
    "preCheckAndDeleteAfterRowLock(org.apache.hadoop.hbase.coprocessor.ObserverContext,[B,org.apache.hadoop.hbase.filter.Filter,org.apache.hadoop.hbase.client.Delete,boolean)",
    // Deprecated preGetUserPermissions(6-arg) delegates to preGetUserPermissions(7-arg with Scope)
    "preGetUserPermissions(ObserverContext,String,String,TableName,byte[],byte[])",
    // Deprecated WAL-append / timestamp hooks
    "prePrepareTimeStampForDeleteVersion", "preWALAppend",
    // Deprecated preModifyXXX where we do not pass the old descriptor
    "preModifyNamespace(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.NamespaceDescriptor)",
    "preModifyTable(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.TableName,org.apache.hadoop.hbase.client.TableDescriptor)",
    // Deprecated dangerous force unassign
    "preUnassign(org.apache.hadoop.hbase.coprocessor.ObserverContext,org.apache.hadoop.hbase.client.RegionInfo,boolean)",
    // --- Replication sink is a trusted internal cluster-to-cluster operation ---
    "preReplicationSinkBatchMutate"));

  private static final Class<?>[] OBSERVER_INTERFACES =
    { MasterObserver.class, RegionObserver.class, RegionServerObserver.class,
      EndpointObserver.class, BulkLoadObserver.class };

  /**
   * Returns true if the method matches a pattern rule that makes it safe to skip without an
   * explicit whitelist entry.
   */
  private static boolean matchesSkipPattern(Class<?> iface, Method m) {
    String name = m.getName();
    // All post* methods are post-operation notifications.
    // Permission checks must happen before the operation, not after.
    if (name.startsWith("post")) {
      return true;
    }
    // *Action suffix on pre* hooks are procedure-level callbacks.
    // Authorization is done at the RPC layer in the corresponding pre* hook.
    if (name.endsWith("Action")) {
      return true;
    }
    // RSGroup related access control is implemented separated in RSGroupAdminEndpoint
    if (name.contains("RSGroup")) {
      return true;
    }
    // RegionObserver internal storage hooks: flush, compaction, in-memory
    // compaction. These are sub-step callbacks within a region storage
    // operation. The region-level entry hook (preFlush, preCompact) already
    // handles authorization in AccessController.
    if (
      iface == RegionObserver.class && (name.startsWith("preFlush") || name.startsWith("preCompact")
        || name.startsWith("preMemStore"))
    ) {
      return true;
    }
    return false;
  }

  private static String methodSignatureKey(Method m) {
    String paramTypes = Arrays.stream(m.getParameterTypes()).map(Class::getSimpleName)
      .collect(Collectors.joining(","));
    return m.getName() + "(" + paramTypes + ")";
  }

  private static String fullMethodSignatureKey(Method m) {
    String paramTypes =
      Arrays.stream(m.getParameterTypes()).map(Class::getName).collect(Collectors.joining(","));
    return m.getName() + "(" + paramTypes + ")";
  }

  private static boolean isMethodImplemented(Class<?> implClass, Method ifaceMethod) {
    Class<?> clazz = implClass;
    while (clazz != null) {
      for (Method m : clazz.getDeclaredMethods()) {
        if (
          m.getName().equals(ifaceMethod.getName())
            && Arrays.equals(m.getParameterTypes(), ifaceMethod.getParameterTypes())
        ) {
          return true;
        }
      }
      clazz = clazz.getSuperclass();
    }
    return false;
  }

  @Test
  public void testAllObserverMethodsAreImplemented() {
    Set<String> missing = new TreeSet<>();

    for (Class<?> iface : OBSERVER_INTERFACES) {
      for (Method m : iface.getMethods()) {
        if (m.getDeclaringClass() == Object.class) {
          continue;
        }
        if (!m.getDeclaringClass().equals(iface)) {
          continue;
        }

        if (matchesSkipPattern(iface, m)) {
          continue;
        }

        String simpleName = m.getName();
        String simpleKey = methodSignatureKey(m);
        String fullKey = fullMethodSignatureKey(m);

        if (
          WHITELIST.contains(simpleName) || WHITELIST.contains(simpleKey)
            || WHITELIST.contains(fullKey)
        ) {
          continue;
        }

        if (!isMethodImplemented(AccessController.class, m)) {
          missing.add("  " + iface.getSimpleName() + "." + simpleKey);
        }
      }
    }

    if (!missing.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("AccessController does not implement the following observer methods.\n");
      sb.append("Either override them in AccessController (with permission checks),\n");
      sb.append("or add them to the WHITELIST with a justification comment.\n\n");
      sb.append("Missing methods:\n");
      missing.forEach(m -> sb.append(m).append("\n"));
      fail(sb.toString());
    }
  }
}

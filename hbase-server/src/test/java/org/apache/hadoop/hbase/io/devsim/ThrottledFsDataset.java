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
package org.apache.hadoop.hbase.io.devsim;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic proxy wrapping a real {@code FsDatasetImpl} to apply EBS volume emulation at the DataNode
 * storage level. One proxy is created per DataNode by {@link ThrottledFsDatasetFactory}.
 * <p>
 * Models the Linux ext4 page cache behavior: application writes go to the page cache with no EBS
 * charge. EBS is engaged only when dirty pages are flushed to the block device, which happens at
 * two interception points on {@code FsDatasetSpi}:
 * <ul>
 * <li>{@code getBlockInputStream(ExtendedBlock, long)} -- wraps the returned InputStream with
 * {@link ThrottledBlockInputStream} for per-byte read throttling
 * <li>{@code submitBackgroundSyncFileRangeRequest(ExtendedBlock, ..., nbytes, ...)} -- charges
 * {@code nbytes} against the volume's BW and IOPS budgets, modeling the async writeback of dirty
 * pages triggered by {@code sync_file_range(SYNC_FILE_RANGE_WRITE)} every ~8MB
 * <li>{@code finalizeBlock(ExtendedBlock, boolean)} -- charges the remaining unflushed bytes (block
 * size minus bytes already charged via sync_file_range) against the volume's budgets, modeling the
 * {@code fsync()} at block finalization
 * </ul>
 * Cumulative synced bytes are tracked per block to avoid double-counting between the intermediate
 * sync_file_range charges and the final fsync charge.
 * <p>
 * All other methods are delegated transparently to the inner {@code FsDatasetImpl}.
 */
public final class ThrottledFsDataset {

  private static final Logger LOG = LoggerFactory.getLogger(ThrottledFsDataset.class);

  private ThrottledFsDataset() {
  }

  /**
   * Create a dynamic proxy wrapping the given {@code FsDatasetSpi} delegate with EBS throttling.
   * @param delegate the real FsDatasetImpl
   * @param dnId     DataNode identifier for metrics registration
   * @param conf     Hadoop configuration with EBS parameters
   * @return a proxy implementing FsDatasetSpi with throttling
   */
  @SuppressWarnings("unchecked")
  public static <T extends FsDatasetSpi<?>> T wrap(FsDatasetSpi<?> delegate, String dnId,
    Configuration conf) {

    long bytesPerSec = conf.getLong(EBSDevice.IO_BUDGET_BYTES_PER_SEC_KEY, 0);
    int iops = conf.getInt(EBSDevice.IO_BUDGET_IOPS_KEY, 0);
    int windowMs = conf.getInt(EBSDevice.IO_BUDGET_WINDOW_MS_KEY, EBSDevice.DEFAULT_WINDOW_MS);
    int maxIoKb = conf.getInt(EBSDevice.IO_MAX_IO_SIZE_KB_KEY, EBSDevice.DEFAULT_MAX_IO_SIZE_KB);
    int maxIoSizeBytes = maxIoKb * 1024;
    int instMbps = conf.getInt(EBSDevice.IO_INSTANCE_MBPS_KEY, EBSDevice.DEFAULT_INSTANCE_MBPS);
    int deviceLatencyUs =
      conf.getInt(EBSDevice.IO_DEVICE_LATENCY_US_KEY, EBSDevice.DEFAULT_DEVICE_LATENCY_US);

    List<FsVolumeSpi> realVolumes = new ArrayList<>();
    try (FsDatasetSpi.FsVolumeReferences refs = delegate.getFsVolumeReferences()) {
      for (FsVolumeSpi v : refs) {
        realVolumes.add(v);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to enumerate volumes from delegate dataset", e);
    }

    int numVolumes = realVolumes.size();
    EBSVolumeDevice[] ebsVolumes = new EBSVolumeDevice[numVolumes];
    Map<String, EBSVolumeDevice> storageIdToVolume = new HashMap<>();
    for (int i = 0; i < numVolumes; i++) {
      IOBudget volBw = bytesPerSec > 0 ? new IOBudget(bytesPerSec, windowMs) : null;
      IOBudget volIops = iops > 0 ? new IOBudget(iops, windowMs) : null;
      ebsVolumes[i] = new EBSVolumeDevice(i, volBw, volIops, maxIoSizeBytes, deviceLatencyUs);
      String storageId = realVolumes.get(i).getStorageID();
      storageIdToVolume.put(storageId, ebsVolumes[i]);
    }

    IOBudget instanceBw = null;
    if (instMbps > 0) {
      long instBytesPerSec = (long) instMbps * 1024L * 1024L;
      instanceBw = new IOBudget(instBytesPerSec, windowMs);
    }

    EBSDevice.DataNodeContext dnContext =
      EBSDevice.register(dnId, ebsVolumes, instanceBw, bytesPerSec, iops, deviceLatencyUs);

    int reportIntervalSec = conf.getInt(EBSDevice.IO_BUDGET_REPORT_INTERVAL_SEC_KEY,
      EBSDevice.DEFAULT_REPORT_INTERVAL_SEC);
    EBSDevice.startMetricsReporter(reportIntervalSec);

    LOG.info(
      "ThrottledFsDataset: DN {} wrapped with {} EBS volumes "
        + "(BW={} MB/s, IOPS={}, coalesce={}KB, latency={}us, instance_cap={} MB/s)",
      dnId, numVolumes,
      bytesPerSec > 0 ? String.format("%.1f", bytesPerSec / (1024.0 * 1024.0)) : "unlimited",
      iops > 0 ? iops : "unlimited", maxIoKb > 0 ? maxIoKb : "off",
      deviceLatencyUs > 0 ? deviceLatencyUs : "off", instMbps > 0 ? instMbps : "unlimited");

    String[] interceptedMethods =
      { "getBlockInputStream", "submitBackgroundSyncFileRangeRequest", "finalizeBlock" };
    for (String m : interceptedMethods) {
      boolean found = false;
      for (Method dm : delegate.getClass().getMethods()) {
        if (dm.getName().equals(m)) {
          found = true;
          break;
        }
      }
      if (!found) {
        LOG.warn("EBS interception target method '{}' not found on {}; "
          + "throttling for this path will be inactive", m, delegate.getClass().getName());
      }
    }

    EBSInvocationHandler handler = new EBSInvocationHandler(delegate, storageIdToVolume, dnContext);

    Set<Class<?>> interfaces = new HashSet<>();
    collectInterfaces(delegate.getClass(), interfaces);
    // Ensure FsDatasetSpi is always included
    interfaces.add(FsDatasetSpi.class);

    return (T) Proxy.newProxyInstance(delegate.getClass().getClassLoader(),
      interfaces.toArray(new Class<?>[0]), handler);
  }

  private static void collectInterfaces(Class<?> clazz, Set<Class<?>> result) {
    if (clazz == null || clazz == Object.class) {
      return;
    }
    for (Class<?> iface : clazz.getInterfaces()) {
      if (result.add(iface)) {
        collectInterfaces(iface, result);
      }
    }
    collectInterfaces(clazz.getSuperclass(), result);
  }

  private static class EBSInvocationHandler implements InvocationHandler {

    private final FsDatasetSpi<?> delegate;
    private final Map<String, EBSVolumeDevice> storageIdToVolume;
    private final EBSDevice.DataNodeContext dnContext;
    private final ConcurrentHashMap<Long, Long> syncedBytesPerBlock = new ConcurrentHashMap<>();

    EBSInvocationHandler(FsDatasetSpi<?> delegate, Map<String, EBSVolumeDevice> storageIdToVolume,
      EBSDevice.DataNodeContext dnContext) {
      this.delegate = delegate;
      this.storageIdToVolume = storageIdToVolume;
      this.dnContext = dnContext;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();

      // Read path: wrap the block input stream with throttling
      if (
        "getBlockInputStream".equals(name) && args != null && args.length >= 2
          && args[0] instanceof ExtendedBlock
      ) {
        return handleGetBlockInputStream(method, args);
      }

      // Write path: intermediate async writeback of dirty pages (sync_file_range)
      if (
        "submitBackgroundSyncFileRangeRequest".equals(name) && args != null && args.length >= 4
          && args[0] instanceof ExtendedBlock
      ) {
        return handleSyncFileRange(method, args);
      }

      // Write path: final fsync at block finalization (charge remaining unflushed bytes)
      if (
        "finalizeBlock".equals(name) && args != null && args.length >= 1
          && args[0] instanceof ExtendedBlock
      ) {
        return handleFinalizeBlock(method, args);
      }

      // Object methods
      if ("toString".equals(name) && (args == null || args.length == 0)) {
        return "ThrottledFsDataset[" + delegate.toString() + "]";
      }
      if ("hashCode".equals(name) && (args == null || args.length == 0)) {
        return System.identityHashCode(proxy);
      }
      if ("equals".equals(name) && args != null && args.length == 1) {
        return proxy == args[0];
      }

      return delegateInvoke(method, args);
    }

    private Object handleGetBlockInputStream(Method method, Object[] args) throws Throwable {
      ExtendedBlock block = (ExtendedBlock) args[0];
      long offset = (Long) args[1];
      EBSDevice.recordReadIntercept();

      Object result = delegateInvoke(method, args);
      if (result instanceof InputStream) {
        EBSVolumeDevice vol = resolveVolume(block);
        if (vol != null) {
          return new ThrottledBlockInputStream((InputStream) result, vol, dnContext, offset);
        }
        EBSDevice.recordUnresolvedVolume();
      }
      return result;
    }

    /**
     * Intercepts the DataNode's async dirty page writeback. In production, BlockReceiver calls this
     * every ~8MB ({@code CACHE_DROP_LAG_BYTES}) to issue
     * {@code sync_file_range(fd, offset, nbytes, SYNC_FILE_RANGE_WRITE)}, which triggers
     * asynchronous writeback of dirty pages from the page cache to the EBS device.
     * <p>
     * We charge {@code nbytes} against the volume's BW and IOPS budgets here, and track the
     * cumulative synced bytes per block so that {@code finalizeBlock} only charges the remaining
     * unflushed delta.
     */
    private Object handleSyncFileRange(Method method, Object[] args) throws Throwable {
      ExtendedBlock block = (ExtendedBlock) args[0];
      // args: (ExtendedBlock, ReplicaOutputStreams, long offset, long nbytes, int flags)
      long nbytes = args[3] instanceof Number ? ((Number) args[3]).longValue() : 0;
      EBSDevice.recordWriteIntercept();
      EBSVolumeDevice vol = resolveVolume(block);
      if (vol != null && nbytes > 0) {
        vol.accountBulkWrite(nbytes);
        dnContext.consumeInstanceBw(nbytes);
        syncedBytesPerBlock.merge(block.getBlockId(), nbytes, Long::sum);
      } else if (vol == null) {
        EBSDevice.recordUnresolvedVolume();
      }
      return delegateInvoke(method, args);
    }

    /**
     * Intercepts block finalization, which in the DataNode follows immediately after
     * {@code BlockReceiver.close()} (which includes {@code syncDataOut()} / fsync). Charges only
     * the remaining bytes not already flushed via {@code sync_file_range} calls.
     */
    private Object handleFinalizeBlock(Method method, Object[] args) throws Throwable {
      ExtendedBlock block = (ExtendedBlock) args[0];
      EBSDevice.recordWriteIntercept();
      EBSVolumeDevice vol = resolveVolume(block);
      if (vol != null) {
        long blockBytes = block.getNumBytes();
        long alreadySynced = syncedBytesPerBlock.getOrDefault(block.getBlockId(), 0L);
        long remaining = Math.max(0, blockBytes - alreadySynced);
        if (remaining > 0) {
          vol.accountBulkWrite(remaining);
          dnContext.consumeInstanceBw(remaining);
        }
        syncedBytesPerBlock.remove(block.getBlockId());
      } else {
        EBSDevice.recordUnresolvedVolume();
      }
      return delegateInvoke(method, args);
    }

    private EBSVolumeDevice resolveVolume(ExtendedBlock block) {
      try {
        FsVolumeSpi vol = delegate.getVolume(block);
        if (vol != null) {
          return storageIdToVolume.get(vol.getStorageID());
        }
      } catch (Exception e) {
        LOG.debug("Could not resolve volume for block {}", block, e);
      }
      return null;
    }

    private Object delegateInvoke(Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}

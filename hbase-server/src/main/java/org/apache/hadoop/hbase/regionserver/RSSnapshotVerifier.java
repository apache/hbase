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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil.StoreFileVisitor;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;

/**
 *  Used by {@link org.apache.hadoop.hbase.master.procedure.SnapshotVerifyProcedure} to verify
 *  if the region info and store file info in RegionManifest are intact.
 */
@InterfaceAudience.Private
public class RSSnapshotVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(RSSnapshotVerifier.class);

  private final LoadingCache<SnapshotDescription,
    Pair<FileSystem, Map<String, SnapshotRegionManifest>>> SNAPSHOT_MANIFEST_CACHE;
  private final Configuration conf;

  public RSSnapshotVerifier(Configuration conf) {
    this.conf = conf;
    long expiredTime = conf.getLong("hbase.snapshot-manifest.cache.expired.sec", 600);
    long maxSize = conf.getLong("hbase.snapshot-manifest.cache.max.size", 10);
    this.SNAPSHOT_MANIFEST_CACHE = CacheBuilder.newBuilder()
      .expireAfterAccess(expiredTime, TimeUnit.SECONDS).maximumSize(maxSize)
      .build(new SnapshotManifestCacheLoader(conf));
  }

  public void verifyRegion(SnapshotDescription snapshot, RegionInfo region) throws IOException {
    try {
      Pair<FileSystem, Map<String, SnapshotRegionManifest>>
        cache = SNAPSHOT_MANIFEST_CACHE.get(snapshot);
      Map<String, SnapshotRegionManifest> rmMap = cache.getSecond();
      if (rmMap == null) {
        throw new CorruptedSnapshotException(snapshot.getName() + "looks empty");
      }
      SnapshotRegionManifest regionManifest = rmMap.get(region.getEncodedName());
      if (regionManifest == null) {
        LOG.warn("No snapshot region directory found for {}", region.getRegionNameAsString());
        return;
      }
      // verify region info
      RegionInfo manifestRegionInfo = ProtobufUtil.toRegionInfo(regionManifest.getRegionInfo());
      if (RegionInfo.COMPARATOR.compare(region, manifestRegionInfo) != 0) {
        String msg =
          "Manifest region info " + manifestRegionInfo + "doesn't match expected region:" + region;
        throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
      }
      // verify store file
      SnapshotReferenceUtil.visitRegionStoreFiles(regionManifest, new StoreFileVisitor() {
        @Override public void storeFile(RegionInfo region, String familyName, StoreFile storeFile)
            throws IOException {
          SnapshotReferenceUtil.verifyStoreFile(conf, cache.getFirst(),
            /* snapshotDir= */ null, // snapshotDir is never used, so it's ok to pass null here.
                                     // maybe we can remove this parameter later.
            snapshot, region, familyName, storeFile);
        }
      });
    } catch (ExecutionException e) {
      if (e.getCause() instanceof CorruptedSnapshotException) {
        throw new CorruptedSnapshotException(e.getCause().getMessage(),
          ProtobufUtil.createSnapshotDesc(snapshot));
      } else {
        LOG.error("Failed loading snapshot manifest for {} from filesystem",
          snapshot.getName(), e.getCause());
        throw new IOException(e.getCause());
      }
    }
  }

  // to avoid loading snapshot manifest from filesystem for each region, try to cache it here
  private static final class SnapshotManifestCacheLoader extends
      CacheLoader<SnapshotDescription, Pair<FileSystem, Map<String, SnapshotRegionManifest>>> {
    private final Configuration conf;

    private SnapshotManifestCacheLoader(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Pair<FileSystem, Map<String, SnapshotRegionManifest>>
        load(SnapshotDescription snapshot) throws Exception {
      Path rootDir = CommonFSUtils.getRootDir(conf);
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir, conf);
      FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf);
      FileSystem workingDirFS = workingDir.getFileSystem(conf);
      SnapshotManifest manifest = SnapshotManifest.open(conf, workingDirFS, workingDir, snapshot);
      LOG.debug("loading snapshot manifest for {} from {}", snapshot.getName(), workingDir);
      return Pair.newPair(rootFs, manifest.getRegionManifestsMap());
    }
  }
}

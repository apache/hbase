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
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegion.BulkLoadListener;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.ClientTokenUtil;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;

/**
 * Bulk loads in secure mode.
 *
 * This service addresses two issues:
 * <ol>
 * <li>Moving files in a secure filesystem wherein the HBase Client
 * and HBase Server are different filesystem users.</li>
 * <li>Does moving in a secure manner. Assuming that the filesystem
 * is POSIX compliant.</li>
 * </ol>
 *
 * The algorithm is as follows:
 * <ol>
 * <li>Create an hbase owned staging directory which is
 * world traversable (711): {@code /hbase/staging}</li>
 * <li>A user writes out data to his secure output directory: {@code /user/foo/data}</li>
 * <li>A call is made to hbase to create a secret staging directory
 * which globally rwx (777): {@code /user/staging/averylongandrandomdirectoryname}</li>
 * <li>The user moves the data into the random staging directory,
 * then calls bulkLoadHFiles()</li>
 * </ol>
 *
 * Like delegation tokens the strength of the security lies in the length
 * and randomness of the secret directory.
 *
 */
@InterfaceAudience.Private
public class SecureBulkLoadManager {

  public static final long VERSION = 0L;

  //320/5 = 64 characters
  private static final int RANDOM_WIDTH = 320;
  private static final int RANDOM_RADIX = 32;

  private static final Logger LOG = LoggerFactory.getLogger(SecureBulkLoadManager.class);

  private final static FsPermission PERM_ALL_ACCESS = FsPermission.valueOf("-rwxrwxrwx");
  private final static FsPermission PERM_HIDDEN = FsPermission.valueOf("-rwx--x--x");
  private SecureRandom random;
  private FileSystem fs;
  private Configuration conf;

  //two levels so it doesn't get deleted accidentally
  //no sticky bit in Hadoop 1.0
  private Path baseStagingDir;

  private UserProvider userProvider;
  private ConcurrentHashMap<UserGroupInformation, MutableInt> ugiReferenceCounter;
  private AsyncConnection conn;

  SecureBulkLoadManager(Configuration conf, AsyncConnection conn) {
    this.conf = conf;
    this.conn = conn;
  }

  public void start() throws IOException {
    random = new SecureRandom();
    userProvider = UserProvider.instantiate(conf);
    ugiReferenceCounter = new ConcurrentHashMap<>();
    fs = FileSystem.get(conf);
    baseStagingDir = new Path(CommonFSUtils.getRootDir(conf), HConstants.BULKLOAD_STAGING_DIR_NAME);

    if (conf.get("hbase.bulkload.staging.dir") != null) {
      LOG.warn("hbase.bulkload.staging.dir " + " is deprecated. Bulkload staging directory is "
          + baseStagingDir);
    }
    if (!fs.exists(baseStagingDir)) {
      fs.mkdirs(baseStagingDir, PERM_HIDDEN);
    }
  }

  public void stop() throws IOException {
  }

  public String prepareBulkLoad(final HRegion region, final PrepareBulkLoadRequest request)
      throws IOException {
    User user = getActiveUser();
    region.getCoprocessorHost().prePrepareBulkLoad(user);

    String bulkToken =
        createStagingDir(baseStagingDir, user, region.getTableDescriptor().getTableName())
            .toString();

    return bulkToken;
  }

  public void cleanupBulkLoad(final HRegion region, final CleanupBulkLoadRequest request)
      throws IOException {
    region.getCoprocessorHost().preCleanupBulkLoad(getActiveUser());

    Path path = new Path(request.getBulkToken());
    if (!fs.delete(path, true)) {
      if (fs.exists(path)) {
        throw new IOException("Failed to clean up " + path);
      }
    }
    LOG.trace("Cleaned up {} successfully.", path);
  }

  private Consumer<HRegion> fsCreatedListener;

  void setFsCreatedListener(Consumer<HRegion> fsCreatedListener) {
    this.fsCreatedListener = fsCreatedListener;
  }

  private void incrementUgiReference(UserGroupInformation ugi) {
    // if we haven't seen this ugi before, make a new counter
    ugiReferenceCounter.compute(ugi, (key, value) -> {
      if (value == null) {
        value = new MutableInt(1);
      } else {
        value.increment();
      }
      return value;
    });
  }

  private void decrementUgiReference(UserGroupInformation ugi) {
    // if the count drops below 1 we remove the entry by returning null
    ugiReferenceCounter.computeIfPresent(ugi, (key, value) -> {
      if (value.intValue() > 1) {
        value.decrement();
      } else {
        value = null;
      }
      return value;
    });
  }

  private boolean isUserReferenced(UserGroupInformation ugi) {
    // if the ugi is in the map, based on invariants above
    // the count must be above zero
    return ugiReferenceCounter.containsKey(ugi);
  }

  public Map<byte[], List<Path>> secureBulkLoadHFiles(final HRegion region,
    final BulkLoadHFileRequest request) throws IOException {
    return secureBulkLoadHFiles(region, request, null);
  }

  public Map<byte[], List<Path>> secureBulkLoadHFiles(final HRegion region,
      final BulkLoadHFileRequest request, List<String> clusterIds) throws IOException {
    final List<Pair<byte[], String>> familyPaths = new ArrayList<>(request.getFamilyPathCount());
    for(ClientProtos.BulkLoadHFileRequest.FamilyPath el : request.getFamilyPathList()) {
      familyPaths.add(new Pair<>(el.getFamily().toByteArray(), el.getPath()));
    }

    Token<AuthenticationTokenIdentifier> userToken = null;
    if (userProvider.isHadoopSecurityEnabled()) {
      userToken = new Token<>(request.getFsToken().getIdentifier().toByteArray(),
        request.getFsToken().getPassword().toByteArray(), new Text(request.getFsToken().getKind()),
        new Text(request.getFsToken().getService()));
    }
    final String bulkToken = request.getBulkToken();
    User user = getActiveUser();
    final UserGroupInformation ugi = user.getUGI();
    if (userProvider.isHadoopSecurityEnabled()) {
      try {
        Token<AuthenticationTokenIdentifier> tok = ClientTokenUtil.obtainToken(conn).get();
        if (tok != null) {
          boolean b = ugi.addToken(tok);
          LOG.debug("token added " + tok + " for user " + ugi + " return=" + b);
        }
      } catch (Exception ioe) {
        LOG.warn("unable to add token", ioe);
      }
    }
    if (userToken != null) {
      ugi.addToken(userToken);
    } else if (userProvider.isHadoopSecurityEnabled()) {
      //we allow this to pass through in "simple" security mode
      //for mini cluster testing
      throw new DoNotRetryIOException("User token cannot be null");
    }

    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
    }
    Map<byte[], List<Path>> map = null;

    try {
      incrementUgiReference(ugi);
      // Get the target fs (HBase region server fs) delegation token
      // Since we have checked the permission via 'preBulkLoadHFile', now let's give
      // the 'request user' necessary token to operate on the target fs.
      // After this point the 'doAs' user will hold two tokens, one for the source fs
      // ('request user'), another for the target fs (HBase region server principal).
      if (userProvider.isHadoopSecurityEnabled()) {
        FsDelegationToken targetfsDelegationToken = new FsDelegationToken(userProvider,"renewer");
        targetfsDelegationToken.acquireDelegationToken(fs);

        Token<?> targetFsToken = targetfsDelegationToken.getUserToken();
        if (targetFsToken != null
            && (userToken == null || !targetFsToken.getService().equals(userToken.getService()))){
          ugi.addToken(targetFsToken);
        }
      }

      map = ugi.doAs(new PrivilegedAction<Map<byte[], List<Path>>>() {
        @Override
        public Map<byte[], List<Path>> run() {
          FileSystem fs = null;
          try {
            /*
             * This is creating and caching a new FileSystem instance. Other code called
             * "beneath" this method will rely on this FileSystem instance being in the
             * cache. This is important as those methods make _no_ attempt to close this
             * FileSystem instance. It is critical that here, in SecureBulkLoadManager,
             * we are tracking the lifecycle and closing the FS when safe to do so.
             */
            fs = FileSystem.get(conf);
            for(Pair<byte[], String> el: familyPaths) {
              Path stageFamily = new Path(bulkToken, Bytes.toString(el.getFirst()));
              if(!fs.exists(stageFamily)) {
                fs.mkdirs(stageFamily);
                fs.setPermission(stageFamily, PERM_ALL_ACCESS);
              }
            }
            if (fsCreatedListener != null) {
              fsCreatedListener.accept(region);
            }
            //We call bulkLoadHFiles as requesting user
            //To enable access prior to staging
            return region.bulkLoadHFiles(familyPaths, true,
                new SecureBulkLoadListener(fs, bulkToken, conf), request.getCopyFile(),
              clusterIds, request.getReplicate());
          } catch (Exception e) {
            LOG.error("Failed to complete bulk load", e);
          }
          return null;
        }
      });
    } finally {
      decrementUgiReference(ugi);
      try {
        if (!UserGroupInformation.getLoginUser().equals(ugi) && !isUserReferenced(ugi)) {
          FileSystem.closeAllForUGI(ugi);
        }
      } catch (IOException e) {
        LOG.error("Failed to close FileSystem for: {}", ugi, e);
      }
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postBulkLoadHFile(familyPaths, map);
      }
    }
    return map;
  }

  private Path createStagingDir(Path baseDir,
                                User user,
                                TableName tableName) throws IOException {
    String tblName = tableName.getNameAsString().replace(":", "_");
    String randomDir = user.getShortName()+"__"+ tblName +"__"+
        (new BigInteger(RANDOM_WIDTH, random).toString(RANDOM_RADIX));
    return createStagingDir(baseDir, user, randomDir);
  }

  private Path createStagingDir(Path baseDir,
                                User user,
                                String randomDir) throws IOException {
    Path p = new Path(baseDir, randomDir);
    fs.mkdirs(p, PERM_ALL_ACCESS);
    fs.setPermission(p, PERM_ALL_ACCESS);
    return p;
  }

  private User getActiveUser() throws IOException {
    // for non-rpc handling, fallback to system user
    User user = RpcServer.getRequestUser().orElse(userProvider.getCurrent());
    // this is for testing
    if (userProvider.isHadoopSecurityEnabled() &&
        "simple".equalsIgnoreCase(conf.get(User.HBASE_SECURITY_CONF_KEY))) {
      return User.createUserForTesting(conf, user.getShortName(), new String[] {});
    }

    return user;
  }

  //package-private for test purpose only
  static class SecureBulkLoadListener implements BulkLoadListener {
    // Target filesystem
    private final FileSystem fs;
    private final String stagingDir;
    private final Configuration conf;
    // Source filesystem
    private FileSystem srcFs = null;
    private Map<String, FsPermission> origPermissions = null;
    private Map<String, String> origSources = null;

    public SecureBulkLoadListener(FileSystem fs, String stagingDir, Configuration conf) {
      this.fs = fs;
      this.stagingDir = stagingDir;
      this.conf = conf;
      this.origPermissions = new HashMap<>();
      this.origSources = new HashMap<>();
    }

    @Override
    public String prepareBulkLoad(final byte[] family, final String srcPath, boolean copyFile,
      String customStaging ) throws IOException {
      Path p = new Path(srcPath);

      //store customStaging for failedBulkLoad
      String currentStaging = stagingDir;
      if(StringUtils.isNotEmpty(customStaging)){
        currentStaging = customStaging;
      }

      Path stageP = new Path(currentStaging, new Path(Bytes.toString(family), p.getName()));

      // In case of Replication for bulk load files, hfiles are already copied in staging directory
      if (p.equals(stageP)) {
        LOG.debug(p.getName()
            + " is already available in staging directory. Skipping copy or rename.");
        return stageP.toString();
      }

      if (srcFs == null) {
        srcFs = FileSystem.newInstance(p.toUri(), conf);
      }

      if(!isFile(p)) {
        throw new IOException("Path does not reference a file: " + p);
      }

      // Check to see if the source and target filesystems are the same
      if (!FSUtils.isSameHdfs(conf, srcFs, fs)) {
        LOG.debug("Bulk-load file " + srcPath + " is on different filesystem than " +
            "the destination filesystem. Copying file over to destination staging dir.");
        FileUtil.copy(srcFs, p, fs, stageP, false, conf);
      } else if (copyFile) {
        LOG.debug("Bulk-load file " + srcPath + " is copied to destination staging dir.");
        FileUtil.copy(srcFs, p, fs, stageP, false, conf);
      } else {
        LOG.debug("Moving " + p + " to " + stageP);
        FileStatus origFileStatus = fs.getFileStatus(p);
        origPermissions.put(srcPath, origFileStatus.getPermission());
        origSources.put(stageP.toString(), srcPath);
        if(!fs.rename(p, stageP)) {
          throw new IOException("Failed to move HFile: " + p + " to " + stageP);
        }
      }

      if(StringUtils.isNotEmpty(customStaging)) {
        fs.setPermission(stageP, PERM_ALL_ACCESS);
      }

      return stageP.toString();
    }

    @Override
    public void doneBulkLoad(byte[] family, String srcPath) throws IOException {
      LOG.debug("Bulk Load done for: " + srcPath);
      closeSrcFs();
    }

    private void closeSrcFs() throws IOException {
      if (srcFs != null) {
        srcFs.close();
        srcFs = null;
      }
    }

    @Override
    public void failedBulkLoad(final byte[] family, final String stagedPath) throws IOException {
      try {
        String src = origSources.get(stagedPath);
        if(StringUtils.isEmpty(src)){
          LOG.debug(stagedPath + " was not moved to staging. No need to move back");
          return;
        }

        Path stageP = new Path(stagedPath);
        if (!fs.exists(stageP)) {
          throw new IOException(
            "Missing HFile: " + stageP + ", can't be moved back to it's original place");
        }

        //we should not move back files if the original exists
        Path srcPath = new Path(src);
        if(srcFs.exists(srcPath)) {
          LOG.debug(src + " is already at it's original place. No need to move.");
          return;
        }

        LOG.debug("Moving " + stageP + " back to " + srcPath);
        if (!fs.rename(stageP, srcPath)) {
          throw new IOException("Failed to move HFile: " + stageP + " to " + srcPath);
        }

        // restore original permission
        if (origPermissions.containsKey(stagedPath)) {
          fs.setPermission(srcPath, origPermissions.get(src));
        } else {
          LOG.warn("Can't find previous permission for path=" + stagedPath);
        }
      } finally {
        closeSrcFs();
      }
    }

    /**
     * Check if the path is referencing a file.
     * This is mainly needed to avoid symlinks.
     * @param p
     * @return true if the p is a file
     * @throws IOException
     */
    private boolean isFile(Path p) throws IOException {
      FileStatus status = srcFs.getFileStatus(p);
      boolean isFile = !status.isDirectory();
      try {
        isFile = isFile && !(Boolean)Methods.call(FileStatus.class, status, "isSymlink", null, null);
      } catch (Exception e) {
      }
      return isFile;
    }
  }
}

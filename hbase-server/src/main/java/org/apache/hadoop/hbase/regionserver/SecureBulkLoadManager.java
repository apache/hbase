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

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.regionserver.Region.BulkLoadListener;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  private static final Log LOG = LogFactory.getLog(SecureBulkLoadManager.class);

  private final static FsPermission PERM_ALL_ACCESS = FsPermission.valueOf("-rwxrwxrwx");
  private final static FsPermission PERM_HIDDEN = FsPermission.valueOf("-rwx--x--x");
  private SecureRandom random;
  private FileSystem fs;
  private Configuration conf;

  //two levels so it doesn't get deleted accidentally
  //no sticky bit in Hadoop 1.0
  private Path baseStagingDir;

  private UserProvider userProvider;

  SecureBulkLoadManager(Configuration conf) {
    this.conf = conf;
  }

  public void start() throws IOException {
    random = new SecureRandom();
    userProvider = UserProvider.instantiate(conf);
    fs = FileSystem.get(conf);
    baseStagingDir = new Path(FSUtils.getRootDir(conf), HConstants.BULKLOAD_STAGING_DIR_NAME);

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

  public String prepareBulkLoad(final Region region, final PrepareBulkLoadRequest request)
      throws IOException {
    List<BulkLoadObserver> bulkLoadObservers = getBulkLoadObservers(region);

    if (bulkLoadObservers != null && bulkLoadObservers.size() != 0) {
      ObserverContext<RegionCoprocessorEnvironment> ctx =
          new ObserverContext<RegionCoprocessorEnvironment>(getActiveUser());
      ctx.prepare((RegionCoprocessorEnvironment) region.getCoprocessorHost()
          .findCoprocessorEnvironment(BulkLoadObserver.class).get(0));

      for (BulkLoadObserver bulkLoadObserver : bulkLoadObservers) {
        bulkLoadObserver.prePrepareBulkLoad(ctx, request);
      }
    }

    String bulkToken =
        createStagingDir(baseStagingDir, getActiveUser(), region.getTableDesc().getTableName())
            .toString();

    return bulkToken;
  }

  public void cleanupBulkLoad(final Region region, final CleanupBulkLoadRequest request)
      throws IOException {
    List<BulkLoadObserver> bulkLoadObservers = getBulkLoadObservers(region);

    if (bulkLoadObservers != null && bulkLoadObservers.size() != 0) {
      ObserverContext<RegionCoprocessorEnvironment> ctx =
          new ObserverContext<RegionCoprocessorEnvironment>(getActiveUser());
      ctx.prepare((RegionCoprocessorEnvironment) region.getCoprocessorHost()
        .findCoprocessorEnvironment(BulkLoadObserver.class).get(0));

      for (BulkLoadObserver bulkLoadObserver : bulkLoadObservers) {
        bulkLoadObserver.preCleanupBulkLoad(ctx, request);
      }
    }

    fs.delete(new Path(request.getBulkToken()), true);
  }

  public boolean secureBulkLoadHFiles(final Region region,
      final BulkLoadHFileRequest request) throws IOException {
    final List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>(request.getFamilyPathCount());
    for(ClientProtos.BulkLoadHFileRequest.FamilyPath el : request.getFamilyPathList()) {
      familyPaths.add(new Pair<byte[], String>(el.getFamily().toByteArray(), el.getPath()));
    }

    Token userToken = null;
    if (userProvider.isHadoopSecurityEnabled()) {
      userToken = new Token(request.getFsToken().getIdentifier().toByteArray(), request.getFsToken()
              .getPassword().toByteArray(), new Text(request.getFsToken().getKind()), new Text(
              request.getFsToken().getService()));
    }
    final String bulkToken = request.getBulkToken();
    User user = getActiveUser();
    final UserGroupInformation ugi = user.getUGI();
    if(userToken != null) {
      ugi.addToken(userToken);
    } else if (userProvider.isHadoopSecurityEnabled()) {
      //we allow this to pass through in "simple" security mode
      //for mini cluster testing
      throw new DoNotRetryIOException("User token cannot be null");
    }

    boolean bypass = false;
    if (region.getCoprocessorHost() != null) {
        bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
    }
    boolean loaded = false;
    if (!bypass) {
      // Get the target fs (HBase region server fs) delegation token
      // Since we have checked the permission via 'preBulkLoadHFile', now let's give
      // the 'request user' necessary token to operate on the target fs.
      // After this point the 'doAs' user will hold two tokens, one for the source fs
      // ('request user'), another for the target fs (HBase region server principal).
      if (userProvider.isHadoopSecurityEnabled()) {
        FsDelegationToken targetfsDelegationToken = new FsDelegationToken(userProvider, "renewer");
        targetfsDelegationToken.acquireDelegationToken(fs);

        Token<?> targetFsToken = targetfsDelegationToken.getUserToken();
        if (targetFsToken != null
            && (userToken == null || !targetFsToken.getService().equals(userToken.getService()))) {
          ugi.addToken(targetFsToken);
        }
      }

      loaded = ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          FileSystem fs = null;
          try {
            fs = FileSystem.get(conf);
            for(Pair<byte[], String> el: familyPaths) {
              Path stageFamily = new Path(bulkToken, Bytes.toString(el.getFirst()));
              if(!fs.exists(stageFamily)) {
                fs.mkdirs(stageFamily);
                fs.setPermission(stageFamily, PERM_ALL_ACCESS);
              }
            }
            //We call bulkLoadHFiles as requesting user
            //To enable access prior to staging
            return region.bulkLoadHFiles(familyPaths, true,
                new SecureBulkLoadListener(fs, bulkToken, conf));
          } catch (Exception e) {
            LOG.error("Failed to complete bulk load", e);
          }
          return false;
        }
      });
    }
    if (region.getCoprocessorHost() != null) {
       loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
    }
    return loaded;
  }

  private List<BulkLoadObserver> getBulkLoadObservers(Region region) {
    List<BulkLoadObserver> coprocessorList =
        region.getCoprocessorHost().findCoprocessors(BulkLoadObserver.class);

    return coprocessorList;
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
    User user = RpcServer.getRequestUser();
    if (user == null) {
      // for non-rpc handling, fallback to system user
      user = userProvider.getCurrent();
    }

    //this is for testing
    if (userProvider.isHadoopSecurityEnabled()
        && "simple".equalsIgnoreCase(conf.get(User.HBASE_SECURITY_CONF_KEY))) {
      return User.createUserForTesting(conf, user.getShortName(), new String[]{});
    }

    return user;
  }

  private static class SecureBulkLoadListener implements BulkLoadListener {
    // Target filesystem
    private final FileSystem fs;
    private final String stagingDir;
    private final Configuration conf;
    // Source filesystem
    private FileSystem srcFs = null;
    private Map<String, FsPermission> origPermissions = null;

    public SecureBulkLoadListener(FileSystem fs, String stagingDir, Configuration conf) {
      this.fs = fs;
      this.stagingDir = stagingDir;
      this.conf = conf;
      this.origPermissions = new HashMap<String, FsPermission>();
    }

    @Override
    public String prepareBulkLoad(final byte[] family, final String srcPath) throws IOException {
      Path p = new Path(srcPath);
      Path stageP = new Path(stagingDir, new Path(Bytes.toString(family), p.getName()));

      // In case of Replication for bulk load files, hfiles are already copied in staging directory
      if (p.equals(stageP)) {
        LOG.debug(p.getName()
            + " is already available in staging directory. Skipping copy or rename.");
        return stageP.toString();
      }

      if (srcFs == null) {
        srcFs = FileSystem.get(p.toUri(), conf);
      }

      if(!isFile(p)) {
        throw new IOException("Path does not reference a file: " + p);
      }

      // Check to see if the source and target filesystems are the same
      if (!FSHDFSUtils.isSameHdfs(conf, srcFs, fs)) {
        LOG.debug("Bulk-load file " + srcPath + " is on different filesystem than " +
            "the destination filesystem. Copying file over to destination staging dir.");
        FileUtil.copy(srcFs, p, fs, stageP, false, conf);
      } else {
        LOG.debug("Moving " + p + " to " + stageP);
        FileStatus origFileStatus = fs.getFileStatus(p);
        origPermissions.put(srcPath, origFileStatus.getPermission());
        if(!fs.rename(p, stageP)) {
          throw new IOException("Failed to move HFile: " + p + " to " + stageP);
        }
      }
      fs.setPermission(stageP, PERM_ALL_ACCESS);
      return stageP.toString();
    }

    @Override
    public void doneBulkLoad(byte[] family, String srcPath) throws IOException {
      LOG.debug("Bulk Load done for: " + srcPath);
    }

    @Override
    public void failedBulkLoad(final byte[] family, final String srcPath) throws IOException {
      if (!FSHDFSUtils.isSameHdfs(conf, srcFs, fs)) {
        // files are copied so no need to move them back
        return;
      }
      Path p = new Path(srcPath);
      Path stageP = new Path(stagingDir,
          new Path(Bytes.toString(family), p.getName()));

      // In case of Replication for bulk load files, hfiles are not renamed by end point during
      // prepare stage, so no need of rename here again
      if (p.equals(stageP)) {
        LOG.debug(p.getName() + " is already available in source directory. Skipping rename.");
        return;
      }

      LOG.debug("Moving " + stageP + " back to " + p);
      if(!fs.rename(stageP, p))
        throw new IOException("Failed to move HFile: " + stageP + " to " + p);

      // restore original permission
      if (origPermissions.containsKey(srcPath)) {
        fs.setPermission(p, origPermissions.get(srcPath));
      } else {
        LOG.warn("Can't find previous permission for path=" + srcPath);
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

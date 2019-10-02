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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadService;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadHFilesRequest;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadHFilesResponse;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.BulkLoadListener;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.SecureBulkLoadUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Coprocessor service for bulk loads in secure mode.
 * This coprocessor has to be installed as part of enabling
 * security in HBase.
 *
 * This service addresses two issues:
 *
 * 1. Moving files in a secure filesystem wherein the HBase Client
 * and HBase Server are different filesystem users.
 * 2. Does moving in a secure manner. Assuming that the filesystem
 * is POSIX compliant.
 *
 * The algorithm is as follows:
 *
 * 1. Create an hbase owned staging directory which is
 * world traversable (711): /hbase/staging
 * 2. A user writes out data to his secure output directory: /user/foo/data
 * 3. A call is made to hbase to create a secret staging directory
 * which globally rwx (777): /user/staging/averylongandrandomdirectoryname
 * 4. The user moves the data into the random staging directory,
 * then calls bulkLoadHFiles()
 *
 * Like delegation tokens the strength of the security lies in the length
 * and randomness of the secret directory.
 *
 */
@InterfaceAudience.Private
public class SecureBulkLoadEndpoint extends SecureBulkLoadService
    implements CoprocessorService, Coprocessor {

  public static final long VERSION = 0L;

  //320/5 = 64 characters
  private static final int RANDOM_WIDTH = 320;
  private static final int RANDOM_RADIX = 32;

  private static final Log LOG = LogFactory.getLog(SecureBulkLoadEndpoint.class);

  private final static FsPermission PERM_ALL_ACCESS = FsPermission.valueOf("-rwxrwxrwx");
  private final static FsPermission PERM_HIDDEN = FsPermission.valueOf("-rwx--x--x");

  public static final String FS_WITHOUT_SUPPORT_PERMISSION_KEY =
      "hbase.secure.bulkload.fs.permission.lacking";
  public static final String FS_WITHOUT_SUPPORT_PERMISSION_DEFAULT =
      "s3,s3a,s3n,wasb,wasbs,swift,adfs,abfs,viewfs";

  private SecureRandom random;
  private FileSystem fs;
  private Configuration conf;

  //two levels so it doesn't get deleted accidentally
  //no sticky bit in Hadoop 1.0
  private Path baseStagingDir;

  private RegionCoprocessorEnvironment env;
  private Connection conn;

  private UserProvider userProvider;
  private static HashMap<UserGroupInformation, MutableInt> ugiReferenceCounter = new HashMap<>();

  @Override
  public void start(CoprocessorEnvironment env) {
    this.env = (RegionCoprocessorEnvironment)env;
    random = new SecureRandom();
    RegionServerServices svc = ((RegionCoprocessorEnvironment)env).getRegionServerServices();
    this.conn = svc.getConnection();
    conf = env.getConfiguration();
    baseStagingDir = SecureBulkLoadUtil.getBaseStagingDir(conf);
    this.userProvider = UserProvider.instantiate(conf);
    Set<String> fsSet = getFileSystemSchemesWithoutPermissionSupport(conf);

    try {
      fs = baseStagingDir.getFileSystem(conf);
      if (!fs.exists(baseStagingDir)) {
        fs.mkdirs(baseStagingDir, PERM_HIDDEN);
      }
      FileStatus status = fs.getFileStatus(baseStagingDir);
      if (status == null) {
        throw new IllegalStateException("Failed to create staging directory");
      }

      // If HDFS UMASK value has limited scope then staging directory permission may not be 711
      // after creation, so we should set staging directory permission explicitly.
      if (!status.getPermission().equals(PERM_HIDDEN)) {
        fs.setPermission(baseStagingDir, PERM_HIDDEN);
        status = fs.getFileStatus(baseStagingDir);
      }

      // no sticky bit in hadoop-1.0, making directory nonempty so it never gets erased
      Path doNotEraseDir = new Path(baseStagingDir, "DONOTERASE");
      if (!fs.exists(doNotEraseDir)) {
        fs.mkdirs(doNotEraseDir, PERM_HIDDEN);
        fs.setPermission(doNotEraseDir, PERM_HIDDEN);
      }

      String scheme = fs.getScheme().toLowerCase();
      if (!fsSet.contains(scheme) && !status.getPermission().equals(PERM_HIDDEN)) {
        throw new IllegalStateException(
            "Staging directory of " + baseStagingDir + " already exists but permissions aren't set to '-rwx--x--x' ");
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get FileSystem instance",e);
    }
  }

  Set<String> getFileSystemSchemesWithoutPermissionSupport(Configuration conf) {
    final String value = conf.get(
        FS_WITHOUT_SUPPORT_PERMISSION_KEY, FS_WITHOUT_SUPPORT_PERMISSION_DEFAULT);
    return new HashSet<String>(Arrays.asList(StringUtils.split(value, ',')));
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void prepareBulkLoad(RpcController controller, PrepareBulkLoadRequest request,
      RpcCallback<PrepareBulkLoadResponse> done) {
    try {
      List<BulkLoadObserver> bulkLoadObservers = getBulkLoadObservers();
      if (bulkLoadObservers != null) {
        ObserverContext<RegionCoprocessorEnvironment> ctx =
          new ObserverContext<RegionCoprocessorEnvironment>(RpcServer.getRequestUser());
        ctx.prepare(env);
        for (BulkLoadObserver bulkLoadObserver : bulkLoadObservers) {
          bulkLoadObserver.prePrepareBulkLoad(ctx, request);
        }
      }
      String bulkToken = createStagingDir(baseStagingDir, getActiveUser(),
        ProtobufUtil.toTableName(request.getTableName())).toString();
      done.run(PrepareBulkLoadResponse.newBuilder().setBulkToken(bulkToken).build());
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(null);
  }

  @Override
  public void cleanupBulkLoad(RpcController controller, CleanupBulkLoadRequest request,
      RpcCallback<CleanupBulkLoadResponse> done) {
    try {
      List<BulkLoadObserver> bulkLoadObservers = getBulkLoadObservers();
      if (bulkLoadObservers != null) {
        ObserverContext<RegionCoprocessorEnvironment> ctx =
          new ObserverContext<RegionCoprocessorEnvironment>(RpcServer.getRequestUser());
        ctx.prepare(env);
        for (BulkLoadObserver bulkLoadObserver : bulkLoadObservers) {
          bulkLoadObserver.preCleanupBulkLoad(ctx, request);
        }
      }
      Path path = new Path(request.getBulkToken());
      if (!fs.delete(path, true)) {
        if (fs.exists(path)) {
          throw new IOException("Failed to clean up " + path);
        }
      }
      LOG.info("Cleaned up " + path + " successfully.");
      done.run(CleanupBulkLoadResponse.newBuilder().build());
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      UserGroupInformation ugi = getActiveUser().getUGI();
      try {
        if (!UserGroupInformation.getLoginUser().equals(ugi) && !isUserReferenced(ugi)) {
          FileSystem.closeAllForUGI(ugi);
        }
      } catch (IOException e) {
        LOG.error("Failed to close FileSystem for: " + ugi, e);
      }
    }
    done.run(null);
  }

  @VisibleForTesting
  interface Consumer<T> {
    void accept(T t);
  }

  private static Consumer<Region> fsCreatedListener;

  @VisibleForTesting
  static void setFsCreatedListener(Consumer<Region> listener) {
    fsCreatedListener = listener;
  }

  private void incrementUgiReference(UserGroupInformation ugi) {
    synchronized (ugiReferenceCounter) {
      final MutableInt counter = ugiReferenceCounter.get(ugi);
      if (counter == null) {
        ugiReferenceCounter.put(ugi, new MutableInt(1));
      } else {
        counter.increment();
      }
    }
  }

  private void decrementUgiReference(UserGroupInformation ugi) {
    synchronized (ugiReferenceCounter) {
      final MutableInt counter = ugiReferenceCounter.get(ugi);
      if(counter == null || counter.intValue() <= 1) {
        ugiReferenceCounter.remove(ugi);
      } else {
        counter.decrement();
      }
    }
  }

  private boolean isUserReferenced(UserGroupInformation ugi) {
    synchronized (ugiReferenceCounter) {
      final MutableInt counter = ugiReferenceCounter.get(ugi);
      return counter != null && counter.intValue() > 0;
    }
  }

  @Override
  public void secureBulkLoadHFiles(RpcController controller,
                                   final SecureBulkLoadHFilesRequest request,
                                   RpcCallback<SecureBulkLoadHFilesResponse> done) {
    final List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
    for(ClientProtos.BulkLoadHFileRequest.FamilyPath el : request.getFamilyPathList()) {
      familyPaths.add(new Pair(el.getFamily().toByteArray(),el.getPath()));
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
    if (userProvider.isHadoopSecurityEnabled()) {
      try {
        Token tok = TokenUtil.obtainToken(conn);
        if (tok != null) {
          boolean b = ugi.addToken(tok);
          LOG.debug("extra token added " + tok + " ret=" + b);
        }
      } catch (IOException ioe) {
        LOG.warn("unable to add token", ioe);
      }
    }
    if (userToken != null) {
      ugi.addToken(userToken);
    } else if (userProvider.isHadoopSecurityEnabled()) {
      //we allow this to pass through in "simple" security mode
      //for mini cluster testing
      ResponseConverter.setControllerException(controller,
          new DoNotRetryIOException("User token cannot be null"));
      done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(false).build());
      return;
    }

    Region region = env.getRegion();
    boolean bypass = false;
    if (region.getCoprocessorHost() != null) {
      try {
        bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
      } catch (IOException e) {
        ResponseConverter.setControllerException(controller, e);
        done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(false).build());
        return;
      }
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
        try {
          targetfsDelegationToken.acquireDelegationToken(fs);
        } catch (IOException e) {
          ResponseConverter.setControllerException(controller, e);
          done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(false).build());
          return;
        }
        Token<?> targetFsToken = targetfsDelegationToken.getUserToken();
        if (targetFsToken != null
            && (userToken == null || !targetFsToken.getService().equals(userToken.getService()))) {
          ugi.addToken(targetFsToken);
        }
      }

      incrementUgiReference(ugi);
      loaded = ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          FileSystem fs = null;
          try {
            Configuration conf = env.getConfiguration();
            fs = FileSystem.get(conf);
            for(Pair<byte[], String> el: familyPaths) {
              Path stageFamily = new Path(bulkToken, Bytes.toString(el.getFirst()));
              if(!fs.exists(stageFamily)) {
                fs.mkdirs(stageFamily);
                fs.setPermission(stageFamily, PERM_ALL_ACCESS);
              }
            }
            if (fsCreatedListener != null) {
              fsCreatedListener.accept(env.getRegion());
            }
            //We call bulkLoadHFiles as requesting user
            //To enable access prior to staging
            return env.getRegion().bulkLoadHFiles(familyPaths, true,
                new SecureBulkLoadListener(fs, bulkToken, conf), request.getClusterIdsList());
          } catch (Exception e) {
            LOG.error("Failed to complete bulk load", e);
          }
          return false;
        }
      });
      decrementUgiReference(ugi);
    }
    if (region.getCoprocessorHost() != null) {
      try {
        loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
      } catch (IOException e) {
        ResponseConverter.setControllerException(controller, e);
        done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(false).build());
        return;
      }
    }
    done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(loaded).build());
  }

  private List<BulkLoadObserver> getBulkLoadObservers() {
    List<BulkLoadObserver> coprocessorList =
              this.env.getRegion().getCoprocessorHost().findCoprocessors(BulkLoadObserver.class);

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

  private User getActiveUser() {
    User user = RpcServer.getRequestUser();
    if (user == null) {
      return null;
    }

    //this is for testing
    if (userProvider.isHadoopSecurityEnabled()
        && "simple".equalsIgnoreCase(conf.get(User.HBASE_SECURITY_CONF_KEY))) {
      return User.createUserForTesting(conf, user.getShortName(), new String[]{});
    }

    return user;
  }

  @Override
  public Service getService() {
    return this;
  }

  private static class SecureBulkLoadListener implements BulkLoadListener {
    // Target filesystem
    private FileSystem fs;
    private String stagingDir;
    private Configuration conf;
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
        srcFs = FileSystem.newInstance(p.toUri(), conf);
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
      closeSrcFs();
    }

    private void closeSrcFs() throws IOException {
      if (srcFs != null) {
        srcFs.close();
        srcFs = null;
      }
    }

    @Override
    public void failedBulkLoad(final byte[] family, final String srcPath) throws IOException {
      try {
        Path p = new Path(srcPath);
        if (srcFs == null) {
          srcFs = FileSystem.newInstance(p.toUri(), conf);
        }
        if (!FSHDFSUtils.isSameHdfs(conf, srcFs, fs)) {
          // files are copied so no need to move them back
          return;
        }
        Path stageP = new Path(stagingDir, new Path(Bytes.toString(family), p.getName()));

        // In case of Replication for bulk load files, hfiles are not renamed by end point during
        // prepare stage, so no need of rename here again
        if (p.equals(stageP)) {
          LOG.debug(p.getName() + " is already available in source directory. Skipping rename.");
          return;
        }

        LOG.debug("Moving " + stageP + " back to " + p);
        if (!fs.rename(stageP, p))
          throw new IOException("Failed to move HFile: " + stageP + " to " + p);

        // restore original permission
        if (origPermissions.containsKey(srcPath)) {
          fs.setPermission(p, origPermissions.get(srcPath));
        } else {
          LOG.warn("Can't find previous permission for path=" + srcPath);
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

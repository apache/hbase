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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.List;

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
 * 4. The user makes the data world readable and writable, then moves it
 * into the random staging directory, then calls bulkLoadHFiles()
 *
 * Like delegation tokens the strength of the security lies in the length
 * and randomness of the secret directory.
 *
 */
@InterfaceAudience.Private
public class SecureBulkLoadEndpoint extends BaseEndpointCoprocessor
    implements SecureBulkLoadProtocol {

  public static final long VERSION = 0L;

  //Random number is 320 bits wide
  private static final int RANDOM_WIDTH = 320;
  //We picked 32 as the radix, so the character set
  //will only contain alpha numeric values
  //320/5 = 64 characters
  private static final int RANDOM_RADIX = 32;

  private static Log LOG = LogFactory.getLog(SecureBulkLoadEndpoint.class);

  private final static FsPermission PERM_ALL_ACCESS = FsPermission.valueOf("-rwxrwxrwx");
  private final static FsPermission PERM_HIDDEN = FsPermission.valueOf("-rwx--x--x");
  private final static String BULKLOAD_STAGING_DIR = "hbase.bulkload.staging.dir";

  private SecureRandom random;
  private FileSystem fs;
  private Configuration conf;

  //two levels so it doesn't get deleted accidentally
  //no sticky bit in Hadoop 1.0
  private Path baseStagingDir;

  private RegionCoprocessorEnvironment env;

  private UserProvider provider;

  @Override
  public void start(CoprocessorEnvironment env) {
    super.start(env);

    this.env = (RegionCoprocessorEnvironment)env;
    random = new SecureRandom();
    conf = env.getConfiguration();
    baseStagingDir = getBaseStagingDir(conf);
    this.provider = UserProvider.instantiate(conf);

    try {
      fs = FileSystem.get(conf);
      fs.mkdirs(baseStagingDir, PERM_HIDDEN);
      fs.setPermission(baseStagingDir, PERM_HIDDEN);
      //no sticky bit in hadoop-1.0, making directory nonempty so it never gets erased
      fs.mkdirs(new Path(baseStagingDir,"DONOTERASE"), PERM_HIDDEN);
      FileStatus status = fs.getFileStatus(baseStagingDir);
      if(status == null) {
        throw new IllegalStateException("Failed to create staging directory");
      }
      if(!status.getPermission().equals(PERM_HIDDEN)) {
        throw new IllegalStateException("Directory already exists but permissions aren't set to '-rwx--x--x' ");
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get FileSystem instance",e);
    }
  }

  @Override
  public String prepareBulkLoad(byte[] tableName) throws IOException {
    getAccessController().prePrepareBulkLoad(env);
    return createStagingDir(baseStagingDir, getActiveUser(), tableName).toString();
  }

  @Override
  public void cleanupBulkLoad(String bulkToken) throws IOException {
    getAccessController().preCleanupBulkLoad(env);
    fs.delete(createStagingDir(baseStagingDir,
        getActiveUser(),
        env.getRegion().getTableDesc().getName(),
        new Path(bulkToken).getName()),
        true);
  }

  @Override
  public boolean bulkLoadHFiles(final List<Pair<byte[], String>> familyPaths,
                                final Token<?> userToken, final String bulkToken, boolean assignSeqNum) throws IOException {
    User user = getActiveUser();
    final UserGroupInformation ugi = user.getUGI();
    if(userToken != null) {
      ugi.addToken(userToken);
    } else if (provider.isHadoopSecurityEnabled()) {
      //we allow this to pass through in "simple" security mode
      //for mini cluster testing
      throw new DoNotRetryIOException("User token cannot be null");
    }

    HRegion region = env.getRegion();
    boolean bypass = false;
    if (region.getCoprocessorHost() != null) {
      bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
    }
    boolean loaded = false;
    final IOException[] es = new IOException[1];
    if (!bypass) {
      loaded = ugi.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          FileSystem fs = null;
          try {
            Configuration conf = env.getConfiguration();
            fs = FileSystem.get(conf);
            for(Pair<byte[], String> el: familyPaths) {
              Path p = new Path(el.getSecond());
              LOG.debug("Setting permission for: " + p);
              fs.setPermission(p, PERM_ALL_ACCESS);
              Path stageFamily = new Path(bulkToken, Bytes.toString(el.getFirst()));
              if(!fs.exists(stageFamily)) {
                fs.mkdirs(stageFamily);
                fs.setPermission(stageFamily, PERM_ALL_ACCESS);
              }
            }
            //We call bulkLoadHFiles as requesting user
            //To enable access prior to staging
            return env.getRegion().bulkLoadHFiles(familyPaths,
                new SecureBulkLoadListener(fs, bulkToken));
          }
          catch(DoNotRetryIOException e){
            es[0] = e;
          }
          catch (Exception e) {
            LOG.error("Failed to complete bulk load", e);
          }
          return false;
        }
      });
    }

    if (es[0] != null) {
      throw es[0];
    }

    if (region.getCoprocessorHost() != null) {
      loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
    }
    return loaded;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (SecureBulkLoadProtocol.class.getName().equals(protocol)) {
      return SecureBulkLoadEndpoint.VERSION;
    }
    LOG.warn("Unknown protocol requested: " + protocol);
    return -1;
  }

  private AccessController getAccessController() {
    return (AccessController) this.env.getRegion()
        .getCoprocessorHost().findCoprocessor(AccessController.class.getName());
  }

  private Path createStagingDir(Path baseDir, User user, byte[] tableName) throws IOException {
    String randomDir = user.getShortName()+"__"+Bytes.toString(tableName)+"__"+
        (new BigInteger(RANDOM_WIDTH, random).toString(RANDOM_RADIX));
    return createStagingDir(baseDir, user, tableName, randomDir);
  }

  private Path createStagingDir(Path baseDir,
                                User user,
                                byte[] tableName,
                                String randomDir) throws IOException {
    Path p = new Path(baseDir, randomDir);
    fs.mkdirs(p, PERM_ALL_ACCESS);
    fs.setPermission(p, PERM_ALL_ACCESS);
    return p;
  }

  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      throw new DoNotRetryIOException("Failed to get requesting user");
    }

    //this is for testing
    if("simple".equalsIgnoreCase(conf.get(User.HBASE_SECURITY_CONF_KEY))) {
      return User.createUserForTesting(conf, user.getShortName(), new String[]{});
    }

    return user;
  }

  /**
   * This returns the staging path for a given column family.
   * This is needed for clean recovery and called reflectively in LoadIncrementalHFiles
   */
  public static Path getStagingPath(Configuration conf, String bulkToken, byte[] family) {
    Path stageP = new Path(getBaseStagingDir(conf), bulkToken);
    return new Path(stageP, Bytes.toString(family));
  }

  private static Path getBaseStagingDir(Configuration conf) {
    return new Path(conf.get(BULKLOAD_STAGING_DIR, "/tmp/hbase-staging"));
  }

  private static class SecureBulkLoadListener implements HRegion.BulkLoadListener {
    private FileSystem fs;
    private String stagingDir;

    public SecureBulkLoadListener(FileSystem fs, String stagingDir) {
      this.fs = fs;
      this.stagingDir = stagingDir;
    }

    @Override
    public String prepareBulkLoad(final byte[] family, final String srcPath) throws IOException {
      Path p = new Path(srcPath);
      Path stageP = new Path(stagingDir, new Path(Bytes.toString(family), p.getName()));

      if(!isFile(p)) {
        throw new IOException("Path does not reference a file: " + p);
      }

      LOG.debug("Moving " + p + " to " + stageP);
      if(!fs.rename(p, stageP)) {
        throw new IOException("Failed to move HFile: " + p + " to " + stageP);
      }
      return stageP.toString();
    }

    @Override
    public void doneBulkLoad(byte[] family, String srcPath) throws IOException {
      LOG.debug("Bulk Load done for: " + srcPath);
    }

    @Override
    public void failedBulkLoad(final byte[] family, final String srcPath) throws IOException {
      Path p = new Path(srcPath);
      Path stageP = new Path(stagingDir,
          new Path(Bytes.toString(family), p.getName()));
      LOG.debug("Moving " + stageP + " back to " + p);
      if(!fs.rename(stageP, p))
        throw new IOException("Failed to move HFile: " + stageP + " to " + p);
    }

    /**
     * Check if the path is referencing a file.
     * This is mainly needed to avoid symlinks.
     * @param p
     * @return true if the p is a file
     * @throws IOException
     */
    private boolean isFile(Path p) throws IOException {
      FileStatus status = fs.getFileStatus(p);
      boolean isFile = !status.isDir();
      try {
        isFile = isFile && !(Boolean)Methods.call(FileStatus.class, status, "isSymlink", null, null);
      } catch (Exception e) {
      }
      return isFile;
    }
  }
}

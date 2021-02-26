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
package org.apache.hadoop.hbase.security.token;

import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;
import static org.apache.hadoop.hdfs.web.WebHdfsConstants.SWEBHDFS_TOKEN_KIND;
import static org.apache.hadoop.hdfs.web.WebHdfsConstants.WEBHDFS_TOKEN_KIND;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({SecurityTests.class, SmallTests.class})
public class TestFsDelegationToken {
  private UserProvider userProvider = Mockito.mock(UserProvider.class);
  private User user = Mockito.mock(User.class);
  private FsDelegationToken fsDelegationToken = new FsDelegationToken(userProvider, "Renewer");
  private Token hdfsToken = Mockito.mock(Token.class);
  private Token webhdfsToken = Mockito.mock(Token.class);
  private Token swebhdfsToken = Mockito.mock(Token.class);
  private WebHdfsFileSystem webHdfsFileSystem = Mockito.mock(WebHdfsFileSystem.class);
  private WebHdfsFileSystem swebHdfsFileSystem = Mockito.mock(SWebHdfsFileSystem.class);
  private FileSystem fileSystem = Mockito.mock(FileSystem.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFsDelegationToken.class);

  @Before
  public void setup() throws IOException, URISyntaxException {
    when(userProvider.getCurrent()).thenReturn(user);
    when(userProvider.isHadoopSecurityEnabled()).thenReturn(true);
    when(fileSystem.getCanonicalServiceName()).thenReturn("hdfs://");
    when(fileSystem.getUri()).thenReturn(new URI("hdfs://someUri"));
    when(webHdfsFileSystem.getCanonicalServiceName()).thenReturn("webhdfs://");
    when(webHdfsFileSystem.getUri()).thenReturn(new URI("webhdfs://someUri"));
    when(swebHdfsFileSystem.getCanonicalServiceName()).thenReturn("swebhdfs://");
    when(swebHdfsFileSystem.getUri()).thenReturn(new URI("swebhdfs://someUri"));
    when(user.getToken(
        HDFS_DELEGATION_KIND.toString(),
        fileSystem.getCanonicalServiceName()))
        .thenReturn(hdfsToken);
    when(user.getToken(
        WEBHDFS_TOKEN_KIND.toString(),
        webHdfsFileSystem.getCanonicalServiceName())).thenReturn(webhdfsToken);
    when(user.getToken(
        SWEBHDFS_TOKEN_KIND.toString(),
        swebHdfsFileSystem.getCanonicalServiceName())).thenReturn(swebhdfsToken);
    when(hdfsToken.getKind()).thenReturn(new Text("HDFS_DELEGATION_TOKEN"));
    when(webhdfsToken.getKind()).thenReturn(WEBHDFS_TOKEN_KIND);
    when(swebhdfsToken.getKind()).thenReturn(SWEBHDFS_TOKEN_KIND);
  }

  @Test
  public void acquireDelegationToken_defaults_to_hdfsFileSystem() throws IOException {
    fsDelegationToken.acquireDelegationToken(fileSystem);
    assertEquals(
        fsDelegationToken.getUserToken().getKind(), HDFS_DELEGATION_KIND);
  }

  @Test
  public void acquireDelegationToken_webhdfsFileSystem() throws IOException {
    fsDelegationToken.acquireDelegationToken(webHdfsFileSystem);
    assertEquals(
        fsDelegationToken.getUserToken().getKind(), WEBHDFS_TOKEN_KIND);
  }

  @Test
  public void acquireDelegationToken_swebhdfsFileSystem() throws IOException {
    fsDelegationToken.acquireDelegationToken(swebHdfsFileSystem);
    assertEquals(
        fsDelegationToken.getUserToken().getKind(), SWEBHDFS_TOKEN_KIND);
  }

  @Test(expected = NullPointerException.class)
  public void acquireDelegationTokenByTokenKind_rejects_null_token_kind() throws IOException {
    fsDelegationToken.acquireDelegationToken(null, fileSystem);
  }

  @Test
  public void acquireDelegationTokenByTokenKind_webhdfsFileSystem() throws IOException {
    fsDelegationToken
        .acquireDelegationToken(WEBHDFS_TOKEN_KIND.toString(), webHdfsFileSystem);
    assertEquals(fsDelegationToken.getUserToken().getKind(), WEBHDFS_TOKEN_KIND);
  }

  @Test
  public void acquireDelegationTokenByTokenKind_swebhdfsFileSystem() throws IOException {
    fsDelegationToken
        .acquireDelegationToken(
            SWEBHDFS_TOKEN_KIND.toString(), swebHdfsFileSystem);
    assertEquals(fsDelegationToken.getUserToken().getKind(), SWEBHDFS_TOKEN_KIND);
  }
}

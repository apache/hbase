/**
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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.ListLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.SetAuthsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabel;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsRequest;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsService;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

/**
 * Utility client for doing visibility labels admin operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class VisibilityClient {

  /**
   * Return true if cell visibility features are supported and enabled
   * @param connection The connection to use
   * @return true if cell visibility features are supported and enabled, false otherwise
   * @throws IOException
   */
  public static boolean isCellVisibilityEnabled(Connection connection) throws IOException {
    return connection.getAdmin().getSecurityCapabilities()
        .contains(SecurityCapability.CELL_VISIBILITY);
  }

  /**
   * Utility method for adding label to the system.
   *
   * @param conf
   * @param label
   * @return VisibilityLabelsResponse
   * @throws Throwable
   * @deprecated Use {@link #addLabel(Connection,String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsResponse addLabel(Configuration conf, final String label)
      throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return addLabels(connection, new String[] { label });
    }
  }

  /**
   * Utility method for adding label to the system.
   *
   * @param connection
   * @param label
   * @return VisibilityLabelsResponse
   * @throws Throwable
   */
  public static VisibilityLabelsResponse addLabel(Connection connection, final String label)
      throws Throwable {
    return addLabels(connection, new String[] { label });
  }

  /**
   * Utility method for adding labels to the system.
   *
   * @param conf
   * @param labels
   * @return VisibilityLabelsResponse
   * @throws Throwable
   * @deprecated Use {@link #addLabels(Connection,String[])} instead.
   */
  @Deprecated
  public static VisibilityLabelsResponse addLabels(Configuration conf, final String[] labels)
      throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return addLabels(connection, labels);
    }
  }

  /**
   * Utility method for adding labels to the system.
   *
   * @param connection
   * @param labels
   * @return VisibilityLabelsResponse
   * @throws Throwable
   */
  public static VisibilityLabelsResponse addLabels(Connection connection, final String[] labels)
      throws Throwable {

    try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
      Batch.Call<VisibilityLabelsService, VisibilityLabelsResponse> callable =
          new Batch.Call<VisibilityLabelsService, VisibilityLabelsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<VisibilityLabelsResponse> rpcCallback =
                new BlockingRpcCallback<VisibilityLabelsResponse>();

            public VisibilityLabelsResponse call(VisibilityLabelsService service)
                throws IOException {
              VisibilityLabelsRequest.Builder builder = VisibilityLabelsRequest.newBuilder();
              for (String label : labels) {
                if (label.length() > 0) {
                  VisibilityLabel.Builder newBuilder = VisibilityLabel.newBuilder();
                  newBuilder.setLabel(ByteStringer.wrap(Bytes.toBytes(label)));
                  builder.addVisLabel(newBuilder.build());
                }
              }
              service.addLabels(controller, builder.build(), rpcCallback);
              VisibilityLabelsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], VisibilityLabelsResponse> result =
          table.coprocessorService(VisibilityLabelsService.class, HConstants.EMPTY_BYTE_ARRAY,
            HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
  }

  /**
   * Sets given labels globally authorized for the user.
   * @param conf
   * @param auths
   * @param user
   * @return VisibilityLabelsResponse
   * @throws Throwable
   * @deprecated Use {@link #setAuths(Connection,String[],String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsResponse setAuths(Configuration conf, final String[] auths,
      final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return setOrClearAuths(connection, auths, user, true);
    }
  }

  /**
   * Sets given labels globally authorized for the user.
   * @param connection
   * @param auths
   * @param user
   * @return VisibilityLabelsResponse
   * @throws Throwable
   */
  public static VisibilityLabelsResponse setAuths(Connection connection, final String[] auths,
      final String user) throws Throwable {
    return setOrClearAuths(connection, auths, user, true);
  }

  /**
   * @param conf
   * @param user
   * @return labels, the given user is globally authorized for.
   * @throws Throwable
   * @deprecated Use {@link #getAuths(Connection,String)} instead.
   */
  @Deprecated
  public static GetAuthsResponse getAuths(Configuration conf, final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return getAuths(connection, user);
    }
  }

  /**
   * @param connection the Connection instance to use.
   * @param user
   * @return labels, the given user is globally authorized for.
   * @throws Throwable
   */
  public static GetAuthsResponse getAuths(Connection connection, final String user)
      throws Throwable {
      try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
        Batch.Call<VisibilityLabelsService, GetAuthsResponse> callable =
            new Batch.Call<VisibilityLabelsService, GetAuthsResponse>() {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<GetAuthsResponse> rpcCallback =
              new BlockingRpcCallback<GetAuthsResponse>();

          public GetAuthsResponse call(VisibilityLabelsService service) throws IOException {
            GetAuthsRequest.Builder getAuthReqBuilder = GetAuthsRequest.newBuilder();
            getAuthReqBuilder.setUser(ByteStringer.wrap(Bytes.toBytes(user)));
            service.getAuths(controller, getAuthReqBuilder.build(), rpcCallback);
            GetAuthsResponse response = rpcCallback.get();
            if (controller.failedOnException()) {
              throw controller.getFailedOn();
            }
            return response;
          }
        };
        Map<byte[], GetAuthsResponse> result =
          table.coprocessorService(VisibilityLabelsService.class,
            HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
        return result.values().iterator().next(); // There will be exactly one region for labels
        // table and so one entry in result Map.
      }
  }

  /**
   * Retrieve the list of visibility labels defined in the system.
   * @param conf
   * @param regex  The regular expression to filter which labels are returned.
   * @return labels The list of visibility labels defined in the system.
   * @throws Throwable
   * @deprecated Use {@link #listLabels(Connection,String)} instead.
   */
  @Deprecated
  public static ListLabelsResponse listLabels(Configuration conf, final String regex)
      throws Throwable {
    try(Connection connection = ConnectionFactory.createConnection(conf)){
      return listLabels(connection, regex);
    }
  }

  /**
   * Retrieve the list of visibility labels defined in the system.
   * @param connection The Connection instance to use.
   * @param regex  The regular expression to filter which labels are returned.
   * @return labels The list of visibility labels defined in the system.
   * @throws Throwable
   */
  public static ListLabelsResponse listLabels(Connection connection, final String regex)
      throws Throwable {
    Table table = null;
    try {
      table = connection.getTable(LABELS_TABLE_NAME);
      Batch.Call<VisibilityLabelsService, ListLabelsResponse> callable =
          new Batch.Call<VisibilityLabelsService, ListLabelsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<ListLabelsResponse> rpcCallback =
                new BlockingRpcCallback<ListLabelsResponse>();

            public ListLabelsResponse call(VisibilityLabelsService service) throws IOException {
              ListLabelsRequest.Builder listAuthLabelsReqBuilder = ListLabelsRequest.newBuilder();
              if (regex != null) {
                // Compile the regex here to catch any regex exception earlier.
                Pattern pattern = Pattern.compile(regex);
                listAuthLabelsReqBuilder.setRegex(pattern.toString());
              }
              service.listLabels(controller, listAuthLabelsReqBuilder.build(), rpcCallback);
              ListLabelsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], ListLabelsResponse> result =
          table.coprocessorService(VisibilityLabelsService.class, HConstants.EMPTY_BYTE_ARRAY,
            HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
    finally {
      if (table != null) {
        table.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  /**
   * Removes given labels from user's globally authorized list of labels.
   * @param conf
   * @param auths
   * @param user
   * @return VisibilityLabelsResponse
   * @throws Throwable
   * @deprecated Use {@link #clearAuths(Connection,String[],String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsResponse clearAuths(Configuration conf, final String[] auths,
      final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return setOrClearAuths(connection, auths, user, false);
    }
  }

  /**
   * Removes given labels from user's globally authorized list of labels.
   * @param connection
   * @param auths
   * @param user
   * @return VisibilityLabelsResponse
   * @throws Throwable
   */
  public static VisibilityLabelsResponse clearAuths(Connection connection, final String[] auths,
      final String user) throws Throwable {
    return setOrClearAuths(connection, auths, user, false);
  }

  private static VisibilityLabelsResponse setOrClearAuths(Connection connection,
      final String[] auths, final String user, final boolean setOrClear)
          throws IOException, ServiceException, Throwable {

      try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
        Batch.Call<VisibilityLabelsService, VisibilityLabelsResponse> callable =
            new Batch.Call<VisibilityLabelsService, VisibilityLabelsResponse>() {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<VisibilityLabelsResponse> rpcCallback =
              new BlockingRpcCallback<VisibilityLabelsResponse>();

          public VisibilityLabelsResponse call(VisibilityLabelsService service) throws IOException {
            SetAuthsRequest.Builder setAuthReqBuilder = SetAuthsRequest.newBuilder();
            setAuthReqBuilder.setUser(ByteStringer.wrap(Bytes.toBytes(user)));
            for (String auth : auths) {
              if (auth.length() > 0) {
                setAuthReqBuilder.addAuth(ByteStringer.wrap(Bytes.toBytes(auth)));
              }
            }
            if (setOrClear) {
              service.setAuths(controller, setAuthReqBuilder.build(), rpcCallback);
            } else {
              service.clearAuths(controller, setAuthReqBuilder.build(), rpcCallback);
            }
            VisibilityLabelsResponse response = rpcCallback.get();
            if (controller.failedOnException()) {
              throw controller.getFailedOn();
            }
            return response;
          }
        };
        Map<byte[], VisibilityLabelsResponse> result = table.coprocessorService(
            VisibilityLabelsService.class, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
            callable);
        return result.values().iterator().next(); // There will be exactly one region for labels
        // table and so one entry in result Map.
      }
  }
}
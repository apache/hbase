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

package org.apache.hadoop.hbase.rest.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.model.StorageClusterStatusModel;
import org.apache.hadoop.hbase.rest.model.StorageClusterVersionModel;
import org.apache.hadoop.hbase.rest.model.TableListModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.rest.model.VersionModel;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class RemoteAdmin {

  final Client client;
  final Configuration conf;
  final String accessToken;
  final int maxRetries;
  final long sleepTime;

  // This unmarshaller is necessary for getting the /version/cluster resource.
  // This resource does not support protobufs. Therefore this is necessary to
  // request/interpret it as XML.
  private static volatile Unmarshaller versionClusterUnmarshaller;

  /**
   * Constructor
   * 
   * @param client
   * @param conf
   */
  public RemoteAdmin(Client client, Configuration conf) {
    this(client, conf, null);
  }

  static Unmarshaller getUnmarsheller() throws JAXBException {

    if (versionClusterUnmarshaller == null) {

      RemoteAdmin.versionClusterUnmarshaller = JAXBContext.newInstance(
          StorageClusterVersionModel.class).createUnmarshaller();
    }
    return RemoteAdmin.versionClusterUnmarshaller;
  }

  /**
   * Constructor
   * @param client
   * @param conf
   * @param accessToken
   */
  public RemoteAdmin(Client client, Configuration conf, String accessToken) {
    this.client = client;
    this.conf = conf;
    this.accessToken = accessToken;
    this.maxRetries = conf.getInt("hbase.rest.client.max.retries", 10);
    this.sleepTime = conf.getLong("hbase.rest.client.sleep", 1000);
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * @return string representing the rest api's version
   * @throws IOException
   *           if the endpoint does not exist, there is a timeout, or some other
   *           general failure mode
   */
  public VersionModel getRestVersion() throws IOException {

    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }

    path.append("version/rest");

    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(path.toString(),
          Constants.MIMETYPE_PROTOBUF);
      code = response.getCode();
      switch (code) {
      case 200:

        VersionModel v = new VersionModel();
        return (VersionModel) v.getObjectFromMessage(response.getBody());
      case 404:
        throw new IOException("REST version not found");
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("get request to " + path.toString()
            + " returned " + code);
      }
    }
    throw new IOException("get request to " + path.toString() + " timed out");
  }

  /**
   * @return string representing the cluster's version
   * @throws IOException if the endpoint does not exist, there is a timeout, or some other general failure mode
   */
  public StorageClusterStatusModel getClusterStatus() throws IOException {

      StringBuilder path = new StringBuilder();
      path.append('/');
      if (accessToken !=null) {
          path.append(accessToken);
          path.append('/');
      }

    path.append("status/cluster");

    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(path.toString(),
          Constants.MIMETYPE_PROTOBUF);
      code = response.getCode();
      switch (code) {
      case 200:
        StorageClusterStatusModel s = new StorageClusterStatusModel();
        return (StorageClusterStatusModel) s.getObjectFromMessage(response
            .getBody());
      case 404:
        throw new IOException("Cluster version not found");
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("get request to " + path + " returned " + code);
      }
    }
    throw new IOException("get request to " + path + " timed out");
  }

  /**
   * @return string representing the cluster's version
   * @throws IOException
   *           if the endpoint does not exist, there is a timeout, or some other
   *           general failure mode
   */
  public StorageClusterVersionModel getClusterVersion() throws IOException {

    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }

    path.append("version/cluster");

    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(path.toString(), Constants.MIMETYPE_XML);
      code = response.getCode();
      switch (code) {
      case 200:
        try {

          return (StorageClusterVersionModel) getUnmarsheller().unmarshal(
              getInputStream(response));
        } catch (JAXBException jaxbe) {

          throw new IOException(
              "Issue parsing StorageClusterVersionModel object in XML form: "
                  + jaxbe.getLocalizedMessage(), jaxbe);
        }
      case 404:
        throw new IOException("Cluster version not found");
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException(path.toString() + " request returned " + code);
      }
    }
    throw new IOException("get request to " + path.toString()
        + " request timed out");
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }
    path.append(Bytes.toStringBinary(tableName));
    path.append('/');
    path.append("exists");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(path.toString(), Constants.MIMETYPE_PROTOBUF);
      code = response.getCode();
      switch (code) {
      case 200:
        return true;
      case 404:
        return false;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("get request to " + path.toString() + " returned " + code);
      }
    }
    throw new IOException("get request to " + path.toString() + " timed out");
  }

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc)
      throws IOException {
    TableSchemaModel model = new TableSchemaModel(desc);
    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }
    path.append(desc.getTableName());
    path.append('/');
    path.append("schema");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(path.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      code = response.getCode();
      switch (code) {
      case 201:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("create request to " + path.toString() + " returned " + code);
      }
    }
    throw new IOException("create request to " + path.toString() + " timed out");
  }

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(final byte [] tableName) throws IOException {
    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }
    path.append(Bytes.toStringBinary(tableName));
    path.append('/');
    path.append("schema");
    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.delete(path.toString());
      code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("delete request to " + path.toString() + " returned " + code);
      }
    }
    throw new IOException("delete request to " + path.toString() + " timed out");
  }

  /**
   * @return string representing the cluster's version
   * @throws IOException
   *           if the endpoint does not exist, there is a timeout, or some other
   *           general failure mode
   */
  public TableListModel getTableList() throws IOException {

    StringBuilder path = new StringBuilder();
    path.append('/');
    if (accessToken != null) {
      path.append(accessToken);
      path.append('/');
    }

    int code = 0;
    for (int i = 0; i < maxRetries; i++) {
      // Response response = client.get(path.toString(),
      // Constants.MIMETYPE_XML);
      Response response = client.get(path.toString(),
          Constants.MIMETYPE_PROTOBUF);
      code = response.getCode();
      switch (code) {
      case 200:
        TableListModel t = new TableListModel();
        return (TableListModel) t.getObjectFromMessage(response.getBody());
      case 404:
        throw new IOException("Table list not found");
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("get request to " + path.toString()
            + " request returned " + code);
      }
    }
    throw new IOException("get request to " + path.toString()
        + " request timed out");
  }

  /**
   * Convert the REST server's response to an XML reader.
   *
   * @param response The REST server's response.
   * @return A reader over the parsed XML document.
   * @throws IOException If the document fails to parse
   */
  private XMLStreamReader getInputStream(Response response) throws IOException {
    try {
      // Prevent the parser from reading XMl with external entities defined
      XMLInputFactory xif = XMLInputFactory.newFactory();
      xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
      xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
      return xif.createXMLStreamReader(new ByteArrayInputStream(response.getBody()));
    } catch (XMLStreamException e) {
      throw new IOException("Failed to parse XML", e);
    }
  }
}

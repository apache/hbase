/*
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

package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.CheckPermissionsRequest;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;

import com.google.protobuf.ServiceException;

/**
 * Utility methods for testing security
 */
public class SecureTestUtil {
  public static void enableSecurity(Configuration conf) throws IOException {
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hbase.coprocessor.master.classes", AccessController.class.getName());
    conf.set("hbase.coprocessor.region.classes", AccessController.class.getName()+
        ","+SecureBulkLoadEndpoint.class.getName());
    // add the process running user to superusers
    String currentUser = User.getCurrent().getName();
    conf.set("hbase.superuser", "admin,"+currentUser);
    // Need HFile V3 for tags for security features
    conf.setInt("hfile.format.version", 3);
  }
  
  public void verifyAllowed(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
      } catch (AccessDeniedException ade) {
        fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
      }
    }
  }

  public void verifyAllowed(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyAllowed(user, action);
    }
  }

  public void verifyDenied(User user, PrivilegedExceptionAction... actions) throws Exception {
    for (PrivilegedExceptionAction action : actions) {
      try {
        user.runAs(action);
        fail("Expected AccessDeniedException for user '" + user.getShortName() + "'");
      } catch (IOException e) {
        boolean isAccessDeniedException = false;
        if(e instanceof RetriesExhaustedWithDetailsException) {
          // in case of batch operations, and put, the client assembles a
          // RetriesExhaustedWithDetailsException instead of throwing an
          // AccessDeniedException
          for(Throwable ex : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
            if (ex instanceof AccessDeniedException) {
              isAccessDeniedException = true;
              break;
            }
          }
        }
        else {
          // For doBulkLoad calls AccessDeniedException
          // is buried in the stack trace
          Throwable ex = e;
          do {
            if (ex instanceof AccessDeniedException) {
              isAccessDeniedException = true;
              break;
            }
          } while((ex = ex.getCause()) != null);
        }
        if (!isAccessDeniedException) {
          fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
        }
      } catch (UndeclaredThrowableException ute) {
        // TODO why we get a PrivilegedActionException, which is unexpected?
        Throwable ex = ute.getUndeclaredThrowable();
        if (ex instanceof PrivilegedActionException) {
          ex = ((PrivilegedActionException) ex).getException();
        }
        if (ex instanceof ServiceException) {
          ServiceException se = (ServiceException)ex;
          if (se.getCause() != null && se.getCause() instanceof AccessDeniedException) {
            // expected result
            return;
          }
        }
        fail("Not receiving AccessDeniedException for user '" + user.getShortName() + "'");
      }
    }
  }

  public void verifyDenied(PrivilegedExceptionAction action, User... users) throws Exception {
    for (User user : users) {
      verifyDenied(user, action);
    }
  }
  
  public void checkTablePerms(Configuration conf, byte[] table, byte[] family, byte[] column,
      Permission.Action... actions) throws IOException {
    Permission[] perms = new Permission[actions.length];
    for (int i = 0; i < actions.length; i++) {
      perms[i] = new TablePermission(TableName.valueOf(table), family, column, actions[i]);
    }

    checkTablePerms(conf, table, perms);
  }

  public void checkTablePerms(Configuration conf, byte[] table, Permission... perms) throws IOException {
    CheckPermissionsRequest.Builder request = CheckPermissionsRequest.newBuilder();
    for (Permission p : perms) {
      request.addPermission(ProtobufUtil.toPermission(p));
    }
    HTable acl = new HTable(conf, table);
    try {
      AccessControlService.BlockingInterface protocol =
        AccessControlService.newBlockingStub(acl.coprocessorService(new byte[0]));
      try {
        protocol.checkPermissions(null, request.build());
      } catch (ServiceException se) {
        ProtobufUtil.toIOException(se);
      }
    } finally {
      acl.close();
    }
  }
}

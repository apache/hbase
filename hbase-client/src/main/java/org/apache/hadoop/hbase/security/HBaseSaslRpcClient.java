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

package org.apache.hadoop.hbase.security;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class that encapsulates SASL logic for RPC client. Copied from
 * <code>org.apache.hadoop.security</code>
 */
@InterfaceAudience.Private
public class HBaseSaslRpcClient extends AbstractHBaseSaslRpcClient {

  private static final Log LOG = LogFactory.getLog(HBaseSaslRpcClient.class);

  public HBaseSaslRpcClient(AuthMethod method, Token<? extends TokenIdentifier> token,
      String serverPrincipal, boolean fallbackAllowed) throws IOException {
    super(method, token, serverPrincipal, fallbackAllowed);
  }

  public HBaseSaslRpcClient(AuthMethod method, Token<? extends TokenIdentifier> token,
      String serverPrincipal, boolean fallbackAllowed, String rpcProtection) throws IOException {
    super(method, token, serverPrincipal, fallbackAllowed, rpcProtection);
  }

  private static void readStatus(DataInputStream inStream) throws IOException {
    int status = inStream.readInt(); // read status
    if (status != SaslStatus.SUCCESS.state) {
      throw new RemoteException(WritableUtils.readString(inStream),
          WritableUtils.readString(inStream));
    }
  }

  /**
   * Do client side SASL authentication with server via the given InputStream and OutputStream
   * @param inS InputStream to use
   * @param outS OutputStream to use
   * @return true if connection is set up, or false if needs to switch to simple Auth.
   * @throws IOException
   */
  public boolean saslConnect(InputStream inS, OutputStream outS) throws IOException {
    DataInputStream inStream = new DataInputStream(new BufferedInputStream(inS));
    DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(outS));

    try {
      byte[] saslToken = getInitialResponse();
      if (saslToken != null) {
        outStream.writeInt(saslToken.length);
        outStream.write(saslToken, 0, saslToken.length);
        outStream.flush();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Have sent token of size " + saslToken.length + " from initSASLContext.");
        }
      }
      if (!isComplete()) {
        readStatus(inStream);
        int len = inStream.readInt();
        if (len == SaslUtil.SWITCH_TO_SIMPLE_AUTH) {
          if (!fallbackAllowed) {
            throw new IOException("Server asks us to fall back to SIMPLE auth, "
                + "but this client is configured to only allow secure connections.");
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Server asks us to fall back to simple auth.");
          }
          dispose();
          return false;
        }
        saslToken = new byte[len];
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will read input token of size " + saslToken.length
              + " for processing by initSASLContext");
        }
        inStream.readFully(saslToken);
      }

      while (!isComplete()) {
        saslToken = evaluateChallenge(saslToken);
        if (saslToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + saslToken.length + " from initSASLContext.");
          }
          outStream.writeInt(saslToken.length);
          outStream.write(saslToken, 0, saslToken.length);
          outStream.flush();
        }
        if (!isComplete()) {
          readStatus(inStream);
          saslToken = new byte[inStream.readInt()];
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will read input token of size " + saslToken.length
                + " for processing by initSASLContext");
          }
          inStream.readFully(saslToken);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client context established. Negotiated QoP: "
            + saslClient.getNegotiatedProperty(Sasl.QOP));
      }
      return true;
    } catch (IOException e) {
      try {
        saslClient.dispose();
      } catch (SaslException ignored) {
        // ignore further exceptions during cleanup
      }
      throw e;
    }
  }

  /**
   * Get a SASL wrapped InputStream. Can be called only after saslConnect() has been called.
   * @param in the InputStream to wrap
   * @return a SASL wrapped InputStream
   * @throws IOException
   */
  public InputStream getInputStream(InputStream in) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslInputStream(in, saslClient);
  }

  /**
   * Get a SASL wrapped OutputStream. Can be called only after saslConnect() has been called.
   * @param out the OutputStream to wrap
   * @return a SASL wrapped OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream(OutputStream out) throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    return new SaslOutputStream(out, saslClient);
  }
}

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
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that encapsulates SASL logic for RPC client. Copied from
 * <code>org.apache.hadoop.security</code>
 */
@InterfaceAudience.Private
public class HBaseSaslRpcClient extends AbstractHBaseSaslRpcClient {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSaslRpcClient.class);
  private boolean cryptoAesEnable;
  private CryptoAES cryptoAES;
  private InputStream saslInputStream;
  private InputStream cryptoInputStream;
  private OutputStream saslOutputStream;
  private OutputStream cryptoOutputStream;
  private boolean initStreamForCrypto;

  public HBaseSaslRpcClient(Configuration conf, SaslClientAuthenticationProvider provider,
      Token<? extends TokenIdentifier> token, InetAddress serverAddr, SecurityInfo securityInfo,
      boolean fallbackAllowed) throws IOException {
    super(conf, provider, token, serverAddr, securityInfo, fallbackAllowed);
  }

  public HBaseSaslRpcClient(Configuration conf, SaslClientAuthenticationProvider provider,
      Token<? extends TokenIdentifier> token, InetAddress serverAddr, SecurityInfo securityInfo,
      boolean fallbackAllowed, String rpcProtection, boolean initStreamForCrypto)
          throws IOException {
    super(conf, provider, token, serverAddr, securityInfo, fallbackAllowed, rpcProtection);
    this.initStreamForCrypto = initStreamForCrypto;
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

      try {
        readStatus(inStream);
      }
      catch (IOException e){
        if(e instanceof RemoteException){
          LOG.debug("Sasl connection failed: ", e);
          throw e;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("SASL client context established. Negotiated QoP: "
            + saslClient.getNegotiatedProperty(Sasl.QOP));
      }
      // initial the inputStream, outputStream for both Sasl encryption
      // and Crypto AES encryption if necessary
      // if Crypto AES encryption enabled, the saslInputStream/saslOutputStream is
      // only responsible for connection header negotiation,
      // cryptoInputStream/cryptoOutputStream is responsible for rpc encryption with Crypto AES
      saslInputStream = new SaslInputStream(inS, saslClient);
      saslOutputStream = new SaslOutputStream(outS, saslClient);
      if (initStreamForCrypto) {
        cryptoInputStream = new WrappedInputStream(inS);
        cryptoOutputStream = new WrappedOutputStream(outS);
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

  public String getSaslQOP() {
    return (String) saslClient.getNegotiatedProperty(Sasl.QOP);
  }

  public void initCryptoCipher(RPCProtos.CryptoCipherMeta cryptoCipherMeta,
      Configuration conf) throws IOException {
    // create SaslAES for client
    cryptoAES = EncryptionUtil.createCryptoAES(cryptoCipherMeta, conf);
    cryptoAesEnable = true;
  }

  /**
   * Get a SASL wrapped InputStream. Can be called only after saslConnect() has been called.
   * @return a SASL wrapped InputStream
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    // If Crypto AES is enabled, return cryptoInputStream which unwrap the data with Crypto AES.
    if (cryptoAesEnable && cryptoInputStream != null) {
      return cryptoInputStream;
    }
    return saslInputStream;
  }

  class WrappedInputStream extends FilterInputStream {
    private ByteBuffer unwrappedRpcBuffer = ByteBuffer.allocate(0);
    public WrappedInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int n = read(b, 0, 1);
      return (n != -1) ? b[0] : -1;
    }

    @Override
    public int read(byte b[]) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
      // fill the buffer with the next RPC message
      if (unwrappedRpcBuffer.remaining() == 0) {
        readNextRpcPacket();
      }
      // satisfy as much of the request as possible
      int readLen = Math.min(len, unwrappedRpcBuffer.remaining());
      unwrappedRpcBuffer.get(buf, off, readLen);
      return readLen;
    }

    // unwrap messages with Crypto AES
    private void readNextRpcPacket() throws IOException {
      LOG.debug("reading next wrapped RPC packet");
      DataInputStream dis = new DataInputStream(in);
      int rpcLen = dis.readInt();
      byte[] rpcBuf = new byte[rpcLen];
      dis.readFully(rpcBuf);

      // unwrap with Crypto AES
      rpcBuf = cryptoAES.unwrap(rpcBuf, 0, rpcBuf.length);
      if (LOG.isDebugEnabled()) {
        LOG.debug("unwrapping token of length:" + rpcBuf.length);
      }
      unwrappedRpcBuffer = ByteBuffer.wrap(rpcBuf);
    }
  }

  /**
   * Get a SASL wrapped OutputStream. Can be called only after saslConnect() has been called.
   * @return a SASL wrapped OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException {
    if (!saslClient.isComplete()) {
      throw new IOException("Sasl authentication exchange hasn't completed yet");
    }
    // If Crypto AES is enabled, return cryptoOutputStream which wrap the data with Crypto AES.
    if (cryptoAesEnable && cryptoOutputStream != null) {
      return cryptoOutputStream;
    }
    return saslOutputStream;
  }

  class WrappedOutputStream extends FilterOutputStream {
    public WrappedOutputStream(OutputStream out) throws IOException {
      super(out);
    }
    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("wrapping token of length:" + len);
      }

      // wrap with Crypto AES
      byte[] wrapped = cryptoAES.wrap(buf, off, len);
      DataOutputStream dob = new DataOutputStream(out);
      dob.writeInt(wrapped.length);
      dob.write(wrapped, 0, wrapped.length);
      dob.flush();
    }
  }
}

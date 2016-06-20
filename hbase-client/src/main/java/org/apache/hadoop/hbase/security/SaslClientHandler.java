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
package org.apache.hadoop.hbase.security;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Random;

/**
 * Handles Sasl connections
 */
@InterfaceAudience.Private
public class SaslClientHandler extends ChannelDuplexHandler {
  private static final Log LOG = LogFactory.getLog(SaslClientHandler.class);

  private final boolean fallbackAllowed;

  private final UserGroupInformation ticket;

  /**
   * Used for client or server's token to send or receive from each other.
   */
  private final SaslClient saslClient;
  private final Map<String, String> saslProps;
  private final SaslExceptionHandler exceptionHandler;
  private final SaslSuccessfulConnectHandler successfulConnectHandler;
  private byte[] saslToken;
  private byte[] connectionHeader;
  private boolean firstRead = true;

  private int retryCount = 0;
  private Random random;

  /**
   * @param ticket                   the ugi
   * @param method                   auth method
   * @param token                    for Sasl
   * @param serverPrincipal          Server's Kerberos principal name
   * @param fallbackAllowed          True if server may also fall back to less secure connection
   * @param rpcProtection            Quality of protection. Can be 'authentication', 'integrity' or
   *                                 'privacy'.
   * @throws java.io.IOException if handler could not be created
   */
  public SaslClientHandler(UserGroupInformation ticket, AuthMethod method,
      Token<? extends TokenIdentifier> token, String serverPrincipal, boolean fallbackAllowed,
      String rpcProtection, byte[] connectionHeader, SaslExceptionHandler exceptionHandler,
      SaslSuccessfulConnectHandler successfulConnectHandler) throws IOException {
    this.ticket = ticket;
    this.fallbackAllowed = fallbackAllowed;
    this.connectionHeader = connectionHeader;

    this.exceptionHandler = exceptionHandler;
    this.successfulConnectHandler = successfulConnectHandler;

    saslProps = SaslUtil.initSaslProperties(rpcProtection);
    switch (method) {
    case DIGEST:
      if (LOG.isDebugEnabled())
        LOG.debug("Creating SASL " + AuthMethod.DIGEST.getMechanismName()
            + " client to authenticate to service at " + token.getService());
      saslClient = createDigestSaslClient(new String[] { AuthMethod.DIGEST.getMechanismName() },
          SaslUtil.SASL_DEFAULT_REALM, new HBaseSaslRpcClient.SaslClientCallbackHandler(token));
      break;
    case KERBEROS:
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating SASL " + AuthMethod.KERBEROS.getMechanismName()
            + " client. Server's Kerberos principal name is " + serverPrincipal);
      }
      if (serverPrincipal == null || serverPrincipal.isEmpty()) {
        throw new IOException("Failed to specify server's Kerberos principal name");
      }
      String[] names = SaslUtil.splitKerberosName(serverPrincipal);
      if (names.length != 3) {
        throw new IOException(
            "Kerberos principal does not have the expected format: " + serverPrincipal);
      }
      saslClient = createKerberosSaslClient(new String[] { AuthMethod.KERBEROS.getMechanismName() },
          names[0], names[1]);
      break;
    default:
      throw new IOException("Unknown authentication method " + method);
    }
    if (saslClient == null) {
      throw new IOException("Unable to find SASL client implementation");
    }
  }

  /**
   * Create a Digest Sasl client
   */
  protected SaslClient createDigestSaslClient(String[] mechanismNames, String saslDefaultRealm,
      CallbackHandler saslClientCallbackHandler) throws IOException {
    return Sasl.createSaslClient(mechanismNames, null, null, saslDefaultRealm, saslProps,
        saslClientCallbackHandler);
  }

  /**
   * Create Kerberos client
   *
   * @param userFirstPart  first part of username
   * @param userSecondPart second part of username
   */
  protected SaslClient createKerberosSaslClient(String[] mechanismNames, String userFirstPart,
      String userSecondPart) throws IOException {
    return Sasl
        .createSaslClient(mechanismNames, null, userFirstPart, userSecondPart, saslProps,
            null);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    saslClient.dispose();
  }

  private byte[] evaluateChallenge(final byte[] challenge) throws Exception {
    return ticket.doAs(new PrivilegedExceptionAction<byte[]>() {

      @Override
      public byte[] run() throws Exception {
        return saslClient.evaluateChallenge(challenge);
      }
    });
  }

  @Override
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    saslToken = new byte[0];
    if (saslClient.hasInitialResponse()) {
      saslToken = evaluateChallenge(saslToken);
    }
    if (saslToken != null) {
      writeSaslToken(ctx, saslToken);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have sent token of size " + saslToken.length + " from initSASLContext.");
      }
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf in = (ByteBuf) msg;

    // If not complete, try to negotiate
    if (!saslClient.isComplete()) {
      while (!saslClient.isComplete() && in.isReadable()) {
        readStatus(in);
        int len = in.readInt();
        if (firstRead) {
          firstRead = false;
          if (len == SaslUtil.SWITCH_TO_SIMPLE_AUTH) {
            if (!fallbackAllowed) {
              throw new IOException("Server asks us to fall back to SIMPLE auth, " + "but this "
                  + "client is configured to only allow secure connections.");
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Server asks us to fall back to simple auth.");
            }
            saslClient.dispose();

            ctx.pipeline().remove(this);
            successfulConnectHandler.onSuccess(ctx.channel());
            return;
          }
        }
        saslToken = new byte[len];
        if (LOG.isDebugEnabled()) {
          LOG.debug("Will read input token of size " + saslToken.length
              + " for processing by initSASLContext");
        }
        in.readBytes(saslToken);

        saslToken = evaluateChallenge(saslToken);
        if (saslToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + saslToken.length + " from initSASLContext.");
          }
          writeSaslToken(ctx, saslToken);
        }
      }

      if (saslClient.isComplete()) {
        String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);

        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client context established. Negotiated QoP: " + qop);
        }

        boolean useWrap = qop != null && !"auth".equalsIgnoreCase(qop);

        if (!useWrap) {
          ctx.pipeline().remove(this);
          successfulConnectHandler.onSuccess(ctx.channel());
        } else {
          byte[] wrappedCH = saslClient.wrap(connectionHeader, 0, connectionHeader.length);
          // write connection header
          writeSaslToken(ctx, wrappedCH);
          successfulConnectHandler.onSaslProtectionSucess(ctx.channel());
        }
      }
    }
    // Normal wrapped reading
    else {
      try {
        int length = in.readInt();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Actual length is " + length);
        }
        saslToken = new byte[length];
        in.readBytes(saslToken);
      } catch (IndexOutOfBoundsException e) {
        return;
      }
      try {
        ByteBuf b = ctx.channel().alloc().buffer(saslToken.length);

        b.writeBytes(saslClient.unwrap(saslToken, 0, saslToken.length));
        ctx.fireChannelRead(b);

      } catch (SaslException se) {
        try {
          saslClient.dispose();
        } catch (SaslException ignored) {
          LOG.debug("Ignoring SASL exception", ignored);
        }
        throw se;
      }
    }
  }

  private void writeSaslToken(final ChannelHandlerContext ctx, byte[] saslToken) {
    ByteBuf b = ctx.alloc().buffer(4 + saslToken.length);
    b.writeInt(saslToken.length);
    b.writeBytes(saslToken, 0, saslToken.length);
    ctx.writeAndFlush(b).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          exceptionCaught(ctx, future.cause());
        }
      }
    });
  }

  /**
   * Get the read status
   */
  private static void readStatus(ByteBuf inStream) throws RemoteException {
    int status = inStream.readInt(); // read status
    if (status != SaslStatus.SUCCESS.state) {
      throw new RemoteException(inStream.toString(Charset.forName("UTF-8")),
          inStream.toString(Charset.forName("UTF-8")));
    }
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
      throws Exception {
    saslClient.dispose();

    ctx.close();

    if (this.random == null) {
      this.random = new Random();
    }
    exceptionHandler.handle(this.retryCount++, this.random, cause);
  }

  @Override
  public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    // If not complete, try to negotiate
    if (!saslClient.isComplete()) {
      super.write(ctx, msg, promise);
    } else {
      ByteBuf in = (ByteBuf) msg;
      byte[] unwrapped = new byte[in.readableBytes()];
      in.readBytes(unwrapped);
      // release the memory
      in.release();

      try {
        saslToken = saslClient.wrap(unwrapped, 0, unwrapped.length);
      } catch (SaslException se) {
        try {
          saslClient.dispose();
        } catch (SaslException ignored) {
          LOG.debug("Ignoring SASL exception", ignored);
        }
        promise.setFailure(se);
      }
      if (saslToken != null) {
        ByteBuf out = ctx.channel().alloc().buffer(4 + saslToken.length);
        out.writeInt(saslToken.length);
        out.writeBytes(saslToken, 0, saslToken.length);

        ctx.write(out).addListener(new ChannelFutureListener() {
          @Override public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
              exceptionCaught(ctx, future.cause());
            }
          }
        });

        saslToken = null;
      }
    }
  }

  /**
   * Handler for exceptions during Sasl connection
   */
  public interface SaslExceptionHandler {
    /**
     * Handle the exception
     *
     * @param retryCount current retry count
     * @param random     to create new backoff with
     */
    public void handle(int retryCount, Random random, Throwable cause);
  }

  /**
   * Handler for successful connects
   */
  public interface SaslSuccessfulConnectHandler {
    /**
     * Runs on success
     *
     * @param channel which is successfully authenticated
     */
    public void onSuccess(Channel channel);

    /**
     * Runs on success if data protection used in Sasl
     *
     * @param channel which is successfully authenticated
     */
    public void onSaslProtectionSucess(Channel channel);
  }
}

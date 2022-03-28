/**
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
package org.apache.hadoop.hbase.client.example;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.bootstrap.ServerBootstrap;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInitializer;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.channel.group.ChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.group.DefaultChannelGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.HttpVersion;
import org.apache.hbase.thirdparty.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GlobalEventExecutor;

/**
 * A simple example on how to use {@link org.apache.hadoop.hbase.client.AsyncTable} to write a fully
 * asynchronous HTTP proxy server. The {@link AsyncConnection} will share the same event loop with
 * the HTTP server.
 * <p>
 * The request URL is:
 *
 * <pre>
 * http://&lt;host&gt;:&lt;port&gt;/&lt;table&gt;/&lt;rowgt;/&lt;family&gt;:&lt;qualifier&gt;
 * </pre>
 *
 * Use HTTP GET to fetch data, and use HTTP PUT to put data. Encode the value as the request content
 * when doing PUT.
 */
@InterfaceAudience.Private
public class HttpProxyExample {

  private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);

  private final EventLoopGroup workerGroup = new NioEventLoopGroup();

  private final Configuration conf;

  private final int port;

  private AsyncConnection conn;

  private Channel serverChannel;

  private ChannelGroup channelGroup;

  public HttpProxyExample(Configuration conf, int port) {
    this.conf = conf;
    this.port = port;
  }

  private static final class Params {
    public final String table;

    public final String row;

    public final String family;

    public final String qualifier;

    public Params(String table, String row, String family, String qualifier) {
      this.table = table;
      this.row = row;
      this.family = family;
      this.qualifier = qualifier;
    }
  }

  private static final class RequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final AsyncConnection conn;

    private final ChannelGroup channelGroup;

    public RequestHandler(AsyncConnection conn, ChannelGroup channelGroup) {
      this.conn = conn;
      this.channelGroup = channelGroup;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      channelGroup.add(ctx.channel());
      ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      channelGroup.remove(ctx.channel());
      ctx.fireChannelInactive();
    }

    private void write(ChannelHandlerContext ctx, HttpResponseStatus status) {
      write(ctx, status, null);
    }

    private void write(ChannelHandlerContext ctx, HttpResponseStatus status,
        String content) {
      DefaultFullHttpResponse resp;
      if (content != null) {
        ByteBuf buf = ctx.alloc().buffer().writeBytes(Bytes.toBytes(content));
        resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, buf);
        resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, buf.readableBytes());
      } else {
        resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
      }
      resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text-plain; charset=UTF-8");
      ctx.writeAndFlush(resp);
    }

    private Params parse(FullHttpRequest req) {
      String[] components = new QueryStringDecoder(req.uri()).path().split("/");
      Preconditions.checkArgument(components.length == 4, "Unrecognized uri: %s", req.uri());
      // path is start with '/' so split will give an empty component
      String[] cfAndCq = components[3].split(":");
      Preconditions.checkArgument(cfAndCq.length == 2, "Unrecognized uri: %s", req.uri());
      return new Params(components[1], components[2], cfAndCq[0], cfAndCq[1]);
    }

    private void get(ChannelHandlerContext ctx, FullHttpRequest req) {
      Params params = parse(req);
      addListener(
        conn.getTable(TableName.valueOf(params.table)).get(new Get(Bytes.toBytes(params.row))
          .addColumn(Bytes.toBytes(params.family), Bytes.toBytes(params.qualifier))),
        (r, e) -> {
          if (e != null) {
            exceptionCaught(ctx, e);
          } else {
            byte[] value =
              r.getValue(Bytes.toBytes(params.family), Bytes.toBytes(params.qualifier));
            if (value != null) {
              write(ctx, HttpResponseStatus.OK, Bytes.toStringBinary(value));
            } else {
              write(ctx, HttpResponseStatus.NOT_FOUND);
            }
          }
        });
    }

    private void put(ChannelHandlerContext ctx, FullHttpRequest req) {
      Params params = parse(req);
      byte[] value = new byte[req.content().readableBytes()];
      req.content().readBytes(value);
      addListener(
        conn.getTable(TableName.valueOf(params.table)).put(new Put(Bytes.toBytes(params.row))
          .addColumn(Bytes.toBytes(params.family), Bytes.toBytes(params.qualifier), value)),
        (r, e) -> {
          if (e != null) {
            exceptionCaught(ctx, e);
          } else {
            write(ctx, HttpResponseStatus.OK);
          }
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
      switch (req.method().name()) {
        case "GET":
          get(ctx, req);
          break;
        case "PUT":
          put(ctx, req);
          break;
        default:
          write(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED);
          break;
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof IllegalArgumentException) {
        write(ctx, HttpResponseStatus.BAD_REQUEST, cause.getMessage());
      } else {
        write(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
          Throwables.getStackTraceAsString(cause));
      }
    }
  }

  public void start() throws InterruptedException, ExecutionException {
    NettyRpcClientConfigHelper.setEventLoopConfig(conf, workerGroup, NioSocketChannel.class);
    conn = ConnectionFactory.createAsyncConnection(conf).get();
    channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    serverChannel = new ServerBootstrap().group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class).childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childHandler(new ChannelInitializer<Channel>() {

          @Override
          protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addFirst(new HttpServerCodec(), new HttpObjectAggregator(4 * 1024 * 1024),
              new RequestHandler(conn, channelGroup));
          }
        }).bind(port).syncUninterruptibly().channel();
  }

  public void join() {
    serverChannel.closeFuture().awaitUninterruptibly();
  }

  public int port() {
    if (serverChannel == null) {
      return port;
    } else {
      return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }
  }

  public void stop() throws IOException {
    serverChannel.close().syncUninterruptibly();
    serverChannel = null;
    channelGroup.close().syncUninterruptibly();
    channelGroup = null;
    conn.close();
    conn = null;
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    int port = Integer.parseInt(args[0]);
    HttpProxyExample proxy = new HttpProxyExample(HBaseConfiguration.create(), port);
    proxy.start();
    proxy.join();
  }
}

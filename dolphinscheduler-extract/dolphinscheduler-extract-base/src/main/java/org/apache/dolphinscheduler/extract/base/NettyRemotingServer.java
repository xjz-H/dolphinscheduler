/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.extract.base;

import org.apache.dolphinscheduler.extract.base.config.NettyServerConfig;
import org.apache.dolphinscheduler.extract.base.exception.RemoteException;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterDecoder;
import org.apache.dolphinscheduler.extract.base.protocal.TransporterEncoder;
import org.apache.dolphinscheduler.extract.base.server.JdkDynamicServerHandler;
import org.apache.dolphinscheduler.extract.base.server.ServerMethodInvoker;
import org.apache.dolphinscheduler.extract.base.utils.Constants;
import org.apache.dolphinscheduler.extract.base.utils.NettyUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * remoting netty server
 */
@Slf4j
public class NettyRemotingServer {

    /***
     * ServerBootstrap是Netty中用于创建和配置服务器的引导类。它是Netty服务器端的主要入口点，用于启动和管理服务器。
     *
     * 在Netty中，通过ServerBootstrap可以配置服务器的各种参数和属性，包括事件循环组、Channel类型、Channel处理器等。
     * 通常创建一个ServerBootstrap实例，并通过一系列方法来配置服务器端的参数，最后调用bind方法绑定端口并启动服务器。
     */
    // Netty服务启动程序
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();

    private final ExecutorService defaultExecutor =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    // netty的eventLoop组，主要负责连接
    private final EventLoopGroup bossGroup;
    // netty的eventLoop组，主要读写事件
    private final EventLoopGroup workGroup;

    private final NettyServerConfig serverConfig;

    private final JdkDynamicServerHandler serverHandler = new JdkDynamicServerHandler(this);

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    private static final String NETTY_BIND_FAILURE_MSG = "NettyRemotingServer bind %s fail";
    // 为每个NettyRemotingServer定义一个配置类
    public NettyRemotingServer(final NettyServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        /***
         * ThreadFactoryBuilder是Google Guava库中的一个工具类，用于创建ThreadFactory实例，
         * 用于创建新线程。ThreadFactory是用于创建线程的工厂接口，它提供了一种在创建线程时可以指定一些参数的方式，如线程名称、优先级、是否为守护线程等。
         *
         * ThreadFactoryBuilder类提供了一些静态方法和实例方法，
         * 用于配置和创建ThreadFactory实例。通过ThreadFactoryBuilder可以方便地创建具有自定义配置的ThreadFactory实例。
         */
        ThreadFactory bossThreadFactory =
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("NettyServerBossThread_%s").build();
        ThreadFactory workerThreadFactory =
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("NettyServerWorkerThread_%s").build();
        /***
         * 在Java中，Epoll.isAvailable()是一个静态方法，用于检查当前系统是否支持使用Epoll作为Java NIO的实现。
         * Epoll是Linux操作系统上的一种高性能事件通知机制，可以用于实现高性能的非阻塞I/O操作。
         *
         * 当调用Epoll.isAvailable()方法时，它将会返回一个布尔值，表示当前系统是否支持使用Epoll。
         * 如果返回值为true，则表示当前系统支持使用Epoll；如果返回值为false，则表示当前系统不支持使用Epoll。
         *
         * 通常，在使用Java NIO进行网络编程时，可以先通过调用Epoll.isAvailable()方法来检查系统是否支持Epoll，
         * 然后根据返回值决定是否使用Epoll，以提高网络I/O的性能。
         */
        if (Epoll.isAvailable()) {
            this.bossGroup = new EpollEventLoopGroup(1, bossThreadFactory);
            this.workGroup = new EpollEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        } else {
            this.bossGroup = new NioEventLoopGroup(1, bossThreadFactory);
            this.workGroup = new NioEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        }
    }
    // 启动服务...
    public void start() {
        // 服务启动--乐观锁
        if (isStarted.compareAndSet(false, true)) {
            this.serverBootstrap
                    .group(this.bossGroup, this.workGroup)
                    .channel(NettyUtils.getServerSocketChannelClass())
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog())
                    .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isSoKeepalive())
                    .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                    .childOption(ChannelOption.SO_SNDBUF, serverConfig.getSendBufferSize())
                    .childOption(ChannelOption.SO_RCVBUF, serverConfig.getReceiveBufferSize())
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel ch) {
                            initNettyChannel(ch);
                        }
                    });

            ChannelFuture future;
            try {
                // 开启一个netty服务 ChannelFuture
                future = serverBootstrap.bind(serverConfig.getListenPort()).sync();
            } catch (Exception e) {
                log.error("NettyRemotingServer bind fail {}, exit", e.getMessage(), e);
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()));
            }
            if (future.isSuccess()) {
                log.info("NettyRemotingServer bind success at port : {}", serverConfig.getListenPort());
            } else if (future.cause() != null) {
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()),
                        future.cause());
            } else {
                throw new RemoteException(String.format(NETTY_BIND_FAILURE_MSG, serverConfig.getListenPort()));
            }
        }
    }

    /**
     * init netty channel
     *
     * @param ch socket channel
     */
    private void initNettyChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast("encoder", new TransporterEncoder())
                .addLast("decoder", new TransporterDecoder())
                .addLast("server-idle-handle",
                        new IdleStateHandler(0, 0, Constants.NETTY_SERVER_HEART_BEAT_TIME, TimeUnit.MILLISECONDS))
                .addLast("handler", serverHandler);
    }

    public ExecutorService getDefaultExecutor() {
        return defaultExecutor;
    }

    public void registerMethodInvoker(ServerMethodInvoker methodInvoker) {
        serverHandler.registerMethodInvoker(methodInvoker);
    }

    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                if (bossGroup != null) {
                    this.bossGroup.shutdownGracefully();
                }
                if (workGroup != null) {
                    this.workGroup.shutdownGracefully();
                }
                defaultExecutor.shutdown();
            } catch (Exception ex) {
                log.error("netty server close exception", ex);
            }
            log.info("netty server closed");
        }
    }
}

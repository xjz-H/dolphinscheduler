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

package org.apache.dolphinscheduler.extract.base.server;

import org.apache.dolphinscheduler.extract.base.NettyRemotingServer;
import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;

import java.lang.reflect.Method;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.Nullable;

/***
 * 这里定义了一个后置处理器，后置处理器会被加载spring后置处理器的集合中，当每个bean调用getBean方法都会去遍历后置处理器的集合，调用后缀处理器的方法
 */
@Slf4j
public class SpringServerMethodInvokerDiscovery implements BeanPostProcessor {

    // 定义成protected 让子类能够访问到。通过组合的方式引入NettyRemotingServer
    protected final NettyRemotingServer nettyRemotingServer;

    public SpringServerMethodInvokerDiscovery(NettyRemotingServer nettyRemotingServer) {
        this.nettyRemotingServer = nettyRemotingServer;
    }
    // 每个bean 加载的时候都会去调用该方法，判断该bean是否有RpcService注解
    @Nullable
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // 如果spring加载的bean 实现了加了@RpcService 注解的接口
        Class<?>[] interfaces = bean.getClass().getInterfaces();
        for (Class<?> anInterface : interfaces) {
            if (anInterface.getAnnotation(RpcService.class) == null) {
                continue;
            }
            // 接口，bean ，beanName封装成methodInvoker
            registerRpcMethodInvoker(anInterface, bean, beanName);
        }
        return bean;
    }

    private void registerRpcMethodInvoker(Class<?> anInterface, Object bean, String beanName) {
        Method[] declaredMethods = anInterface.getDeclaredMethods();
        for (Method method : declaredMethods) {
            RpcMethod rpcMethod = method.getAnnotation(RpcMethod.class);
            if (rpcMethod == null) {
                continue;
            }
            // 封装Method 调用: bean,method
            ServerMethodInvoker methodInvoker = new ServerMethodInvokerImpl(bean, method);
            // 注册到concurrentHashMap中
            nettyRemotingServer.registerMethodInvoker(methodInvoker);
            log.info("Register ServerMethodInvoker: {} to bean: {}", methodInvoker.getMethodIdentify(), beanName);
        }
    }
}

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

package org.apache.dolphinscheduler.server.master.event;

import org.apache.dolphinscheduler.common.enums.StateEventType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public class StateEventHandlerManager {

    private static final Map<StateEventType, StateEventHandler> stateEventHandlerMap = new HashMap<>();
    //写在静态代码块中来初始化stateEventHandlerMap 是线程安全的
    static {
        ServiceLoader.load(StateEventHandler.class)
                .forEach(stateEventHandler -> stateEventHandlerMap.put(stateEventHandler.getEventType(),
                        stateEventHandler));
    }

    public static Optional<StateEventHandler> getStateEventHandler(StateEventType stateEventType) {
        return Optional.ofNullable(stateEventHandlerMap.get(stateEventType));
    }

}
/***
 * ServiceLoader 是 Java 提供的一个用于加载和提供服务实现的机制，它可以实现在运行时动态加载并发现实现了特定接口的服务提供者。ServiceLoader 主要用于实现服务提供者框架，它使得应用程序能够通过配置文件或其他方式注册和加载服务的实现，而不需要在代码中硬编码具体的实现类。
 *
 * 下面是 ServiceLoader 的主要特点和使用方式：
 *
 * 服务接口：首先要定义一个服务接口，用来描述服务提供者需要实现的方法或功能。
 *
 * 服务提供者：服务提供者是指实现了服务接口并提供具体功能的类。
 *
 * 服务提供者配置文件：ServiceLoader 通过读取配置文件来加载服务提供者。配置文件的路径通常是META-INF/services/接口全限定名，文件内容为实现了接口的服务提供者类的全限定名。
 *
 * 使用方式：通过 ServiceLoader.load() 方法加载服务提供者，在需要使用服务的地方，可以通过 ServiceLoader.iterator() 获取服务提供者的迭代器，然后遍历迭代器获取具体的服务实现。
 *
 * 以下是一个简单的示例代码，演示了如何使用 ServiceLoader 加载服务提供者：
 *
 * 假设有一个服务接口 MyService：
 * // MyService.java
 * public interface MyService {
 *     void doSomething();
 * }
 * 有两个实现类分别为 MyServiceImpl1 和 MyServiceImpl2：
 * // MyServiceImpl1.java
 * public class MyServiceImpl1 implements MyService {
 *     public void doSomething() {
 *         System.out.println("MyServiceImpl1 is doing something.");
 *     }
 * }
 *
 * // MyServiceImpl2.java
 * public class MyServiceImpl2 implements MyService {
 *     public void doSomething() {
 *         System.out.println("MyServiceImpl2 is doing something.");
 *     }
 * }
 * 在 META-INF/services 目录下创建配置文件 MyService，内容如下：
 * com.example.MyServiceImpl1
 * com.example.MyServiceImpl2
 * 使用 ServiceLoader 加载并使用服务提供者：
 * // Main.java
 * import java.util.ServiceLoader;
 *
 * public class Main {
 *     public static void main(String[] args) {
 *         ServiceLoader<MyService> serviceLoader = ServiceLoader.load(MyService.class);
 *         for (MyService service : serviceLoader) {
 *             service.doSomething();
 *         }
 *     }
 * }
 */

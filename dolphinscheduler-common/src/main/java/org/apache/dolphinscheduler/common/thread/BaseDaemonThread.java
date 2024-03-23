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

package org.apache.dolphinscheduler.common.thread;

/**
 * All thread used in DolphinScheduler should extend with this class to avoid the server hang issue.
 *  所有在DolphinScheduler中使用的线程都应该扩展这个类，以避免服务器挂起问题。
 *  q: 为什么会避免服务挂起？       a: 因为这个类设置了所有继承该类的线程都设置成了守护线程
 *  q: 守护线程为什么能避免服务挂起? a: 守护线程是一种支持线程，当所有的非守护线程结束时，守护线程也会结束
 *  q: 什么是服务挂起？
 */
public abstract class BaseDaemonThread extends Thread {

    protected BaseDaemonThread(Runnable runnable) {
        //调用父类的构造方法
        super(runnable);
        //把所有继承该类的线程都设置成了守护线程
        this.setDaemon(true);
    }

    protected BaseDaemonThread(String threadName) {
        super();
        this.setName(threadName);
        //把所有继承该类的线程都设置成了守护线程
        this.setDaemon(true);
    }

}
// 这是一个值得学习的地方，写一个抽象类，这个抽象类第定义了这个线程是一个守护线程的属性，定义两个protected的构造方法

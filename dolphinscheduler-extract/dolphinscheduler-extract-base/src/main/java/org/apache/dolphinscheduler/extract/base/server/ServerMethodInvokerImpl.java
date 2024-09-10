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

import java.lang.reflect.Method;

public class ServerMethodInvokerImpl implements ServerMethodInvoker {

    private final Object serviceBean;

    private final Method method;

    private final String methodIdentify;

    public ServerMethodInvokerImpl(Object serviceBean, Method method) {
        this.serviceBean = serviceBean;
        this.method = method;
        /****
         * toGenericString() 方法返回的是一个字符串，
         * 包含了方法的修饰符、返回类型、方法名、参数类型以及泛型信息等。
         * 这个方法返回的字符串更详细，包含了比 toString() 方法更多的信息，特别是在涉及到泛型的情况下。
         */
        this.methodIdentify = method.toGenericString();
    }

    @Override
    public Object invoke(Object... args) throws Throwable {
        // todo: check the request param when register
        return method.invoke(serviceBean, args);
    }

    @Override
    public String getMethodIdentify() {
        return methodIdentify;
    }
}

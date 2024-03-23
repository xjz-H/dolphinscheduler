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

package org.apache.dolphinscheduler.plugin.storage.s3;

import org.apache.dolphinscheduler.plugin.storage.api.StorageOperate;
import org.apache.dolphinscheduler.plugin.storage.api.StorageOperateFactory;
import org.apache.dolphinscheduler.plugin.storage.api.StorageType;

import com.google.auto.service.AutoService;

/***
 *  @AutoService 这是一个值得学习的注解，
 *  @AutoService 是Google提供的一个注解，主要用于自动生成Java标准服务提供者接口（Service Provider Interface，SPI）的配置文件。在这里提到的StorageOperateFactory应该是一个接口，用于定义存储操作工厂的行为。
 *
 * 当我们在一个实现了StorageOperateFactory接口的类上使用@AutoService(StorageOperateFactory.class)注解时，编译器会在编译时自动生成一个$META-INF/services$目录下的配置文件，文件名为StorageOperateFactory，内容为该类的全限定名。
 *
 * 通过生成的配置文件，可以实现Java的SPI机制，允许其他模块或框架在运行时动态发现并加载这个实现类，而不需要显式地在代码中指定。这样可以方便地实现插件化开发、扩展性设计等功能。
 *
 * 总结一下，@AutoService注解的作用是自动生成Java SPI配置文件，用于标识实现了某个接口的类，使得这些实现类可以被其他模块或框架动态加载，实现解耦和扩展性。
 *
 * 通过这个注解，我们可以实现插件化开发，提高代码的灵活性和可扩展性。
 * 动态的去加载这个实现类，而不需要在代码中显式的指定。其他模块使用到的时候才会去加载。
 */
@AutoService(StorageOperateFactory.class)
public class S3StorageOperatorFactory implements StorageOperateFactory {

    @Override
    public StorageOperate createStorageOperate() {
        S3StorageOperator s3StorageOperator = new S3StorageOperator();
        s3StorageOperator.init();
        return s3StorageOperator;
    }

    @Override
    public StorageType getStorageOperate() {
        return StorageType.S3;
    }
}

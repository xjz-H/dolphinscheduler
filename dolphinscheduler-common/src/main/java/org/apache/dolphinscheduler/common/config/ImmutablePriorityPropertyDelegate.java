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

package org.apache.dolphinscheduler.common.config;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * This class will get the property by the priority of the following: env > jvm > properties.
 */
@Slf4j
public class ImmutablePriorityPropertyDelegate extends ImmutablePropertyDelegate {

    private static final Map<String, Optional<ConfigValue<String>>> configValueMap = new ConcurrentHashMap<>();

    public ImmutablePriorityPropertyDelegate(String propertyAbsolutePath) {
        super(propertyAbsolutePath);
    }

    @Override
    public String get(String key) {
        // computeIfAbsent 如果map存在对应的key 就放入到map中，如果不存在就通过计算后 产生的值放入到map中
        Optional<ConfigValue<String>> configValue = configValueMap.computeIfAbsent(key, k -> {
            Optional<ConfigValue<String>> value = getConfigValueFromEnv(key);
            if (value.isPresent()) {
                log.debug("Override config value from env, key: {} actualKey: {}, value: {}",
                        k,
                        value.get().getActualKey(), value.get().getValue());
                return value;
            }
            value = getConfigValueFromJvm(key);
            if (value.isPresent()) {
                log.debug("Override config value from jvm, key: {} actualKey: {}, value: {}",
                        k, value.get().getActualKey(), value.get().getValue());
                return value;
            }
            value = getConfigValueFromProperties(key);
            value.ifPresent(
                    stringConfigValue -> log.debug("Get config value from properties, key: {} actualKey: {}, value: {}",
                            k, stringConfigValue.getActualKey(), stringConfigValue.getValue()));
            return value;
        });
        return configValue.map(ConfigValue::getValue).orElse(null);
    }

    @Override
    public String get(String key, String defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> propertyKeys = new HashSet<>();
        // 1、从property 文件中获取值
        propertyKeys.addAll(super.getPropertyKeys());
        // 2、从jvm系统获取设置的值
        propertyKeys.addAll(System.getProperties().stringPropertyNames());
        // 3、从操作系统重获取设置的值
        propertyKeys.addAll(System.getenv().keySet());
        return propertyKeys;
    }

    private Optional<ConfigValue<String>> getConfigValueFromEnv(String key) {
        /**
         * 1、 获取操作系统的环境变量的值
         * 2、 操作系统环境变量是在操作系统级别设置的变量，可以在系统环境中定义和配置。
         *
          */

        String value = System.getenv(key);
        if (value != null) {
            return Optional.of(ConfigValue.fromEnv(key, value));
        }
        // 将.和- 替换成—_ 并转化为大写
        String envVarKey = String.valueOf(key).replaceAll("[.-]", "_").toUpperCase();
        String envVarVal = System.getenv(envVarKey);
        if (envVarVal != null) {
            return Optional.of(ConfigValue.fromEnv(key, envVarVal));
        }
        return Optional.empty();
    }

    private Optional<ConfigValue<String>> getConfigValueFromJvm(String key) {
        /***
         * 1、 这个方法用于获取 Java 系统属性的值
         * 2、Java 系统属性是由 Java 虚拟机（JVM）或应用程序设置的属性。
         * 3、Java 系统属性可以通过启动参数（如 -D 参数）、代码中的 System.setProperty(key, value) 方法或其他方式设置。
         */

        String value = System.getProperty(key);
        if (value != null) {
            return Optional.of(ConfigValue.fromJvm(key, value));
        }
        return Optional.empty();
    }

    private Optional<ConfigValue<String>> getConfigValueFromProperties(String key) {
        /***
         * 1、从 property配置文件中获取配置的值
         */
        String value = super.get(key);
        if (value != null) {
            return Optional.of(ConfigValue.fromProperties(key, value));
        }
        return Optional.empty();
    }

    @Data
    @AllArgsConstructor
    public static final class ConfigValue<T> {

        private String actualKey;
        private T value;
        private boolean fromProperties;
        private boolean fromJvm;
        private boolean fromEnv;

        public static <T> ConfigValue<T> fromProperties(String actualKey, T value) {
            return new ConfigValue<>(actualKey, value, true, false, false);
        }

        public static <T> ConfigValue<T> fromJvm(String actualKey, T value) {
            return new ConfigValue<>(actualKey, value, false, true, false);
        }

        public static <T> ConfigValue<T> fromEnv(String actualKey, T value) {
            return new ConfigValue<>(actualKey, value, false, false, true);
        }
    }

}

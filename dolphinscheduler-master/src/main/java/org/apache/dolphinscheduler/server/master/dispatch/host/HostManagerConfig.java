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

package org.apache.dolphinscheduler.server.master.dispatch.host;

import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.dispatch.host.assign.HostSelector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 主键管理配置类
 */
@Configuration
public class HostManagerConfig {

    /**
     * host manager config
     */
    private AutowireCapableBeanFactory beanFactory;

    @Autowired
    private MasterConfig masterConfig;

    @Autowired
    public HostManagerConfig(AutowireCapableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /***
     * 有些配置类是通过在配置类中的方法中注解添加到容器中的
     * @return
     */
    @Bean
    public HostManager hostManager() {
        HostSelector selector = masterConfig.getHostSelector();
        HostManager hostManager;
        switch (selector) {
            case RANDOM:
                hostManager = new RandomHostManager();
                break;
            case ROUND_ROBIN:
                hostManager = new RoundRobinHostManager();
                break;
            case LOWER_WEIGHT:
                hostManager = new LowerWeightHostManager();
                break;
            default:
                throw new IllegalArgumentException("unSupport selector " + selector);
        }
        beanFactory.autowireBean(hostManager);
        return hostManager;
    }
}

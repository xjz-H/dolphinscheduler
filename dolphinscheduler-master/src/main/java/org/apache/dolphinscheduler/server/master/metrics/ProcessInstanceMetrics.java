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

package org.apache.dolphinscheduler.server.master.metrics;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableSet;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

/***
 * @UtilityClass 这是一个lombok 工具类的注解，
 * 1、可以将类中的所有方法都变成静态方法，不需要再加static关键字
 * 2、为这个类创建一个私有的构造方法，并在构造方法中抛出一个异常防止通过构造方法创建对象
 * 3、将这个类标记为final，不允许被继承
 */

// 这个类的作用   用于记录工作流实例的指标    这些指标在什么时候被使用呢？
@UtilityClass
@Slf4j
public class ProcessInstanceMetrics {

    private final Set<String> processInstanceStates = ImmutableSet.of(
            "submit", "timeout", "finish", "failover", "success", "fail", "stop");

    static {
        for (final String state : processInstanceStates) {
            Counter.builder("ds.workflow.instance.count")
                    .tags("state", state, "process.definition.code", "dummy")
                    .description(String.format("Process instance total count by state and definition code"))
                    .register(Metrics.globalRegistry);
        }

    }
   //创建一个计时器
    //Metrics.globalRegistry 是一个全局的注册表，用于存储所有的指标 ，是被记录到内存中吗？    是的，是被记录到内存中的
    private final Timer commandQueryTimer =
            Timer.builder("ds.workflow.command.query.duration")
                    .description("Command query duration")
                    .register(Metrics.globalRegistry);

    private final Timer processInstanceGenerateTimer =
            Timer.builder("ds.workflow.instance.generate.duration")
                    .description("Process instance generated duration")
                    .register(Metrics.globalRegistry);

    /***
     * 记录命令查询时间，这个时间会被记录在commandQueryTimer中
     * @param milliseconds
     */
    public void recordCommandQueryTime(long milliseconds) {
        commandQueryTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    public void recordProcessInstanceGenerateTime(long milliseconds) {
        processInstanceGenerateTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    public synchronized void registerProcessInstanceRunningGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.running", function)
                .description("The current running process instance count")
                .register(Metrics.globalRegistry);
    }

    public synchronized void registerProcessInstanceResubmitGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.resubmit", function)
                .description("The current process instance need to resubmit count")
                .register(Metrics.globalRegistry);
    }

    public void incProcessInstanceByStateAndProcessDefinitionCode(final String state,
                                                                  final String processDefinitionCode) {
        // When tags need to be determined from local context,
        // you have no choice but to construct or lookup the Meter inside your method body.
        // The lookup cost is just a single hash lookup, so it is acceptable for most use cases.
        Metrics.globalRegistry.counter(
                "ds.workflow.instance.count",
                "state", state,
                "process.definition.code", processDefinitionCode)
                .increment();
    }

    public void cleanUpProcessInstanceCountMetricsByDefinitionCode(final Long processDefinitionCode) {
        for (final String state : processInstanceStates) {
            final Counter counter = Metrics.globalRegistry.counter(
                    "ds.workflow.instance.count",
                    "state", state,
                    "process.definition.code", String.valueOf(processDefinitionCode));
            Metrics.globalRegistry.remove(counter);
        }
    }

}

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

package org.apache.dolphinscheduler.plugin.task.api.enums;

import java.util.HashMap;
import java.util.Map;

import com.baomidou.mybatisplus.annotation.EnumValue;

public enum TaskExecutionStatus {
    //第一次根据工作流实例DAG切分出来的任务实例的状态是SUBMITTED_SUCCESS
    SUBMITTED_SUCCESS(0, "submit success"),
    RUNNING_EXECUTION(1, "running"),
    PAUSE(3, "pause"),
    STOP(5, "stop"),
    FAILURE(6, "failure"),
    SUCCESS(7, "success"),
    // 容错
    NEED_FAULT_TOLERANCE(8, "need fault tolerance"),
    KILL(9, "kill"),
    DELAY_EXECUTION(12, "delay execution"),
    FORCED_SUCCESS(13, "forced success"),
    DISPATCH(17, "dispatch"),

    ;
    // 这里是一个巧妙的设计，把枚举的code和其实例通过静态代码块一次初始化到map中，并提供静态方法of（静态工厂模式来进行获取）
    private static final Map<Integer, TaskExecutionStatus> CODE_MAP = new HashMap<>();
    // 进行一次初始化时间复杂度是O(n)
    static {
        for (TaskExecutionStatus executionStatus : TaskExecutionStatus.values()) {
            CODE_MAP.put(executionStatus.getCode(), executionStatus);
        }
    }

    /**
     * Get <code>TaskExecutionStatus</code> by code, if the code is invalidated will throw {@link IllegalArgumentException}.
     */
    // 这是一个值得学习的设计。

    /***
     *在 Java 中，通常情况下，of 方法的语义是用于创建一个新的对象实例，通常是一个不可变的对象，
     * 且该方法用于接受一些参数来初始化对象的状态。这种设计模式通常被称为静态工厂方法或者静态工厂模式。
     * 常见的使用场景包括：
     *
     * 1、构建不可变对象：of 方法通常被用来创建不可变对象，即对象的状态在创建后不可修改。
     *
     * 2、参数校验：of 方法通常会对传入的参数进行校验，以确保创建的对象状态是有效的。
     *
     * 3、简化对象创建：通过使用of 方法，可以在不直接调用构造函数的情况下创建对象，提供了更加简洁的方式。
     *
     *of 强调的是创建一个不可变的对象
     */
    public static TaskExecutionStatus of(int code) {
        TaskExecutionStatus taskExecutionStatus = CODE_MAP.get(code);
        if (taskExecutionStatus == null) {
            throw new IllegalArgumentException(String.format("The task execution status code: %s is invalidated",
                    code));
        }
        return taskExecutionStatus;
    }

    public boolean isRunning() {
        return this == RUNNING_EXECUTION;
    }

    public boolean isSuccess() {
        return this == TaskExecutionStatus.SUCCESS;
    }

    public boolean isForceSuccess() {
        return this == TaskExecutionStatus.FORCED_SUCCESS;
    }

    public boolean isKill() {
        return this == TaskExecutionStatus.KILL;
    }

    public boolean isFailure() {
        return this == TaskExecutionStatus.FAILURE || this == NEED_FAULT_TOLERANCE;
    }

    public boolean isPause() {
        return this == TaskExecutionStatus.PAUSE;
    }

    public boolean isStop() {
        return this == TaskExecutionStatus.STOP;
    }
    // finished状态有：成功，杀死，失败，暂停，停止，强制成功
    public boolean isFinished() {
        return isSuccess() || isKill() || isFailure() || isPause() || isStop() || isForceSuccess();
    }

    public boolean isNeedFaultTolerance() {
        return this == NEED_FAULT_TOLERANCE;
    }

    public boolean shouldFailover() {
        //SUBMITTED_SUCCESS 状态表示封装了taskInstance的task执行的runnable并添加到队列中后的状态。
        //DISPATCH 状态是将任务分配到worker 后的状态
        return SUBMITTED_SUCCESS == this
                || DISPATCH == this
                || RUNNING_EXECUTION == this
                || DELAY_EXECUTION == this;
    }

    @EnumValue
    private final int code;
    private final String desc;

    TaskExecutionStatus(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return "TaskExecutionStatus{" + "code=" + code + ", desc='" + desc + '\'' + '}';
    }

}

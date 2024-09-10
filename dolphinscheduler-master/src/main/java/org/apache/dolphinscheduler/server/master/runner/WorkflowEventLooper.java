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

package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.event.WorkflowEvent;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventHandleError;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventHandleException;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventHandler;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventQueue;
import org.apache.dolphinscheduler.server.master.event.WorkflowEventType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WorkflowEventLooper extends BaseDaemonThread implements AutoCloseable {

    @Autowired
    private WorkflowEventQueue workflowEventQueue;
    // @Autowired 将所有类型为WorkflowEventHandler的bean注入到list中，这个list 的初始化直接使用spring 的bean 注入来完成
    @Autowired
    private List<WorkflowEventHandler> workflowEventHandlerList;
    // WorkflowEventType工作流事件类型，WorkflowEventHandler 工作流事件handler
    private final Map<WorkflowEventType, WorkflowEventHandler> workflowEventHandlerMap = new HashMap<>();

    private final AtomicBoolean RUNNING_FLAG = new AtomicBoolean(false);

    protected WorkflowEventLooper() {
        super("WorkflowEventLooper");
    }
    // 其实这个也可以使用静态代码块来完成初始化，这一套初始化使用spring bean 来进行初始化
    @PostConstruct
    public void init() {
        workflowEventHandlerList.forEach(
                workflowEventHandler -> workflowEventHandlerMap.put(workflowEventHandler.getHandleWorkflowEventType(),
                        workflowEventHandler));
    }

    @Override
    public synchronized void start() {
        // 使用无锁机制启动这消费事件的守护线程
        if (!RUNNING_FLAG.compareAndSet(false, true)) {
            log.error("WorkflowEventLooper thread has already started, will not start again");
            return;
        }
        log.info("WorkflowEventLooper starting...");
        // 再去调用父类的start 方法
        super.start();// 异步
        log.info("WorkflowEventLooper started...");
    }
    @Override
    public void run() {
        WorkflowEvent workflowEvent;
        while (RUNNING_FLAG.get()) {
            try {
                // 先从队列中获取transfer后生产的事件
                workflowEvent = workflowEventQueue.poolEvent();
                // take 的阻塞操作可能会被打断
            } catch (InterruptedException e) {
                log.warn("WorkflowEventLooper thread is interrupted, will close this loop");
                Thread.currentThread().interrupt();
                break;
            }
            try {
                // q：MDC是一个记录日志信息的上下文，本质是一个ThreadLocalContext
                LogUtils.setWorkflowInstanceIdMDC(workflowEvent.getWorkflowInstanceId());
                log.info("Begin to handle WorkflowEvent: {}", workflowEvent);
                // 从map中获取对应的handler， 根据事件来获取handler 类型
                WorkflowEventHandler workflowEventHandler =
                        workflowEventHandlerMap.get(workflowEvent.getWorkflowEventType());
                // 处理事件的handler
                workflowEventHandler.handleWorkflowEvent(workflowEvent);
                log.info("Success handle WorkflowEvent: {}", workflowEvent);
            } catch (WorkflowEventHandleException workflowEventHandleException) {
                log.error("Handle workflow event failed, will retry again: {}", workflowEvent,
                        workflowEventHandleException);
                // 消费失败后会重新添加到队列的尾部
                workflowEventQueue.addEvent(workflowEvent);
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } catch (WorkflowEventHandleError workflowEventHandleError) {
                log.error("Handle workflow event error, will drop this event: {}",
                        workflowEvent,
                        workflowEventHandleError);
            } catch (Exception unknownException) {
                log.error("Handle workflow event failed, get a unknown exception, will retry again: {}", workflowEvent,
                        unknownException);
                workflowEventQueue.addEvent(workflowEvent);
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            } finally {
                LogUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (!RUNNING_FLAG.compareAndSet(true, false)) {
            log.info("WorkflowEventLooper thread is not start, no need to close");
            return;
        }
        log.info("WorkflowEventLooper is closing...");
        this.interrupt();
        log.info("WorkflowEventLooper closed...");
    }
}

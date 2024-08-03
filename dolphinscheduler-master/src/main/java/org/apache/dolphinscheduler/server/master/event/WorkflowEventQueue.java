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

import java.util.concurrent.LinkedBlockingQueue;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 消费command 生成的事件会放在这个队列里面
 */
@Component
@Slf4j
public class WorkflowEventQueue {

    // 事件队列 ，是静态的是类共享的，所以其他bean注入的WorkflowEventQueue bean 虽然是队列，但是是同一个队列
    private static final LinkedBlockingQueue<WorkflowEvent> workflowEventQueue = new LinkedBlockingQueue<>();

    /**
     * Add a workflow event.
     */
    public void addEvent(WorkflowEvent workflowEvent) {
        workflowEventQueue.add(workflowEvent);
        log.info("Added workflow event to workflowEvent queue, event: {}", workflowEvent);
    }

    /**
     * Pool the head of the workflow event queue and wait an workflow event.
     */
    public WorkflowEvent poolEvent() throws InterruptedException {
        // LinkedBlockingQueue 是无锁机制cas实现的链式队列，take 方法会获取队列头部的元素，并出队，如果队列元素为空会阻塞直到队列中有元素
        return workflowEventQueue.take();
    }

    public void clearWorkflowEventQueue() {
        workflowEventQueue.clear();
    }
}

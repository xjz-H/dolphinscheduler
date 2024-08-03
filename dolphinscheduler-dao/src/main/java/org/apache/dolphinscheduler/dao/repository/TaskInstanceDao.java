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

package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.model.DateInterval;

import java.util.List;
import java.util.Set;

/**
 * Task Instance DAO
 *
 * 为什么这么设计？
 * 接口隔离原则（Interface Segregation Principle，ISP）是面向对象设计原则中的一条重要原则，它要求接口应该尽量小而专门，避免定义臃肿庞大的接口。接口隔离原则的核心思想是：使用多个专门的接口比使用一个总接口要好。
 *
 * 具体来说，接口隔离原则可以通过以下几个方面来详细说明：
 *
 * 小接口：接口应该尽量小，只包含客户端需要的方法，避免定义过多的方法，减少实现类需要实现的方法数量。这样可以降低耦合度，提高接口的内聚性。
 *
 * 高内聚性：一个接口应该只服务于一个子模块或业务逻辑，不要将多个不同的功能混合在同一个接口中。不同的功能应该分离开来，降低接口的复杂性。
 *
 * 专门化：接口应该专门为某一类客户端定制，不应该强迫客户端去实现那些它们不需要的方法。这样可以使接口的设计更加精准和高效。
 *
 * 接口细化：根据实际需求将大接口细化为多个小接口，每个小接口服务于特定的客户端或功能模块。这样可以使接口的设计更加灵活和可扩展。
 *
 * 通过遵循接口隔离原则，可以提高系统的灵活性、可维护性和扩展性，降低代码的耦合度，减少对不必要接口的依赖，使系统更加稳定和易于维护。在设计接口时，应该根据实际业务需求和客户端的使用情况，合理划分和设计接口，避免定义冗余和臃肿的接口，从而提高代码的质量和可维护性。
 *IDao 是一个通用的接口
 *
 */
public interface TaskInstanceDao extends IDao<TaskInstance> {

    /**
     * Update or Insert task instance to DB.
     * ID is null -> Insert
     * ID is not null -> Update
     *
     * @param taskInstance task instance
     * @return result
     */
    boolean upsertTaskInstance(TaskInstance taskInstance);

    /**
     * Submit a task instance to DB.
     * @param taskInstance task instance
     * @param processInstance process instance
     * @return task instance
     */
    boolean submitTaskInstanceToDB(TaskInstance taskInstance, ProcessInstance processInstance);

    /**
     * Query list of valid task instance by process instance id
     * @param processInstanceId processInstanceId
     * @param testFlag test flag
     * @return list of valid task instance
     */
    List<TaskInstance> queryValidTaskListByWorkflowInstanceId(Integer processInstanceId, int testFlag);

    /**
     * Query list of task instance by process instance id and task code
     * @param processInstanceId processInstanceId
     * @param taskCode task code
     * @return list of valid task instance
     */
    TaskInstance queryByWorkflowInstanceIdAndTaskCode(Integer processInstanceId, Long taskCode);

    /**
     * find previous task list by work process id
     * @param processInstanceId processInstanceId
     * @return task instance list
     */
    List<TaskInstance> queryPreviousTaskListByWorkflowInstanceId(Integer processInstanceId);

    /**
     * find task instance by cache_key
     * @param cacheKey cache key
     * @return task instance
     */
    TaskInstance queryByCacheKey(String cacheKey);

    /**
     * clear task instance cache by cache_key
     * @param cacheKey cache key
     * @return task instance
     */
    Boolean clearCacheByCacheKey(String cacheKey);

    void deleteByWorkflowInstanceId(int workflowInstanceId);

    List<TaskInstance> queryByWorkflowInstanceId(Integer processInstanceId);

    /**
     * find last task instance list corresponding to taskCodes in the date interval
     *
     * @param taskCodes taskCodes
     * @param dateInterval dateInterval
     * @param testFlag test flag
     * @return task instance list
     */
    List<TaskInstance> queryLastTaskInstanceListIntervalByTaskCodes(Set<Long> taskCodes, DateInterval dateInterval,
                                                                    int testFlag);

    /**
     * find last task instance corresponding to taskCode in the date interval
     * @param depTaskCode taskCode
     * @param dateInterval dateInterval
     * @param testFlag test flag
     * @return task instance
     */
    TaskInstance queryLastTaskInstanceIntervalByTaskCode(long depTaskCode, DateInterval dateInterval, int testFlag);
}

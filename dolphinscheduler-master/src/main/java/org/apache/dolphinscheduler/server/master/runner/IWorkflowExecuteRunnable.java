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

import java.util.concurrent.Callable;

public interface IWorkflowExecuteRunnable extends Callable<WorkflowStartStatus> {
    // todo: add control method to manage the workflow runnable e.g. pause/stop ....

    /***
     * 通过默认方法的方式重写了 Callable 接口中的 call 方法
     * 将 call 方法的实现委托给了 startWorkflow 方法，这样在实现 IWorkflowExecuteRunnable 接口时，只需要实现 startWorkflow 方法即可。
     */
    //Callable的call 方法可能会抛出异常，在使用future 对象的get 的方法时可能会抛出异常
   //这里有一个知识点是值得学习的，我们实现接口的方法时，声明的异常，可以在实现的方法中继续声明，或者声明子异常，也可以不声明，直接吞掉
    @Override
    default WorkflowStartStatus call(){
        return startWorkflow();
    }

    WorkflowStartStatus startWorkflow();

}

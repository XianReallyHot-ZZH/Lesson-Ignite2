/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/lang/IgnitePredicate.java
// Lesson 0.3 子集：完整接口（定义小，直接全量复制）。
// 消费者：CommonUtils.forName 的类名过滤器（marshaller 反序列化防护）。

package org.apache.ignite.lang;

import java.io.Serializable;

/**
 * 定义一个谓词（可计算的布尔函数），带一个自由变量。
 * <p>
 * 谓词是可序列化的，可以在网格内任意传递、远程执行。
 *
 * @param <E> 谓词的自由变量类型。
 */
public interface IgnitePredicate<E> extends Serializable {
    /**
     * 对给定入参求谓词值。
     *
     * @param e 谓词入参。
     * @return 谓词结果。
     */
    boolean apply(E e);
}

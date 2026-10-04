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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/util/typedef/G.java
// Lesson 0.2 子集：完整复刻（历史别名：GridGain 时代的 G 门面，现仅作 Ignition 别名）。
// IgnitionEx 用 G.class 作工厂日志类别——本类存在即为此。

package org.apache.ignite.internal.util.typedef;

import org.apache.ignite.Ignition;

/**
 * {@code typedef}：{@link Ignition} 的历史别名（旧 API 以 {@code G.start(...)} 著称）。
 * 内核源码以其类名充当日志类别标记。
 */
@SuppressWarnings({"ExtendsUtilityClass"})
public class G extends Ignition { /* 空实现。 */ }

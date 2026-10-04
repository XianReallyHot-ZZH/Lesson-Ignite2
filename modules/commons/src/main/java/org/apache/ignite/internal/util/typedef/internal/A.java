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

// 对应 vendor: vendors/ignite/modules/commons/src/main/java/org/apache/ignite/internal/util/typedef/internal/A.java
// Lesson 0.2 子集：完整复刻（纯别名类，vendor 即一行 extends）。

package org.apache.ignite.internal.util.typedef.internal;

import org.apache.ignite.internal.util.GridArgumentCheck;

/**
 * {@code typedef}：内部使用的 {@link GridArgumentCheck} 简称，
 * 让参数校验代码更简洁。
 */
@SuppressWarnings({"ExtendsUtilityClass"})
public class A extends GridArgumentCheck { /* 空实现。 */ }

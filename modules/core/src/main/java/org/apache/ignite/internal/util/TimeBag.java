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

// 对应 vendor: vendors/ignite/modules/core/src/main/java/org/apache/ignite/internal/util/TimeBag.java
// Lesson 0.3 子集：全局阶段计时（record 开关、finishGlobalStage、阶段清单）。
// vendor 另有本地阶段（finishLocalStage/CompositeStage 排序聚合）——启动路径
// 目前只用全局阶段，本地阶段随细粒度计时需求补。

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * 启动分段计时袋：按阶段记录耗时，供"Node started"日志输出。
 */
public class TimeBag {
    /** 全部已完成阶段（描述 + 耗时）。 */
    private final List<Stage> finishedStages;

    /** 是否记录。 */
    private final boolean record;

    /** 当前阶段起点纳秒。 */
    private long curStageStart;

    /**
     * @param record 是否记录（{@code false} 时所有方法近似 no-op）。
     */
    public TimeBag(boolean record) {
        this.record = record;

        finishedStages = new ArrayList<>();

        curStageStart = System.nanoTime();
    }

    /**
     * 结束当前全局阶段并开启下一阶段计时。
     *
     * @param description 刚结束阶段的描述。
     */
    public void finishGlobalStage(@NotNull String description) {
        if (!record)
            return;

        long finishTime = System.nanoTime();

        long dur = (finishTime - curStageStart) / 1_000_000;

        finishedStages.add(new Stage(description, dur));

        // 下一阶段从当前结束点起算。
        curStageStart = finishTime;
    }

    /**
     * @return 已完成阶段的可读计时清单（"desc [123ms]" 每行一条）。
     */
    public List<String> stagesTimings() {
        if (!record)
            return Collections.emptyList();

        List<String> timings = new ArrayList<>(finishedStages.size());

        for (Stage stage : finishedStages)
            timings.add(stage.toString());

        return timings;
    }

    /**
     * 单个阶段记录。
     */
    private static final class Stage {
        /** 描述。 */
        private final String description;

        /** 耗时（毫秒）。 */
        private final long time;

        /**
         * @param description 描述。
         * @param time 耗时毫秒。
         */
        private Stage(String description, long time) {
            this.description = description;
            this.time = time;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return description + " [" + time + "ms]";
        }
    }
}

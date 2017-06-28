/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.AggregateOperations;

import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class PipelineWordCount {

    public static void main(String[] args) {

        final Pattern delimiter = Pattern.compile("\\W+");

        Pipeline p = Pipeline.create();

        PStream<String> books = p.drawFrom(Sources.readFiles("books"));

        PStream<Entry<String, Long>> wordCounts = books
                .flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase())).filter(word -> !word.isEmpty()))
                .groupBy(wholeItem(), AggregateOperations.counting());

        wordCounts.drainTo(Sinks.writeMap("counts"));

        wordCounts
                .groupBy(e -> true, AggregateOperations.summingLong(Entry::getValue))
                .drainTo(Sinks.writeMap("totals"));

        p.execute(null);
    }
}
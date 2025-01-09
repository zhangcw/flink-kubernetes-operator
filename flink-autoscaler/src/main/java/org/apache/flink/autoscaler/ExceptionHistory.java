/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.TreeMap;

@Data
@NoArgsConstructor
public class ExceptionHistory {

    private static final int DEFAULT_HISTORY_SIZE = 10;

    @Getter @Setter private ExceptionRecord current;
    private TreeMap<Instant, ExceptionRecord> history = new TreeMap<>();

    public enum Type {
        Warning,
        Error
    }

    public enum Reason {
        OutOfMemory,
        Exception
    }

    @Data
    @NoArgsConstructor
    public static class ExceptionRecord {
        private long timestamp;
        private Type type;
        private Reason reason;
        private String message;
        private String stacktrace;

        public ExceptionRecord(
                Type type, Reason reason, String message, String stacktrace, long timestamp) {
            this.type = type;
            this.reason = reason;
            this.message = message;
            this.stacktrace = stacktrace;
            this.timestamp = timestamp;
        }
    }

    public void addExceptionRecord(
            Type type, Reason reason, String message, String stacktrace, long timestamp) {
        moveCurrentIntoHistory();
        current = new ExceptionRecord(type, reason, message, stacktrace, timestamp);
    }

    public ExceptionHistory clearCurrentException() {
        return moveCurrentIntoHistory();
    }

    public ExceptionHistory moveCurrentIntoHistory() {
        if (current != null) {
            history.put(Instant.ofEpochMilli(current.timestamp), current);
            while (history.size() > DEFAULT_HISTORY_SIZE) {
                history.remove(history.firstKey());
            }
            current = null;
        }
        return this;
    }
}

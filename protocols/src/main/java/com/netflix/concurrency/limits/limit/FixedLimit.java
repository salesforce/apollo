/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.limit;

/**
 * Non dynamic limit with fixed value
 */
public final class FixedLimit extends AbstractLimit {

    public static FixedLimit of(int limit) {
        return new FixedLimit(limit);
    }
    
    private FixedLimit(int limit) {
        super(limit);
    }

    @Override
    public int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        return getLimit();
    }
    
    @Override
    public String toString() {
        return "FixedLimit [limit=" + getLimit() + "]";
    }
}

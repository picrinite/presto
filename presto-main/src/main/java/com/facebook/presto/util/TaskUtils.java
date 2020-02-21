/*
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
package com.facebook.presto.util;

import com.facebook.airlift.http.client.HttpUriBuilder;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TaskUtils
{
    public static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

    private TaskUtils() {}

    public static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }

    public static Optional<URI> getAsyncPageTransportLocaton(URI location, boolean asyncPageTransportEnabled)
    {
        if (asyncPageTransportEnabled) {
            String path = location.getPath().replace("v1/task", "v1/task/async");
            return Optional.of(HttpUriBuilder.uriBuilderFrom(location).replacePath(path).build());
        }
        else {
            return Optional.empty();
        }
    }
}

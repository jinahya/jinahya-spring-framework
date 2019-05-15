package com.github.jinahya.springframework.data.domain;

/*-
 * #%L
 * jinahya-springframework
 * %%
 * Copyright (C) 2019 Jinahya, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.springframework.data.domain.Page;
import org.springframework.lang.NonNull;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utilities and constants for {@link org.springframework.data.domain.Page}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
public final class JinahyaPages {

    // -----------------------------------------------------------------------------------------------------------------
    @Deprecated
    public static <T> Stream<? extends T> stream(@NonNull final Page<? extends T> page, final boolean parallel) {
        return StreamSupport.stream(page.spliterator(), parallel);
    }

    @Deprecated
    public static <T, R> R collect(@NonNull final Page<? extends T> page, final boolean parallel,
                                   @NonNull final Supplier<R> supplier,
                                   @NonNull final BiConsumer<R, ? super T> accumulator,
                                   @NonNull final BiConsumer<R, R> combiner) {
        return stream(page, parallel).collect(supplier, accumulator, combiner);
    }

    @Deprecated
    public static <T, R, A> R collect(@NonNull final Page<? extends T> page, final boolean parallel,
                                      @NonNull final Collector<? super T, A, R> collector) {
        return stream(page, parallel).collect(collector);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates collect new instance.
     */
    private JinahyaPages() {
        super();
    }
}

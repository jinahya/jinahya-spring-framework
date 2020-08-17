package com.github.jinahya.springframework.web.reactive.function.client.webclient;

/*-
 * #%L
 * jinahya-spring-framework
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

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Function;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.reduceAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndApply;
import static java.util.Objects.requireNonNull;

/**
 * Utilities for {@link WebClient.ResponseSpec}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaResponseSpecUtils {

    public static <R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec response, final Function<? super ReadableByteChannel, ? extends R> function) {
        requireNonNull(response, "response is null");
        return writeToTempFileAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param function the function to be applied with the {@link Pipe#source() source} of the pipe.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     */
    public static <R> Mono<R> pipeBodyAndApply(final WebClient.ResponseSpec response,
                                               final Function<? super ReadableByteChannel, ? extends R> function) {
        requireNonNull(response, "response is null");
        return pipeAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    /**
     * Reduces given response spec's body into a single input stream and returns the result of specified function
     * applied with it.
     *
     * @param response the response spec whose body is reduced.
     * @param function the function to be applied with the reduced body.
     * @param <R>      result type parameter
     * @return a mono of the result of the {@code function}.
     * @implNote This method aggregates all bytes into a single stream in a non-memory-efficient manner.
     */
    public static <R> Mono<R> reduceBodyAndApply(final WebClient.ResponseSpec response,
                                                 final Function<? super InputStream, ? extends R> function) {
        requireNonNull(response, "response is null");
        return reduceAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

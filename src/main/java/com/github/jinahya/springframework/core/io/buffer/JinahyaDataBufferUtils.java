package com.github.jinahya.springframework.core.io.buffer;

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
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

/**
 * A utility class for {@link DataBuffer} class and {@link DataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaDataBufferUtils {

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given stream of data buffers to a temporary file and returns the result of specified function applied with
     * a readable byte channel of the file.
     *
     * @param source   the stream of data buffers to be written to the file.
     * @param function the function to be applied with the channel.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeAndApply(final Publisher<DataBuffer> source,
                                            final Function<? super ReadableByteChannel, ? extends R> function) {
        Objects.requireNonNull(function, "function is null");
        return Mono.using(
                () -> Files.createTempFile(null, null),
                t -> DataBufferUtils
                        .write(source, t)
                        .thenReturn(t)
                        .handle((v, sink) -> {
                            try (ReadableByteChannel channel = FileChannel.open(v, StandardOpenOption.READ)) {
                                sink.next(function.apply(channel));
                            } catch (final IOException ioe) {
                                log.error("failed to apply channel", ioe);
                                sink.error(ioe);
                            }
                        }),
                t -> {
                    try {
                        final boolean deleted = Files.deleteIfExists(t);
                    } catch (final IOException ioe) {
                        log.error("failed to delete temp file: {}", t, ioe);
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    public static <R> Mono<R> pipeAndApply(final Publisher<DataBuffer> source,
                                           final Function<? super ReadableByteChannel, ? extends R> function,
                                           final Executor executor) {
        Objects.requireNonNull(function, "function is null");
        return Mono.using(
                Pipe::open,
                p -> {
                    final CompletableFuture<R> future
                            = CompletableFuture.supplyAsync(() -> function.apply(p.source()), executor);
                    return Mono
                            .fromFuture(future)
                            .doFirst(() -> DataBufferUtils
                                    .write(source, p.sink())
                                    .doOnError(t -> {
                                        log.error("failed to write body to pipe.sink", t);
                                    })
                                    .doFinally(s -> {
                                        try {
                                            p.sink().close();
                                        } catch (final IOException ioe) {
                                            log.error("failed to close pipe.sink", ioe);
                                        }
                                    })
                                    .subscribe(DataBufferUtils.releaseConsumer())
                            )
                            .doFinally(s -> {
                                if (s == SignalType.CANCEL) {
                                    future.cancel(true);
                                }
                            });
                },
                p -> {
                    try {
                        p.source().close();
                    } catch (final IOException ioe) {
                        throw new RuntimeException("failed to close the pipe.source", ioe);
                    }
                }
        );
    }

    public static <R> Mono<R> pipeAndApply(final Publisher<DataBuffer> source,
                                           final Function<? super ReadableByteChannel, ? extends R> function) {
        return pipeAndApply(source, function, ForkJoinPool.commonPool());
    }

    /**
     * Reduces specified stream of data buffers into an input stream and returns specified function's result applies
     * with it.
     *
     * @param source   the stream of data buffers to reduce.
     * @param function the function to apply with the reduced stream.
     * @param <R>      result type parameter
     * @return the result of the function.
     */
    public static <R> Mono<R> reduceAndApply(final Publisher<? extends DataBuffer> source,
                                             final Function<? super InputStream, ? extends R> function) {
        Objects.requireNonNull(function, "function is null");
        return Flux
                .from(source)
                .map(b -> b.asInputStream(true))
                .reduce(SequenceInputStream::new)
                .handle((stream, sink) -> {
                    try (InputStream s = stream) {
                        sink.next(function.apply(s));
                    } catch (final IOException ioe) {
                        sink.error(ioe);
                    }
                })
                ;
    }

    // -----------------------------------------------------------------------------------------------------------------
    private JinahyaDataBufferUtils() {
        throw new AssertionError("instantiation is not allowed");
    }
}

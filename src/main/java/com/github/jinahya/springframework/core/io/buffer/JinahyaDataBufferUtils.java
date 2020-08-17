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
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.springframework.core.io.buffer.DataBufferUtils.releaseConsumer;
import static org.springframework.core.io.buffer.DataBufferUtils.write;

/**
 * A utility class for {@link DataBuffer} class and {@link DataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaDataBufferUtils {

    // -----------------------------------------------------------------------------------------------------------------
    public static Runnable writer(final Publisher<DataBuffer> source, final Pipe.SinkChannel destination) {
        return () -> write(source, destination)
                .doOnError(t -> log.error("failed to write {} to {}", source, destination, t))
                .doFinally(st -> {
                    try {
                        destination.close();
                    } catch (final IOException ioe) {
                        log.error("failed to close {}", destination, ioe);
                    }
                })
                .subscribe(releaseConsumer());
    }

    // -----------------------------------------------------------------------------------------------------------------

    private static final OpenOption[] EMPTY_OPTIONS = new OpenOption[0];

    /**
     * Writes given stream of data buffers to specified file and returns the result of specified function applied with
     * the file.
     *
     * @param <R>         result type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which given stream is written.
     * @param options     an array of open options.
     * @param function    the function to be applied with the file.
     * @return a mono of the result of the function.
     * @see DataBufferUtils#write(Publisher, Path, OpenOption...)
     */
    public static <R> Mono<R> writeAndApply(final Publisher<DataBuffer> source, final Path destination,
                                            final OpenOption[] options,
                                            final Function<? super Path, ? extends R> function) {
        requireNonNull(function, "function is null");
        return DataBufferUtils.write(source, destination, options == null ? EMPTY_OPTIONS : options)
                .thenReturn(destination)
                .map(function);
    }

    /**
     * Writes given stream of data buffers to specified file and returns the result of specified function applied with
     * the file along with an argument supplied by specified supplier.
     *
     * @param <U>         second argument type parameter
     * @param <R>         result type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the stream is written.
     * @param options     an array of open options.
     * @param function    the function to be applied with the file and the second argument.
     * @param supplier    the supplier for the second argument of the function.
     * @return the value the function results.
     * @see #writeAndApply(Publisher, Path, OpenOption[], Function)
     */
    public static <U, R> Mono<R> writeAndApply(
            final Publisher<DataBuffer> source, final Path destination,
            final OpenOption[] options,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        requireNonNull(function, "function is null");
        requireNonNull(supplier, "supplier is null");
        return writeAndApply(source, destination, options, f -> function.apply(f, supplier.get()));
    }

    /**
     * Writes given stream of data buffers to specified file and accepts the file to specified consumer.
     *
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the stream is written.
     * @param options     an array of open options.
     * @param consumer    the consumer to be accepted with the file.
     * @return a mono of {@link Void}.
     */
    public static Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                            final OpenOption[] options,
                                            final Consumer<? super Path> consumer) {
        requireNonNull(consumer, "consumer is null");
        return writeAndApply(source, destination, options, identity())
                .map(p -> {
                    consumer.accept(p);
                    return p;
                })
                .then();
    }

    /**
     * Writes given stream of data buffers to specified file and accepts the file, along with an argument supplied by
     * specified supplier, to specified consumer.
     *
     * @param <U>         second argument type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the stream is written.
     * @param options     an array of open options.
     * @param consumer    the consumer to be accepted with the file along with the second argument.
     * @param supplier    the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeAndAccept(Publisher, Path, OpenOption[], Consumer)
     */
    public static <U> Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                                final OpenOption[] options,
                                                final BiConsumer<? super Path, ? super U> consumer,
                                                final Supplier<? extends U> supplier) {
        requireNonNull(consumer, "consumer is null");
        requireNonNull(supplier, "supplier is null");
        return writeAndAccept(source, destination, options, f -> consumer.accept(f, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given stream of data buffers to a temporary file and returns the result of specified function applied with
     * a readable byte channel for the file.
     *
     * @param source   the stream of data buffers to be written to the file.
     * @param function the function to be applied with the channel.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeToTempFileAndApply(
            final Publisher<DataBuffer> source,
            final Function<? super ReadableByteChannel, ? extends R> function) {
        requireNonNull(source, "source is null");
        requireNonNull(function, "function is null");
        return Mono.using(
                () -> Files.createTempFile(null, null),
                t -> writeAndApply(
                        source,
                        t,
                        null,
                        f -> {
                            try (ReadableByteChannel channel = FileChannel.open(f, READ)) {
                                return function.apply(channel);
                            } catch (final IOException ioe) {
                                log.error("failed to apply channel", ioe);
                                throw new RuntimeException(ioe);
                            }
                        }),
                t -> {
                    try {
                        deleteIfExists(t);
                    } catch (final IOException ioe) {
                        log.error("failed to delete temp file: {}", t, ioe);
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Writes given stream of data buffers to a temporary file and returns the result of specified function applied with
     * a readable byte channel for the file and an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param source   the stream of data buffers to be written to the file.
     * @param function the function to be applied with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of the result of the function.
     * @see #writeToTempFileAndApply(Publisher, Function)
     */
    public static <U, R> Mono<R> writeToTempFileAndApply(
            final Publisher<DataBuffer> source,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        requireNonNull(function, "function is null");
        requireNonNull(supplier, "supplier is null");
        return writeToTempFileAndApply(source, c -> function.apply(c, supplier.get()));
    }

    /**
     * Writes given stream of data buffers to a temporary file and accepts a readable byte channel to specified
     * consumer.
     *
     * @param source   the stream of data buffers to be written to the file.
     * @param consumer the consumer to be accepted with the channel.
     * @return a mono of {@link Void}.
     * @see #writeToTempFileAndApply(Publisher, Function)
     * @see #writeToTempFileAndAccept(Publisher, BiConsumer, Supplier)
     */
    public static Mono<Void> writeToTempFileAndAccept(final Publisher<DataBuffer> source,
                                                      final Consumer<? super ReadableByteChannel> consumer) {
        requireNonNull(consumer, "consumer is null");
        return writeToTempFileAndApply(source,
                                       c -> {
                                           consumer.accept(c);
                                           return c;
                                       })
                .then();
    }

    /**
     * Writes given stream of data buffers to a temporary file and accepts a readable bytes channel from the file, along
     * with an argument supplied by specified supplier, to specified consumer.
     *
     * @param <U>      second argument type parameter
     * @param source   the stream of data buffers to be written to the file.
     * @param consumer the consumer to be accepted with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeToTempFileAndAccept(Publisher, Consumer)
     */
    public static <U> Mono<Void> writeToTempFileAndAccept(
            final Publisher<DataBuffer> source,
            final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        requireNonNull(consumer, "consumer is null");
        requireNonNull(supplier, "supplier is null");
        return writeToTempFileAndAccept(source, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaDataBufferUtils() {
        super();
    }
}

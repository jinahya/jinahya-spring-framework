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
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.channels.FileChannel.open;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.function.Function.identity;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Mono.using;

/**
 * A utility class for {@link DataBuffer} class and {@link DataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaDataBufferUtils {

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Invokes {@link DataBufferUtils#write(Publisher, Path, OpenOption...)} method with given arguments and returns
     * specified file.
     *
     * @param source      the stream of data buffers to be written.
     * @param destination the path to the file.
     * @param options     options specifying how the file is opened.
     * @return a {@link Mono} of specified file.
     */
    public static Mono<Path> write(final Publisher<DataBuffer> source, final Path destination,
                                   final OpenOption... options) {
        return DataBufferUtils.write(source, destination, options).thenReturn(destination);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Write the given stream of data buffers to the specified file and returns the result of specified function applied
     * with the file.
     *
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which body of the spec is written.
     * @param function    the function to be applied with the file.
     * @param <R>         result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeAndApply(final Publisher<DataBuffer> source, final Path destination,
                                            final Function<? super Path, ? extends R> function) {
        return write(source, destination).map(function);
    }

    /**
     * Writes given stream of data buffers to specified file and returns the result of specified function applied with
     * the file along with an argument supplied by specified supplier.
     *
     * @param <U>         second argument type parameter
     * @param <R>         result type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the body of the spec is written.
     * @param function    the function to be applied with the file and the second argument.
     * @param supplier    the supplier for the second argument of the function.
     * @return the value the function results.
     * @see #writeAndApply(Publisher, Path, Function)
     */
    public static <U, R> Mono<R> writeAndApply(
            final Publisher<DataBuffer> source, final Path destination,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        return writeAndApply(
                source,
                destination,
                f -> function.apply(f, supplier.get())
        );
    }

    /**
     * Writes given stream of data buffers to specified file and accepts the file to specified consumer.
     *
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the body is written.
     * @param consumer    the consumer to be accepted with the path.
     * @return a mono of {@link Void}.
     */
    public static Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                            final Consumer<? super Path> consumer) {
        return writeAndApply(source, destination, identity())
                .map(p -> {
                    consumer.accept(p);
                    return p;
                })
                .then();
    }

    /**
     * Writes given response spec's body to specified file and accepts the file, along with an argument supplied by
     * specified supplier, to specified consumer.
     *
     * @param <U>         second argument type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the body is written.
     * @param consumer    the consumer to be accepted with the file along with the second argument.
     * @param supplier    the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeAndAccept(Publisher, Path, Consumer)
     */
    public static <U> Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                                final BiConsumer<? super Path, ? super U> consumer,
                                                final Supplier<? extends U> supplier) {
        return writeAndAccept(source, destination, f -> consumer.accept(f, supplier.get()));
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
    public static <R> Mono<R> writeToTempFileAndApply(final Publisher<DataBuffer> source,
                                                      final Function<? super ReadableByteChannel, ? extends R> function) {
        return using(
                () -> createTempFile(null, null),
                t -> writeAndApply(source, t, f -> {
                    try {
                        try (ReadableByteChannel channel = open(f, READ)) {
                            return function.apply(channel);
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }),
                t -> {
                    try {
                        final boolean deleted = deleteIfExists(t);
                        //assert deleted;
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Writes given stream of data buffers to a temporary path and returns the result of specified path function applied
     * with a readable byte channel for the file and an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param source   the stream of data buffers to be written to the file.
     * @param function the function to be applied with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of the result of the function.
     */
    public static <U, R> Mono<R> writeToTempFileAndApply(
            final Publisher<DataBuffer> source,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return writeToTempFileAndApply(source, c -> function.apply(c, supplier.get()));
    }

    /**
     * Writes given stream of data buffers to a temporary file and accepts a readable byte channel to specified
     * consumer.
     *
     * @param source   the stream of data buffers to be written to the file.
     * @param consumer the consumer to be accepted with the channel.
     * @return a mono of {@link Void}.
     */
    public static Mono<Void> writeToTempFileAndAccept(final Publisher<DataBuffer> source,
                                                      final Consumer<? super ReadableByteChannel> consumer) {
        return writeToTempFileAndApply(
                source,
                c -> {
                    consumer.accept(c);
                    return c; // returning null is not welcome
                }
        )
                .then();
    }

    /**
     * Writes given response spec's body to a temporary path and accept a readable bytes channel from the file, along
     * with an argument supplied by specified supplier, to specified consumer.
     *
     * @param <U>      second argument type parameter
     * @param source   the stream of data buffers to be written to the file.
     * @param consumer the consumer to be accepted with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of {@link Void}.
     */
    public static <U> Mono<Void> writeToTempFileAndAccept(
            final Publisher<DataBuffer> source, final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        return writeToTempFileAndAccept(source, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Pipes given stream of data buffers and returns the result of specified function applied with the {@link
     * Pipe#sink()}.
     *
     * @param source   the stream of data buffers to be piped.
     * @param executor an executor service for writing the body to {@link Pipe#sink()}.
     * @param function the function to be applied with the body channel.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see org.springframework.core.task.support.ExecutorServiceAdapter
     * @see #pipeAndApply(Publisher, ExecutorService, BiFunction, Supplier)
     */
    public static <R> Mono<R> pipeAndApply(final Publisher<DataBuffer> source, final ExecutorService executor,
                                           final Function<? super ReadableByteChannel, ? extends R> function) {
        return using(
                Pipe::open,
                p -> {
                    final Future<Disposable> future = executor.submit(
                            () -> DataBufferUtils.write(source, p.sink())
//                                    .log()
                                    .doFinally(s -> {
                                        try {
                                            p.sink().close();
                                            log.trace("p.sink closed");
                                        } catch (final IOException ioe) {
                                            throw new RuntimeException(ioe);
                                        }
                                    })
                                    .subscribe(DataBufferUtils.releaseConsumer())
                    );
                    return just(function.apply(p.source()))
//                            .log()
                            .doFinally(s -> {
                                try {
                                    final Disposable disposable = future.get();
                                    log.trace("disposable.disposed: {}", disposable.isDisposed());
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                }
                            });
                },
                p -> {
                    try {
                        p.source().close();
                        log.trace("p.source closed");
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Pipes given stream of data buffers and returns the result of specified function applied with the {@link
     * Pipe#sink()} and a second argument from specified supplier.
     *
     * @param source   the stream of data buffers to be piped.
     * @param executor an executor service for writing the body to {@link Pipe#sink()}.
     * @param function the function to be applied with the body channel.
     * @param supplier the supplier for the second argument of the function.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeAndApply(Publisher, ExecutorService, Function)
     */
    public static <U, R> Mono<R> pipeAndApply(
            final Publisher<DataBuffer> source, final ExecutorService executor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return pipeAndApply(source, executor, c -> function.apply(c, supplier.get()));
    }

    /**
     * Pipes given stream of data buffers and accepts the {@link Pipe#sink()} to specified consumer.
     *
     * @param source   the stream of data buffers to be piped.
     * @param executor an executor service for writing the body to {@link Pipe#sink()}.
     * @param consumer the consumer to be accepted with the body channel.
     * @return a mono of {@link Void}.
     * @see #pipeAndApply(Publisher, ExecutorService, Function)
     * @see #pipeAndAccept(Publisher, ExecutorService, BiConsumer, Supplier)
     */
    public static Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source, final ExecutorService executor,
                                           final Consumer<? super ReadableByteChannel> consumer) {
        return pipeAndApply(
                source,
                executor,
                c -> {
                    consumer.accept(c);
                    return c;
                }
        )
                .then();
    }

    /**
     * Pipes given stream of data buffers and accepts the {@link Pipe#source()}, along with an argument from specified
     * supplier, to specified consumer.
     *
     * @param source   the stream of data buffers to be pied.
     * @param executor an executor service for piping the body.
     * @param consumer the consumer to be accepted with the body stream.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     */
    public static <U> Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source, final ExecutorService executor,
                                               final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                               final Supplier<? extends U> supplier) {
        return pipeAndAccept(source, executor, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaDataBufferUtils() {
        super();
    }
}

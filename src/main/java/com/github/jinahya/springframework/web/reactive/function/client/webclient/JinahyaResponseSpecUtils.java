package com.github.jinahya.springframework.web.reactive.function.client.webclient;

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

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.Callable;
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
import static org.springframework.core.io.buffer.DataBufferUtils.write;
import static reactor.core.publisher.Mono.just;
import static reactor.core.publisher.Mono.using;

/**
 * Utilities for {@link org.springframework.web.reactive.function.client.WebClient.ResponseSpec}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaResponseSpecUtils {

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes specified flux to specified file.
     *
     * @param flux the flux to be written.
     * @param file the file to which the flux is written.
     * @return a mono of specified file.
     */
    private static Mono<Path> writeToFile(final Flux<DataBuffer> flux, final Path file) {
        return write(flux, file).thenReturn(file);
    }

    /**
     * Writes specified response spec's body to specified file and returns a mono of the file.
     *
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which the response spec't body is written.
     * @return a mono of specified file.
     */
    private static Mono<Path> writeBodyToFile(final WebClient.ResponseSpec response, final Path file) {
        return writeToFile(response.bodyToFlux(DataBuffer.class), file);
    }

    /**
     * Writes given response spec's body to specified file and returns the result of specified function applied with the
     * file.
     *
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which body of the spec is written.
     * @param function the function to be applied with the file.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeBodyToFileAndApply(final WebClient.ResponseSpec response, final Path file,
                                                      final Function<? super Path, ? extends R> function) {
        return writeBodyToFile(response, file).map(function);
    }

    /**
     * Writes given response spec's body to specified file and returns the result of specified function applied with the
     * file along with an argument supplied by specified supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which the body of the spec is written.
     * @param function the function to be applied with the file and the second argument.
     * @param supplier the supplier for the second argument of the function.
     * @return the value the function results.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, Function)
     */
    public static <U, R> Mono<R> writeBodyToFileAndApply(
            final WebClient.ResponseSpec response, final Path file,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        return writeBodyToFileAndApply(
                response,
                file,
                f -> function.apply(f, supplier.get())
        );
    }

    /**
     * Writes given response spec's body to specified file and accepts the file to specified consumer.
     *
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which the body is written.
     * @param consumer the consumer to be accepted with the path.
     * @return a mono of {@link Void}.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, Function)
     */
    public static Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec response, final Path file,
                                                      final Consumer<? super Path> consumer) {
        return writeBodyToFileAndApply(response, file, identity())
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
     * @param <U>      second argument type parameter
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which the body is written.
     * @param consumer the consumer to be accepted with the file along with the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, Consumer)
     */
    public static <U> Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec response, final Path file,
                                                          final BiConsumer<? super Path, ? super U> consumer,
                                                          final Supplier<? extends U> supplier) {
        return writeBodyToFileAndAccept(response, file, f -> consumer.accept(f, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to a temporary file and returns the result of specified function applied with a
     * readable byte channel for the file.
     *
     * @param response the response spec whose body is written to the temporary file
     * @param function the function to be applied with the channel.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec response, final Function<? super ReadableByteChannel, ? extends R> function) {
        return using(
                () -> createTempFile(null, null),
                t -> writeBodyToFileAndApply(response, t, f -> {
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
                        assert deleted;
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Writes given response spec's body to a temporary path and returns the result of specified path function applied
     * with a readable byte channel for the file and an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param response the response spec whose body is written to the path.
     * @param function the function to be applied with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of the result of the function.
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static <U, R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec response,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return writeBodyToTempFileAndApply(response, c -> function.apply(c, supplier.get()));
    }

    /**
     * Writes given response spec's body to a temporary file and accepts a readable byte channel to specified consumer.
     *
     * @param response the response spec whose body is written to the file.
     * @param consumer the consumer to be accepted with the channel.
     * @return a mono of {@link Void}.
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static Mono<Void> writeBodyToTempFileAndAccept(final WebClient.ResponseSpec response,
                                                          final Consumer<? super ReadableByteChannel> consumer) {
        return writeBodyToTempFileAndApply(
                response,
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
     * @param response the response spec whose body is written to the file.
     * @param consumer the consumer to be accepted with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <U> Mono<Void> writeBodyToTempFileAndAccept(
            final WebClient.ResponseSpec response, final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        return writeBodyToTempFileAndAccept(response, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    private static <R> Mono<R> pipeBodyAndApply(
            final WebClient.ResponseSpec response,
            final Function<Callable<Disposable>, ? extends Future<Disposable>> forker,
            final Function<? super ReadableByteChannel, ? extends R> function) {
        return using(
                Pipe::open,
                p -> {
                    final Future<Disposable> future = forker.apply(
                            () -> write(response.bodyToFlux(DataBuffer.class), p.sink())
//                                    .log()
                                    .doFinally(s -> {
                                        try {
                                            p.sink().close();
//                                            log.debug("p.sink closed");
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
                                    assert disposable.isDisposed();
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                }
                            });
                },
                p -> {
                    try {
                        p.source().close();
//                        log.debug("p.source closed");
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#sink()}.
     *
     * @param response the response spec whose body is piped.
     * @param executor an executor service for writing the body to {@link Pipe#sink()}.
     * @param function the function to be applied with the body channel.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     */
    static <R> Mono<R> pipeBodyAndApply(final WebClient.ResponseSpec response, final ExecutorService executor,
                                        final Function<? super ReadableByteChannel, ? extends R> function) {
        if (true) {
            return pipeBodyAndApply(response, executor::submit, function);
        }
        return using(
                Pipe::open,
                p -> {
                    final Future<Disposable> future = executor.submit(
                            () -> write(response.bodyToFlux(DataBuffer.class), p.sink())
//                                    .log()
                                    .doFinally(s -> {
                                        try {
                                            p.sink().close();
//                                            log.debug("p.sink closed");
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
                                    assert disposable.isDisposed();
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                }
                            });
                },
                p -> {
                    try {
                        p.source().close();
//                        log.debug("p.source closed");
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    static <U, R> Mono<R> pipeBodyAndApply(
            final WebClient.ResponseSpec response, final ExecutorService executor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return pipeBodyAndApply(response, executor, c -> function.apply(c, supplier.get()));
    }

    static Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response, final ExecutorService executor,
                                        final Consumer<? super ReadableByteChannel> consumer) {
        return pipeBodyAndApply(
                response,
                executor,
                c -> {
                    consumer.accept(c);
                    return c;
                }
        )
                .then();
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source()}, along with an argument supplied by
     * specified supplier, to specified consumer.
     *
     * @param response the response spec whose body is piped.
     * @param executor an executor service for piping the body.
     * @param consumer the consumer to be accepted with the body stream.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     */
    static <U> Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response, final ExecutorService executor,
                                            final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                            final Supplier<? extends U> supplier) {
        return pipeBodyAndAccept(response, executor, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    static <R> Mono<R> pipeBodyAndApply(final WebClient.ResponseSpec response, final ThreadPoolTaskExecutor executor,
                                        final Function<? super ReadableByteChannel, ? extends R> function) {
        if (true) {
            return pipeBodyAndApply(response, executor::submit, function);
        }
        return using(
                Pipe::open,
                p -> {
                    final Future<Disposable> future = executor.submit(
                            () -> write(response.bodyToFlux(DataBuffer.class), p.sink())
//                                    .log()
                                    .doFinally(s -> {
                                        try {
                                            p.sink().close();
//                                            log.debug("p.sink closed");
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
                                    assert disposable.isDisposed();
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                }
                            });
                },
                p -> {
                    try {
                        p.source().close();
//                        log.debug("p.source closed");
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    static <U, R> Mono<R> pipeBodyAndApply(
            final WebClient.ResponseSpec response, final ThreadPoolTaskExecutor executor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return pipeBodyAndApply(response, executor, c -> function.apply(c, supplier.get()));
    }

    static Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response, final ThreadPoolTaskExecutor executor,
                                        final Consumer<? super ReadableByteChannel> consumer) {
        return pipeBodyAndApply(
                response,
                executor,
                c -> {
                    consumer.accept(c);
                    return c;
                }
        )
                .then();
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source()}, along with an argument supplied by
     * specified supplier, to specified consumer.
     *
     * @param response the response spec whose body is piped.
     * @param executor an executor service for piping the body.
     * @param consumer the consumer to be accepted with the body stream.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     */
    static <U> Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response,
                                            final ThreadPoolTaskExecutor executor,
                                            final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                            final Supplier<? extends U> supplier) {
        return pipeBodyAndAccept(response, executor, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

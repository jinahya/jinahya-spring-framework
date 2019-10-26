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
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.Executor;
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
import static org.springframework.core.io.buffer.DataBufferUtils.releaseConsumer;
import static org.springframework.core.io.buffer.DataBufferUtils.write;
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

    /**
     * Writes given stream of data buffers to specified file and returns the result of specified function applied with
     * the file.
     *
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which given stream is written.
     * @param function    the function to be applied with the file.
     * @param <R>         result type parameter
     * @return a mono of the result of the function.
     * @see DataBufferUtils#write(Publisher, Path, OpenOption...)
     */
    public static <R> Mono<R> writeAndApply(final Publisher<DataBuffer> source, final Path destination,
                                            final Function<? super Path, ? extends R> function) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        return write(source, destination).thenReturn(destination).map(function);
    }

    /**
     * Writes given stream of data buffers to specified file and returns the result of specified function applied with
     * the file along with an argument supplied by specified supplier.
     *
     * @param <U>         second argument type parameter
     * @param <R>         result type parameter
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the stream is written.
     * @param function    the function to be applied with the file and the second argument.
     * @param supplier    the supplier for the second argument of the function.
     * @return the value the function results.
     * @see #writeAndApply(Publisher, Path, Function)
     */
    public static <U, R> Mono<R> writeAndApply(
            final Publisher<DataBuffer> source, final Path destination,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return writeAndApply(source, destination, f -> function.apply(f, supplier.get()));
    }

    /**
     * Writes given stream of data buffers to specified file and accepts the file to specified consumer.
     *
     * @param source      the stream of data buffers to be written to the file.
     * @param destination the file to which the stream is written.
     * @param consumer    the consumer to be accepted with the file.
     * @return a mono of {@link Void}.
     */
    public static Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                            final Consumer<? super Path> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return writeAndApply(source, destination, identity())
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
     * @param consumer    the consumer to be accepted with the file along with the second argument.
     * @param supplier    the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeAndAccept(Publisher, Path, Consumer)
     */
    public static <U> Mono<Void> writeAndAccept(final Publisher<DataBuffer> source, final Path destination,
                                                final BiConsumer<? super Path, ? super U> consumer,
                                                final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
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
    public static <R> Mono<R> writeToTempFileAndApply(
            final Publisher<DataBuffer> source, final Function<? super ReadableByteChannel, ? extends R> function) {
        if (source == null) {
            throw new NullPointerException("source is null");
        }
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        return using(() -> createTempFile(null, null),
                     t -> writeAndApply(source, t, f -> {
                         try (ReadableByteChannel channel = open(f, READ)) {
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
                     });
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
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
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
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
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
            final Publisher<DataBuffer> source, final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return writeToTempFileAndAccept(source, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Pipes given stream of data buffers and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe.
     *
     * @param source   the stream of data buffers to be piped to {@link Pipe#sink() sink}.
     * @param executor an executor for writing the stream to {@link Pipe#sink() sink}.
     * @param function the function to be applied with the {@link Pipe#source() source}.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see org.springframework.core.task.support.ExecutorServiceAdapter
     * @see #pipeAndApply(Publisher, Executor, BiFunction, Supplier)
     */
    public static <R> Mono<R> pipeAndApply(final Publisher<DataBuffer> source, final Executor executor,
                                           final Function<? super ReadableByteChannel, ? extends R> function) {
        if (source == null) {
            throw new NullPointerException("source is null");
        }
        if (executor == null) {
            throw new NullPointerException("executor is null");
        }
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        return using(Pipe::open,
                     p -> {
                         executor.execute(writer(source, p.sink()));
                         return just(function.apply(p.source()));
                     },
                     p -> {
                         try {
                             p.source().close();
                         } catch (final IOException ioe) {
                             log.error("failed to close the pipe.source", ioe);
                             throw new RuntimeException(ioe);
                         }
                     });
    }

    /**
     * Pipes given stream of data buffers and return the result of the function applied with the {@link Pipe#source()
     * source} of the pipe and an argument from specified supplier.
     *
     * @param source   the stream of data buffers to be piped to {@link Pipe#sink() sink}.
     * @param executor an executor for writing the stream to {@link Pipe#sink() sink}.
     * @param function the function to be applied with the {@link Pipe#source() source}.
     * @param supplier the supplier for the second argument of the function.
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeAndApply(Publisher, Executor, Function)
     */
    public static <U, R> Mono<R> pipeAndApply(
            final Publisher<DataBuffer> source, final Executor executor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeAndApply(source, executor, c -> function.apply(c, supplier.get()));
    }

    /**
     * Pipes given stream of data buffers and accepts the {@link Pipe#source() source} of the pipe to specified
     * consumer.
     *
     * @param source   the stream of data buffers to be piped to {@link Pipe#sink()}.
     * @param executor an executor for writing the stream to {@link Pipe#sink()}.
     * @param consumer the consumer to be accepted with the {@link Pipe#source()}.
     * @return a mono of {@link Void}.
     * @see #pipeAndApply(Publisher, Executor, Function)
     * @see #pipeAndAccept(Publisher, Executor, BiConsumer, Supplier)
     */
    public static Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source, final Executor executor,
                                           final Consumer<? super ReadableByteChannel> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return pipeAndApply(source,
                            executor,
                            c -> {
                                consumer.accept(c);
                                return c;
                            })
                .then();
    }

    /**
     * Pipes given stream of data buffers and accepts the {@link Pipe#source() source} of the pipe, along with an
     * argument from specified supplier, to specified consumer.
     *
     * @param source   the stream of data buffers to be pied to {@link Pipe#sink()}.
     * @param executor an executor for piping the stream to {@link Pipe#sink()}.
     * @param consumer the consumer to be accepted with the {@link Pipe#source()}.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     * @see #pipeAndAccept(Publisher, Executor, Consumer)
     */
    public static <U> Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source, final Executor executor,
                                               final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                               final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeAndAccept(source, executor, c -> consumer.accept(c, supplier.get()));
    }

//    // -----------------------------------------------------------------------------------------------------------------
//
//    /**
//     * Pipes given stream of data buffers and returns the result of specified function applied with the {@link
//     * Pipe#source() source} of the pipe.
//     *
//     * @param source   the stream of data buffers to be written to {@link Pipe#sink() sink}.
//     * @param function the function to be applied with the {@link Pipe#source() source}.
//     * @param <R>      result type parameter
//     * @return a mono of result of the function.
//     * @see org.springframework.core.task.support.ExecutorServiceAdapter
//     * @see #pipeAndApply(Publisher, BiFunction, Supplier)
//     */
//    public static <R> Mono<R> pipeAndApply(final Publisher<DataBuffer> source,
//                                           final Function<? super ReadableByteChannel, ? extends R> function) {
//        if (source == null) {
//            throw new NullPointerException("source is null");
//        }
//        if (function == null) {
//            throw new NullPointerException("function is null");
//        }
//        return using(Pipe::open,
//                     p -> fromFuture(supplyAsync(() -> function.apply(p.source())))
//                             .doFirst(writer(source, p.sink())),
//                     p -> {
//                         try {
//                             p.source().close();
//                         } catch (final IOException ioe) {
//                             log.error("failed to close the pipe.source", ioe);
//                             throw new RuntimeException(ioe);
//                         }
//                     });
//    }
//
//    /**
//     * Pipes given stream of data buffers and returns the result of specified function applied with the {@link
//     * Pipe#source() source} of the pipe and a second argument from specified supplier.
//     *
//     * @param source   the stream of data buffers to be piped to {@link Pipe#sink() sink}.
//     * @param function the function to be applied with the {@link Pipe#source() source}.
//     * @param supplier the supplier for the second argument of the function.
//     * @param <U>      second argument type parameter
//     * @param <R>      result type parameter
//     * @return a mono of result of the function.
//     * @see #pipeAndApply(Publisher, Function)
//     */
//    public static <U, R> Mono<R> pipeAndApply(
//            final Publisher<DataBuffer> source,
//            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
//            final Supplier<? extends U> supplier) {
//        if (function == null) {
//            throw new NullPointerException("function is null");
//        }
//        if (supplier == null) {
//            throw new NullPointerException("supplier is null");
//        }
//        return pipeAndApply(source, c -> function.apply(c, supplier.get()));
//    }
//
//    /**
//     * Pipes given stream of data buffers and accepts the {@link Pipe#source() source} of the pipe to specified
//     * consumer.
//     *
//     * @param source   the stream of data buffers to be piped to {@link Pipe#sink() sink}.
//     * @param consumer the consumer to be accepted with the {@link Pipe#source() source}.
//     * @return a mono of {@link Void}.
//     * @see #pipeAndApply(Publisher, Function)
//     * @see #pipeAndAccept(Publisher, BiConsumer, Supplier)
//     */
//    public static Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source,
//                                           final Consumer<? super ReadableByteChannel> consumer) {
//        if (consumer == null) {
//            throw new NullPointerException("consumer is null");
//        }
//        return pipeAndApply(source,
//                            c -> {
//                                consumer.accept(c);
//                                return c;
//                            })
//                .then();
//    }
//
//    /**
//     * Pipes given stream of data buffers and accepts the {@link Pipe#source() source} of the pipe, along with an
//     * argument from specified supplier, to specified consumer.
//     *
//     * @param source   the stream of data buffers to be pied to {@link Pipe#sink() sink}.
//     * @param consumer the consumer to be accepted with the {@link Pipe#source() source}.
//     * @param supplier the supplier for the second argument.
//     * @param <U>      second argument type parameter
//     * @return a mono of {@link Void}.
//     * @see #pipeAndAccept(Publisher, Consumer)
//     */
//    public static <U> Mono<Void> pipeAndAccept(final Publisher<DataBuffer> source,
//                                               final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
//                                               final Supplier<? extends U> supplier) {
//        if (consumer == null) {
//            throw new NullPointerException("consumer is null");
//        }
//        if (supplier == null) {
//            throw new NullPointerException("supplier is null");
//        }
//        return pipeAndAccept(source, c -> consumer.accept(c, supplier.get()));
//    }

    // -----------------------------------------------------------------------------------------------------------------

//    /**
//     * Reduces given stream of data buffers into a single input stream and returns the result of the function applied
//     * with it.
//     *
//     * @param source   the stream of data buffers to be reduced.
//     * @param function the function to be applied with the stream.
//     * @param <R>      result type parameter
//     * @return a mono of the result of the {@code function}.
//     * @see DataBuffer#asInputStream(boolean)
//     * @see SequenceInputStream
//     */
//    @Deprecated
//    public static <R> Mono<R> reduceAsInputStreamAndApply(final Publisher<? extends DataBuffer> source,
//                                                          final Function<? super InputStream, ? extends R> function) {
//        if (source == null) {
//            throw new NullPointerException("source is null");
//        }
//        if (function == null) {
//            throw new NullPointerException("function is null");
//        }
//        return from(source)
//                .map(b -> b.asInputStream(true))
//                .reduce(SequenceInputStream::new)
//                .map(s -> {
//                    try (InputStream c = s) {
//                        return function.apply(c);
//                    } catch (final IOException ioe) {
//                        throw new RuntimeException(ioe);
//                    }
//                });
//    }
//
//    @Deprecated
//    public static <U, R> Mono<R> reduceAsInputStreamAndApply(
//            final Publisher<? extends DataBuffer> source,
//            final BiFunction<? super InputStream, ? super U, ? extends R> function,
//            final Supplier<? extends U> supplier) {
//        if (function == null) {
//            throw new NullPointerException("function is null");
//        }
//        if (supplier == null) {
//            throw new NullPointerException("supplier is null");
//        }
//        return reduceAsInputStreamAndApply(source, s -> function.apply(s, supplier.get()));
//    }
//
//    /**
//     * Reduces given stream of data buffers into a single input stream and accepts it to specified consumer.
//     *
//     * @param source   the stream of data buffers to be reduced.
//     * @param consumer the consumer to be acepted with the input stream.
//     * @return a mono of {@link Void}.
//     */
//    @Deprecated
//    public static Mono<Void> reduceAsInputStreamAndAccept(final Publisher<? extends DataBuffer> source,
//                                                          final Consumer<? super InputStream> consumer) {
//        if (consumer == null) {
//            throw new NullPointerException("consumer is null");
//        }
//        return reduceAsInputStreamAndApply(source,
//                                           s -> {
//                                               consumer.accept(s);
//                                               return s;
//                                           })
//                .then();
//    }
//
//    @Deprecated
//    public static <U> Mono<Void> reduceAsInputStreamAndAccept(final Publisher<? extends DataBuffer> source,
//                                                              final BiConsumer<? super InputStream, ? super U> consumer,
//                                                              final Supplier<? extends U> supplier) {
//        if (consumer == null) {
//            throw new NullPointerException("consumer is null");
//        }
//        if (supplier == null) {
//            throw new NullPointerException("supplier is null");
//        }
//        return reduceAsInputStreamAndAccept(source, s -> consumer.accept(s, supplier.get()));
//    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaDataBufferUtils() {
        super();
    }
}

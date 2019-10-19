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
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.reduceAsInputStreamAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndApply;
import static java.util.function.Function.identity;

/**
 * Utilities for {@link WebClient.ResponseSpec}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaResponseSpecUtils {

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to specified file and returns the result of specified function applied with the
     * file.
     *
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which body of the spec is written.
     * @param function the function to be applied with the file.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, BiFunction, Supplier)
     * @see #writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, Consumer)
     */
    public static <R> Mono<R> writeBodyToFileAndApply(final WebClient.ResponseSpec response, final Path file,
                                                      final Function<? super Path, ? extends R> function) {
        if (response == null) {
            throw new NullPointerException("response is null");
        }
        return writeAndApply(response.bodyToFlux(DataBuffer.class), file, function);
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
     * @see #writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, Consumer)
     */
    public static <U, R> Mono<R> writeBodyToFileAndApply(
            final WebClient.ResponseSpec response, final Path file,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return writeBodyToFileAndApply(response, file, f -> function.apply(f, supplier.get()));
    }

    /**
     * Writes given response spec's body to specified file and accepts the file to specified consumer.
     *
     * @param response the response spec whose body is written to the file.
     * @param file     the file to which the body is written.
     * @param consumer the consumer to be accepted with the file.
     * @return a mono of {@link Void}.
     * @see #writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, BiConsumer, Supplier)
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, Function)
     */
    public static Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec response, final Path file,
                                                      final Consumer<? super Path> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
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
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, BiFunction, Supplier)
     */
    public static <U> Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec response, final Path file,
                                                          final BiConsumer<? super Path, ? super U> consumer,
                                                          final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
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
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, BiFunction, Supplier)
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec response, final Function<? super ReadableByteChannel, ? extends R> function) {
        if (response == null) {
            throw new NullPointerException("response is null");
        }
        return writeToTempFileAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    /**
     * Writes given response spec's body to a temporary file and returns the result of specified function applied with a
     * readable byte channel for the file and an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param response the response spec whose body is written to the file.
     * @param function the function to be applied with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of the result of the function.
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)
     */
    public static <U, R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec response,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return writeBodyToTempFileAndApply(response, c -> function.apply(c, supplier.get()));
    }

    /**
     * Writes given response spec's body to a temporary file and accepts a readable byte channel to specified consumer.
     *
     * @param response the response spec whose body is written to the file.
     * @param consumer the consumer to be accepted with the channel.
     * @return a mono of {@link Void}.
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static Mono<Void> writeBodyToTempFileAndAccept(final WebClient.ResponseSpec response,
                                                          final Consumer<? super ReadableByteChannel> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return writeBodyToTempFileAndApply(response,
                                           c -> {
                                               consumer.accept(c);
                                               return c; // returning null is not welcome
                                           })
                .then();
    }

    /**
     * Writes given response spec's body to a temporary file and accepts a readable bytes channel from the file, along
     * with an argument supplied by specified supplier, to specified consumer.
     *
     * @param <U>      second argument type parameter
     * @param response the response spec whose body is written to the file.
     * @param consumer the consumer to be accepted with the channel and the second argument.
     * @param supplier the supplier for the second argument.
     * @return a mono of {@link Void}.
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Consumer)
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, BiFunction, Supplier)
     */
    public static <U> Mono<Void> writeBodyToTempFileAndAccept(
            final WebClient.ResponseSpec response, final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return writeBodyToTempFileAndAccept(response, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param executor an executor for writing the body to the {@link Pipe#sink() sink} of the pipe.
     * @param function the function to be applied with the {@link Pipe#source() source} of the pipe.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Executor, BiFunction, Supplier)
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Executor, Consumer)
     * @see com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils#pipeAndApply(Publisher, Executor,
     * Function)
     */
    public static <R> Mono<R> pipeBodyAndApply(final WebClient.ResponseSpec response, final Executor executor,
                                               final Function<? super ReadableByteChannel, ? extends R> function) {
        if (response == null) {
            throw new NullPointerException("response is null");
        }
        return pipeAndApply(response.bodyToFlux(DataBuffer.class), executor, function);
    }

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe and a second argument from specified supplier.
     *
     * @param response the response spec whose body is written to {@link Pipe#sink() sink} of the pipe.
     * @param executor an executor for writing the body to {@link Pipe#sink() sink} of the pipe.
     * @param function the function to be applied with the {@link Pipe#source() source} of the pipe.
     * @param supplier the supplier for the second argument of the function.
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Executor, Function)
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Executor, BiConsumer, Supplier)
     */
    public static <U, R> Mono<R> pipeBodyAndApply(
            final WebClient.ResponseSpec response, final Executor executor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeBodyAndApply(response, executor, c -> function.apply(c, supplier.get()));
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source() source} of the pipe to specified consumer.
     *
     * @param response the response spec whose body is written to {@link Pipe#sink() sink} of the pipe.
     * @param executor an executor for writing the body to {@link Pipe#sink() sink} of the pipe.
     * @param consumer the consumer to be accepted with the {@link Pipe#source() source} of the pipe.
     * @return a mono of {@link Void}.
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Executor, BiConsumer, Supplier)
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Executor, Function)
     */
    public static Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response, final Executor executor,
                                               final Consumer<? super ReadableByteChannel> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return pipeBodyAndApply(response,
                                executor,
                                c -> {
                                    consumer.accept(c);
                                    return c;
                                })
                .then();
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source() source} of the pipe, along with an argument
     * from specified supplier, to specified consumer.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param executor an executor for writing the body to {@link Pipe#sink() sink} of the pipe.
     * @param consumer the consumer to be accepted with the {@link Pipe#source() source} of the pipe.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Executor, Consumer)
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Executor, BiFunction, Supplier)
     */
    public static <U> Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response, final Executor executor,
                                                   final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                                   final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeBodyAndAccept(response, executor, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param function the function to be applied with the {@link Pipe#source() source} of the pipe.
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, BiFunction, Supplier)
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <R> Mono<R> pipeBodyAndApply(final WebClient.ResponseSpec response,
                                               final Function<? super ReadableByteChannel, ? extends R> function) {
        if (response == null) {
            throw new NullPointerException("response is null");
        }
        return pipeAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    /**
     * Pipes given response spec's body and returns the result of specified function applied with the {@link
     * Pipe#source() source} of the pipe and a second argument from specified supplier.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param function the function to be applied with the {@link Pipe#source() source} of the pipe.
     * @param supplier the supplier for the second argument of the function.
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @return a mono of result of the function.
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Function)
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)
     */
    public static <U, R> Mono<R> pipeBodyAndApply(
            final WebClient.ResponseSpec response,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeBodyAndApply(response, c -> function.apply(c, supplier.get()));
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source() source} of the pipe to specified consumer.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param consumer the consumer to be accepted with the {@link Pipe#source() source} of the pipe.
     * @return a mono of {@link Void}.
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, Function)
     */
    public static Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response,
                                               final Consumer<? super ReadableByteChannel> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return pipeBodyAndApply(response,
                                c -> {
                                    consumer.accept(c);
                                    return c;
                                })
                .then();
    }

    /**
     * Pipes given response spec's body and accepts the {@link Pipe#source() source} of the pipe, along with an argument
     * from specified supplier, to specified consumer.
     *
     * @param response the response spec whose body is written to the {@link Pipe#sink() sink} of the pipe.
     * @param consumer the consumer to be accepted with the {@link Pipe#source() source} of the pipe.
     * @param supplier the supplier for the second argument.
     * @param <U>      second argument type parameter
     * @return a mono of {@link Void}.
     * @see #pipeBodyAndAccept(WebClient.ResponseSpec, Consumer)
     * @see #pipeBodyAndApply(WebClient.ResponseSpec, BiFunction, Supplier)
     */
    public static <U> Mono<Void> pipeBodyAndAccept(final WebClient.ResponseSpec response,
                                                   final BiConsumer<? super ReadableByteChannel, ? super U> consumer,
                                                   final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return pipeBodyAndAccept(response, c -> consumer.accept(c, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Reduces given response spec't body as an input stream and returns the result of specified function applies with
     * it.
     *
     * @param response the response spec whose body is reduced.
     * @param function the function to be applied with the reduced body.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    @Deprecated
    public static <R> Mono<R> reduceBodyAsStreamAndApply(final WebClient.ResponseSpec response,
                                                         final Function<? super InputStream, ? extends R> function) {
        if (response == null) {
            throw new NullPointerException("response is null");
        }
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        return reduceAsInputStreamAndApply(response.bodyToFlux(DataBuffer.class), function);
    }

    @Deprecated
    public static <U, R> Mono<R> reduceBodyAsStreamAndApply(
            final WebClient.ResponseSpec response,
            final BiFunction<? super InputStream, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        if (function == null) {
            throw new NullPointerException("function is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return reduceBodyAsStreamAndApply(response, s -> function.apply(s, supplier.get()));
    }

    @Deprecated
    public static Mono<Void> reduceBodyAsStreamAndAccept(final WebClient.ResponseSpec response,
                                                         final Consumer<? super InputStream> consumer) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        return reduceBodyAsStreamAndApply(response,
                                          s -> {
                                              consumer.accept(s);
                                              return s;
                                          })
                .then();
    }

    @Deprecated
    public static <U> Mono<Void> reduceBodyAsStreamAndAccept(final WebClient.ResponseSpec response,
                                                             final BiConsumer<? super InputStream, ? super U> consumer,
                                                             final Supplier<? extends U> supplier) {
        if (consumer == null) {
            throw new NullPointerException("consumer is null");
        }
        if (supplier == null) {
            throw new NullPointerException("supplier is null");
        }
        return reduceBodyAsStreamAndAccept(response, s -> consumer.accept(s, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

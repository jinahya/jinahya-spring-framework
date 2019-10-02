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
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.springframework.core.io.buffer.DataBufferUtils.releaseConsumer;
import static org.springframework.core.io.buffer.DataBufferUtils.write;

/**
 * Utilities and constants for {@link org.springframework.web.reactive.function.client.WebClient.ResponseSpec}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public final class JinahyaResponseSpecUtils {

    // -----------------------------------------------------------------------------------------------------------------
    static Mono<Path> writeBodyToPath(final WebClient.ResponseSpec responseSpec, final Path path) {
        return write(responseSpec.bodyToFlux(DataBuffer.class), path, WRITE).thenReturn(path);
    }

    /**
     * Writes given response spec's body to a path supplied by specified supplier and returns the result of specified
     * function applied with the path.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathSupplier the supplier for the path
     * @param pathFunction the function to be applied with the path
     * @param <R>          result type parameter
     * @return the value the function results
     */
    public static <R> Mono<R> writeBodyToPathAndApply(final WebClient.ResponseSpec responseSpec,
                                                      final Supplier<? extends Path> pathSupplier,
                                                      final Function<? super Path, ? extends R> pathFunction) {
        return writeBodyToPath(responseSpec, pathSupplier.get())
                .map(pathFunction);
    }

    /**
     * Writes given response spec's body to a path supplied by specified supplier and returns the result of specified
     * function applied with the path along with an argument supplied by specified supplier.
     *
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathSupplier     the supplier for the path
     * @param pathFunction     the function to be applied with the path and the second argument
     * @param argumentSupplier the supplier for the second argument of the function
     * @return the value the function results
     * @see #writeBodyToPathAndApply(WebClient.ResponseSpec, Supplier, Function)
     */
    public static <U, R> Mono<R> writeBodyToPathAndApplyWith(
            final WebClient.ResponseSpec responseSpec, final Supplier<? extends Path> pathSupplier,
            final BiFunction<? super Path, ? super U, ? extends R> pathFunction,
            final Supplier<? extends U> argumentSupplier) {
        return writeBodyToPathAndApply(responseSpec, pathSupplier, p -> {
            log.debug("p: {}", p);
            return pathFunction.apply(p, argumentSupplier.get());
        });
    }

    /**
     * Writes given response spec's body to a path supplied by specified supplier and accepts the path to specified
     * consumer.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathSupplier the supplier for the path
     * @param pathConsumer the consumer to be accepted with the path
     * @see #writeBodyToPathAndApply(WebClient.ResponseSpec, Supplier, Function)
     */
    public static Mono<Void> writeBodyToPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                      final Supplier<? extends Path> pathSupplier,
                                                      final Consumer<? super Path> pathConsumer) {
        return writeBodyToPathAndApply(
                responseSpec,
                pathSupplier,
                Function.identity())
                .map(p -> {
                    pathConsumer.accept(p);
                    return p;
                })
                .then();
    }

    /**
     * Writes given response spec's body to a path supplied by specified path supplier and accepts the path to specified
     * path consumer along with an argument supplied by specified argument supplier.
     *
     * @param <U>              second argument type parameter
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathSupplier     the path supplier
     * @param pathConsumer     the path consumer to be accepted with the path along with the second argument
     * @param argumentSupplier the second argument supplier
     * @see #writeBodyToPathAndAccept(WebClient.ResponseSpec, Supplier, Consumer)
     */
    public static <U> Mono<Void> writeBodyToPathAndAcceptWith(final WebClient.ResponseSpec responseSpec,
                                                              final Supplier<? extends Path> pathSupplier,
                                                              final BiConsumer<? super Path, ? super U> pathConsumer,
                                                              final Supplier<? extends U> argumentSupplier) {
        return writeBodyToPathAndAccept(
                responseSpec, pathSupplier, f -> pathConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to a temporary path and returns the result of specified function applied with
     * the path.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathFunction the function to be applied with the path
     * @param <R>          result type parameter
     * @return the result the function results
     */
    public static <R> Mono<R> writeBodyToTempPathAndApply(final WebClient.ResponseSpec responseSpec,
                                                          final Function<? super Path, ? extends R> pathFunction) {
        return writeBodyToPathAndApply(
                responseSpec,
                () -> {
                    try {
                        return createTempFile(null, null);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                },
                pathFunction
        );
    }

    /**
     * Writes given response spec's body to a temporary path and returns the result of specified path function applied
     * with the path along with an argument supplied by specified argument supplier.
     *
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathFunction     the path function to be applied with the path and the second argument
     * @param argumentSupplier the argument spec for the second argument of the path function
     * @return the result the function results
     * @see #writeBodyToTempPathAndApply(WebClient.ResponseSpec, Function)
     */
    public static <U, R> Mono<R> writeBodyToTempPathAndApplyWith(
            final WebClient.ResponseSpec responseSpec,
            final BiFunction<? super Path, ? super U, ? extends R> pathFunction,
            final Supplier<? extends U> argumentSupplier) {
        return writeBodyToTempPathAndApply(responseSpec, p -> pathFunction.apply(p, argumentSupplier.get()));
    }

    /**
     * Writes given response spec's body to a temporary path and accept the path to specified path consumer.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathConsumer the path consumer to be accepted with the path
     * @see #writeBodyToTempPathAndApply(WebClient.ResponseSpec, Function)
     */
    public static Mono<Void> writeBodyToTempPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                          final Consumer<? super Path> pathConsumer) {
        return writeBodyToTempPathAndApply(responseSpec, p -> {
            pathConsumer.accept(p);
            return null;
        });
    }

    /**
     * Writes given response spec's body to a temporary path and accept the path, along with an argument supplied by
     * specified argument supplier, to specified path consumer.
     *
     * @param <U>              second argument type parameter
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathConsumer     the path consumer to be accepted with the path and the second argument
     * @param argumentSupplier the argument supplier for the second argument of the path consumer
     * @see #writeBodyToTempPathAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <U> Mono<Void> writeBodyToTempPathAndAcceptWith(
            final WebClient.ResponseSpec responseSpec, final BiConsumer<? super Path, ? super U> pathConsumer,
            final Supplier<? extends U> argumentSupplier) {
        return writeBodyToTempPathAndAccept(responseSpec, p -> pathConsumer.accept(p, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Pipes given response spec's body and returns the result of specified channel function applied with the piped
     * channel.
     *
     * @param responseSpec    the response spec whose body is pied
     * @param taskExecutor    an executor for blocking underlying data buffer stream
     * @param channelFunction the channel function to be applied with the piped channel
     * @param <R>             result type parameter
     * @return the result the channel function results
     * @throws IOException if an I/O error occurs
     * @see Executor#execute(Runnable)
     */
    public static <R> R pipeBodyToChannelAndApply(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final Function<? super ReadableByteChannel, ? extends R> channelFunction)
            throws IOException {
        final Pipe pipe = Pipe.open();
        taskExecutor.execute(() -> {
            final Flux<DataBuffer> flux = responseSpec.bodyToFlux(DataBuffer.class);
            final Disposable disposable = write(flux, pipe.sink()).subscribe(releaseConsumer());
            final DataBuffer last = flux.blockLast();
            try {
                pipe.sink().close();
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to close the pipe.sink", ioe);
            }
        });
        return channelFunction.apply(pipe.source());
    }

    /**
     * Pipes given response spec's body and returns the result of specified channel function applied with the piped
     * channel along with an argument supplied by specified argument supplier.
     *
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @param responseSpec     the response spec whose body is piped
     * @param taskExecutor     an executor for blocking underlying data buffer stream
     * @param channelFunction  the channel function to be applied with the piped channel along with the second argument
     * @param argumentSupplier the argument supplier for the second argument of channel function
     * @return the result the channel function results
     * @throws IOException if an I/O error occurs.
     * @see #pipeBodyToChannelAndApply(WebClient.ResponseSpec, Executor, Function)
     */
    public static <U, R> R pipeBodyToChannelAndApplyWith(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> channelFunction,
            final Supplier<? extends U> argumentSupplier)
            throws IOException {
        return pipeBodyToChannelAndApply(responseSpec, taskExecutor,
                                         c -> channelFunction.apply(c, argumentSupplier.get()));
    }

    /**
     * Pipes given response spec's body and accept the channel to specified channel consumer.
     *
     * @param responseSpec    the response spec whose body is piped
     * @param taskExecutor    an executor for blocking underlying data buffer stream
     * @param channelConsumer the channel consumer to be accepted with the piped channel
     * @throws IOException if an I/O error occurs
     * @see #pipeBodyToChannelAndApply(WebClient.ResponseSpec, Executor, Function)
     */
    public static void pipeBodyToChannelAndAccept(final WebClient.ResponseSpec responseSpec,
                                                  final Executor taskExecutor,
                                                  final Consumer<? super ReadableByteChannel> channelConsumer)
            throws IOException {
        pipeBodyToChannelAndApply(responseSpec, taskExecutor, c -> {
            channelConsumer.accept(c);
            return null;
        });
    }

    /**
     * Pipes given response spec's body and accept the piped channel to specified channel consumer along with an
     * argument supplied by specified argument supplier.
     *
     * @param <U>              second argument type parameter
     * @param responseSpec     the response spec whose body is piped
     * @param taskExecutor     an executor for blocking the data buffer stream
     * @param channelConsumer  the channel consumer to be accepted with the piped channel along with the second
     *                         argument
     * @param argumentSupplier the argument supplier for the second argument of the channel consumer
     * @throws IOException if an I/O error occurs
     * @see #pipeBodyToChannelAndAccept(WebClient.ResponseSpec, Executor, Consumer)
     */
    public static <U> void pipeBodyToChannelAndAcceptWith(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final BiConsumer<? super ReadableByteChannel, ? super U> channelConsumer,
            final Supplier<? extends U> argumentSupplier)
            throws IOException {
        pipeBodyToChannelAndAccept(responseSpec, taskExecutor, c -> channelConsumer.accept(c, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

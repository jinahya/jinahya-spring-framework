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
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.util.function.Function.identity;
import static org.springframework.core.io.buffer.DataBufferUtils.write;
import static reactor.core.publisher.Mono.using;

/**
 * Utilities and constants for {@link org.springframework.web.reactive.function.client.WebClient.ResponseSpec}.
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
     * @param spec     the response spec whose body is written to the file.
     * @param file     the file to which body of the spec is written.
     * @param function the function to be applied with the file.
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeBodyToFileAndApply(final WebClient.ResponseSpec spec, final Path file,
                                                      final Function<? super Path, ? extends R> function) {
        return write(spec.bodyToFlux(DataBuffer.class), file).thenReturn(file).map(function);
    }

    /**
     * Writes given response spec's body to specified file and returns the result of specified function applied with the
     * file along with an argument supplied by specified supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param spec     the response spec whose body is written to the file.
     * @param file     the file to which the body of the spec is written.
     * @param function the function to be applied with the file and the second argument.
     * @param supplier the supplier for the second argument of the function.
     * @return the value the function results.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, Function)
     */
    public static <U, R> Mono<R> writeBodyToFileAndApply(
            final WebClient.ResponseSpec spec, final Path file,
            final BiFunction<? super Path, ? super U, ? extends R> function, final Supplier<? extends U> supplier) {
        return writeBodyToFileAndApply(
                spec,
                file,
                f -> function.apply(f, supplier.get())
        );
    }

    /**
     * Writes given response spec's body to specified file and accepts the file to specified consumer.
     *
     * @param spec     the response spec whose body is written to the file.
     * @param file     the file to which the body is written.
     * @param consumer the consumer to be accepted with the path.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Path, Function)
     */
    public static Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec spec, final Path file,
                                                      final Consumer<? super Path> consumer) {
        return writeBodyToFileAndApply(spec, file, identity())
                .map(p -> {
                    consumer.accept(p);
                    return p;
                })
                .then();
    }

    /**
     * Writes given response spec's body to a path supplied by specified path supplier and accepts the path to specified
     * path consumer along with an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param spec     the response spec whose body is written to the path
     * @param file     the path supplier
     * @param consumer the path consumer to be accepted with the path along with the second argument
     * @param supplier the second argument supplier
     * @see #writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, Consumer)
     */
    public static <U> Mono<Void> writeBodyToFileAndAccept(final WebClient.ResponseSpec spec, final Path file,
                                                          final BiConsumer<? super Path, ? super U> consumer,
                                                          final Supplier<? extends U> supplier) {
        return writeBodyToFileAndAccept(
                spec,
                file,
                f -> consumer.accept(f, supplier.get())
        );
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to a temporary file and returns the result of specified function applied with
     * the file.
     *
     * @param spec     the response spec whose body is written to the temporary file
     * @param function the function to be applied with the temporary file
     * @param <R>      result type parameter
     * @return a mono of the result of the function.
     */
    public static <R> Mono<R> writeBodyToTempFileAndApply(final WebClient.ResponseSpec spec,
                                                          final Function<? super Path, ? extends R> function) {
        return using(
                () -> createTempFile(null, null),
                f -> write(spec.bodyToFlux(DataBuffer.class), f).thenReturn(f).map(function),
                f -> {
                    try {
                        deleteIfExists(f);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
    }

    /**
     * Writes given response spec's body to a temporary path and returns the result of specified path function applied
     * with the path along with an argument supplied by specified argument supplier.
     *
     * @param <U>      second argument type parameter
     * @param <R>      result type parameter
     * @param spec     the response spec whose body is written to the path
     * @param function the path function to be applied with the path and the second argument
     * @param supplier the argument spec for the second argument of the path function
     * @return the result the function results
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static <U, R> Mono<R> writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec spec,
            final BiFunction<? super Path, ? super U, ? extends R> function,
            final Supplier<? extends U> supplier) {
        return writeBodyToTempFileAndApply(spec, f -> function.apply(f, supplier.get()));
    }

    /**
     * Writes given response spec's body to a temporary path and accept the path to specified path consumer.
     *
     * @param spec     the response spec whose body is written to the path
     * @param consumer the path consumer to be accepted with the path
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static Mono<Void> writeBodyToTempFileAndAccept(final WebClient.ResponseSpec spec,
                                                          final Consumer<? super Path> consumer) {
        return writeBodyToTempFileAndApply(
                spec,
                f -> {
                    consumer.accept(f);
                    return f; // returning null is not welcome
                })
                .then();
    }

    /**
     * Writes given response spec's body to a temporary path and accept the path, along with an argument supplied by
     * specified argument supplier, to specified path consumer.
     *
     * @param <U>      second argument type parameter
     * @param spec     the response spec whose body is written to the path
     * @param consumer the path consumer to be accepted with the path and the second argument
     * @param supplier the argument supplier for the second argument of the path consumer
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <U> Mono<Void> writeBodyToTempFileAndAccept(
            final WebClient.ResponseSpec spec, final BiConsumer<? super Path, ? super U> consumer,
            final Supplier<? extends U> supplier) {
        return writeBodyToTempFileAndAccept(spec, p -> consumer.accept(p, supplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

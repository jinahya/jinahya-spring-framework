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

import com.github.jinahya.junit.jupiter.api.extension.TempFileParameterResolver;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndApply;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.size;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * A class for testing {@link JinahyaResponseSpecUtils}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@ExtendWith({TempFileParameterResolver.class})
@Slf4j
class JinahyaResponseSpecUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------
    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    private static Stream<Arguments> sourceResponseSpec() {
        final LongAdder adder = new LongAdder();
        final Flux<DataBuffer> buffers = Flux.just(
                IntStream.range(0, current().nextInt(1, 128))
                        .mapToObj(i -> {
                                      final int capacity = current().nextInt(1024);
                                      adder.add(capacity);
                                      return DATA_BUFFER_FACTORY.allocateBuffer(capacity).writePosition(capacity);
                                  }
                        )
                        .toArray(DataBuffer[]::new)
        );
        final WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);
        Mockito.when(responseSpec.bodyToFlux(DataBuffer.class)).thenReturn(buffers);
        return Stream.of(Arguments.of(responseSpec, adder.sum()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToFileAndApply(WebClient.ResponseSpec, Path, BiFunction,
     * Supplier)} method.
     *
     * @param response a response spec
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToFileAndApply(final WebClient.ResponseSpec response, final long expected,
                                     @TempFileParameterResolver.TempFile final Path file) {
        final Long actual = writeBodyToFileAndApply(
                response,
                file,
                (f, u) -> {
                    try {
                        return size(f);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                },
                () -> null
        )
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToFileAndAccept(final WebClient.ResponseSpec response, final long expected,
                                      @TempFileParameterResolver.TempFile final Path file) {
        final Void v = writeBodyToFileAndAccept(
                response,
                file,
                (f, u) -> {
                    try {
                        final long actual = size(f);
                        assertEquals(expected, actual);
                    } catch (final IOException ioe) {
                        fail(ioe);
                    }
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndApply(WebClient.ResponseSpec, BiFunction, Supplier)}
     * method.
     *
     * @param responseSpec a response spec to test with.
     * @param expected     an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToTempFileAndApply(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = writeBodyToTempFileAndApply(
                responseSpec,
                (channel, u) -> {
                    assertNotNull(channel);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = allocate(1024);
                    try {
                        for (int r; (r = channel.read(b)) != -1; count += r) {
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    return count;
                },
                () -> null
        )
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)}
     * method.
     *
     * @param responseSpec a response spec to test with.
     * @param expected     an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToTempFileAndAccept(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Void v = writeBodyToTempFileAndAccept(
                responseSpec,
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long actual = 0L;
                    final ByteBuffer b = allocate(1024);
                    try {
                        for (int r; (r = c.read(b)) != -1; actual += r) {
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    assertEquals(expected, actual);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApply(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = pipeBodyAndApply(
                responseSpec,
                newSingleThreadExecutor(),
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = allocate(128);
                    try {
                        for (int r; (r = c.read(b)) != -1; count += r) {
//                            log.debug("r: {}", r);
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    return count;
                },
                () -> null
        )
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApplyEscape(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = pipeBodyAndApply(
                responseSpec,
                newSingleThreadExecutor(),
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = allocate(128);
                    try {
                        for (int r; (r = c.read(b)) != -1; count += r) {
                            if (current().nextBoolean()) {
                                break;
                            }
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    return count;
                },
                () -> null
        )
                .block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAccept(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Void v = pipeBodyAndAccept(
                responseSpec,
                newSingleThreadExecutor(),
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long actual = 0L;
                    final ByteBuffer b = allocate(128);
                    try {
                        for (int r; (r = c.read(b)) != -1; actual += r) {
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    assertEquals(expected, actual);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAcceptEscape(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Void v = pipeBodyAndAccept(
                responseSpec,
                newSingleThreadExecutor(),
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long actual = 0L;
                    final ByteBuffer b = allocate(128);
                    try {
                        for (int r; (r = c.read(b)) != -1; actual += r) {
                            if (current().nextBoolean()) {
                                break;
                            }
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    assertTrue(actual <= expected);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }
}

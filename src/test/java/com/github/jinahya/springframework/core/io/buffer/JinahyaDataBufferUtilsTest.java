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

import com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
import static java.lang.Runtime.getRuntime;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.size;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A class for testing {@link JinahyaResponseSpecUtils}.
 */
@Slf4j
public class JinahyaDataBufferUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    public static Stream<Arguments> sourceDataBuffers() {
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
        return Stream.of(Arguments.of(buffers, adder.sum()));
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void checkTotalSize(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = buffers.reduce(0L, (a, b) -> a + b.readableByteCount()).block();
        assertNotNull(actual);
        Assertions.assertEquals(expected, actual.longValue());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, Function)} method.
     *
     * @param buffers a stream of data buffers.
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteAndApply(final Flux<DataBuffer> buffers, final long expected) throws IOException {
        final Path file = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(!exists(file) || deleteIfExists(file));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Long actual = writeAndApply(
                buffers,
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
        Assertions.assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteAndAccept(final Flux<DataBuffer> buffers, final long expected) throws IOException {
        final Path file = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(!exists(file) || deleteIfExists(file));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Void v = writeAndAccept(
                buffers,
                file,
                (f, u) -> {
                    try {
                        final long actual = size(f);
                        Assertions.assertEquals(expected, actual);
                    } catch (final IOException ioe) {
                        Assertions.fail(ioe);
                    }
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteToTempFileAndApply(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = JinahyaDataBufferUtils.writeToTempFileAndApply(
                buffers,
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = allocate(1024);
                    try {
                        for (int r; (r = c.read(b)) != -1; count += r) {
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
        Assertions.assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteToTempFileAndAccept(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = JinahyaDataBufferUtils.writeToTempFileAndAccept(
                buffers,
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
                    Assertions.assertEquals(expected, actual);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApply(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(
                buffers,
                newSingleThreadExecutor(),
                (c, u) -> {
                    assertNotNull(c);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = allocate(128);
                    try {
                        for (int r; (r = c.read(b)) != -1; count += r) {
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
        Assertions.assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApplyEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(
                buffers,
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

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAccept(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(
                buffers,
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
                    Assertions.assertEquals(expected, actual);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAcceptEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(
                buffers,
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

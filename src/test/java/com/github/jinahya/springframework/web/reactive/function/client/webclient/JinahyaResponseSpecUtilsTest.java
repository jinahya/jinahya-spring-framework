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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndApply;
import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.size;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static reactor.core.publisher.Flux.just;

/**
 * A class for testing {@link JinahyaResponseSpecUtils}.
 */
@Slf4j
public class JinahyaResponseSpecUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    public static WebClient.ResponseSpec mockResponseSpec(final int count, final int capacity) {
        final byte[] bytes = new byte[capacity];
        final WebClient.ResponseSpec mock = mock(WebClient.ResponseSpec.class);
        when(mock.bodyToFlux(DataBuffer.class)).thenReturn(just(
                range(0, count)
                        .mapToObj(i -> DATA_BUFFER_FACTORY.allocateBuffer(capacity))
                        .peek(b -> {
                            assertEquals(capacity, b.capacity());
                            assertEquals(0, b.readPosition());
                            assertEquals(0, b.writePosition());
                        })
                        .map(b -> b.write(bytes))
                        .peek(b -> {
                            assertEquals(capacity, b.capacity());
                            assertEquals(0, b.readPosition());
                            assertEquals(capacity, b.writePosition());
                        })
                        .toArray(DataBuffer[]::new)));
        return mock;
    }

    private static final int DATA_BUFFER_CAPACITY = 128;

    private static final int DATA_BUFFER_COUNT = 1024;

    private static final long EXPECTED_SIZE = DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT;

    public static Stream<Arguments> sourceResponseSpec() {
        return Stream.of(Arguments.of(mockResponseSpec(DATA_BUFFER_COUNT, DATA_BUFFER_CAPACITY)));
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void checkTotalSize(final WebClient.ResponseSpec responseSpec) {
        final Long sum = responseSpec.bodyToFlux(DataBuffer.class)
                .reduce(0L, (a, b) -> a + b.readableByteCount()).block();
        assertNotNull(sum);
        assertEquals(EXPECTED_SIZE, sum.longValue());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToFileAndApply(WebClient.ResponseSpec, Path, BiFunction,
     * Supplier)} method.
     *
     * @param responseSpec a response spec
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    public void testWriteBodyToFileAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path tempFile = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(!exists(tempFile) || deleteIfExists(tempFile));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Long size = writeBodyToFileAndApply(
                responseSpec,
                tempFile,
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
        assertEquals(EXPECTED_SIZE, size);
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    public void testWriteBodyToFileAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path tempFile = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(!exists(tempFile) || deleteIfExists(tempFile));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Void v = writeBodyToFileAndAccept(
                responseSpec,
                tempFile,
                (f, u) -> {
                    try {
                        final long size = size(f);
                        assertEquals(EXPECTED_SIZE, size);
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
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    public void testWriteBodyToTempFileAndApply(final WebClient.ResponseSpec responseSpec) {
        final Long size = writeBodyToTempFileAndApply(
                responseSpec,
                (channel, u) -> {
                    assertNotNull(channel);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = ByteBuffer.allocate(1024);
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
        assertEquals(EXPECTED_SIZE, size);
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)}
     * method.
     *
     * @param responseSpec a response spec to test with.
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    public void testWriteBodyToTempFileAndAccept(final WebClient.ResponseSpec responseSpec) {
        final Void v = writeBodyToTempFileAndAccept(
                responseSpec,
                (channel, u) -> {
                    assertNotNull(channel);
                    assertNull(u);
                    long count = 0L;
                    final ByteBuffer b = ByteBuffer.allocate(1024);
                    try {
                        for (int r; (r = channel.read(b)) != -1; count += r) {
                            b.clear();
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    assertEquals(EXPECTED_SIZE, count);
                },
                () -> null
        )
                .block();
        assertNull(v);
    }
}

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
import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToPathAndAcceptWith;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToPathAndApplyWith;
import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
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

    public static WebClient.ResponseSpec mockResponseSpecBodyToFluxOfDataBuffers(final int count, final int capacity) {
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

    private static final int DATA_BUFFER_CAPACITY = 2;

    private static final int DATA_BUFFER_COUNT = 2;

    private static final long EXPECTED_SIZE = DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT;

    public static Stream<Arguments> sourceResponseSpecBodyToFluxOfDataBuffers() {
        return Stream.of(Arguments.of(
                mockResponseSpecBodyToFluxOfDataBuffers(DATA_BUFFER_COUNT, DATA_BUFFER_CAPACITY)));
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    void checkTotalSize(final WebClient.ResponseSpec responseSpec) {
        final Long sum = responseSpec.bodyToFlux(DataBuffer.class)
                .reduce(0L, (a, b) -> a + b.readableByteCount()).block();
        assertNotNull(sum);
        assertEquals(EXPECTED_SIZE, sum.longValue());
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToPathAndApplyWith(WebClient.ResponseSpec, Supplier, BiFunction,
     * Supplier)}.
     *
     * @param responseSpec a response spec
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToPathAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path path = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(deleteIfExists(path));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Long size = writeBodyToPathAndApplyWith(
                responseSpec,
                () -> path,
                (p, u) -> {
                    try {
                        return size(p);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                },
                () -> null)
                .block();
        log.debug("size: {}", size);
        assertEquals(EXPECTED_SIZE, size);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToPathAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path path = createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                assertTrue(deleteIfExists(path));
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final Void v = writeBodyToPathAndAcceptWith(
                responseSpec,
                () -> path,
                (p, u) -> {
                    try {
                        final long size = size(p);
                        assertEquals(EXPECTED_SIZE, size);
                    } catch (final IOException ioe) {
                        fail(ioe);
                    }
                }, () -> null
        ).block();
        assertNull(v);
    }

//    // -----------------------------------------------------------------------------------------------------------------
//    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
//    @ParameterizedTest
//    public void testWriteBodyToTempPathAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
//        final long size = JinahyaResponseSpecUtils.writeBodyToTempPathAndApplyWith(
//                responseSpec,
//                (p, u) -> {
//                    try {
//                        return size(p);
//                    } catch (final IOException ioe) {
//                        throw new RuntimeException(ioe);
//                    }
//                }, () -> null
//        );
//        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
//    }
//
//    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
//    @ParameterizedTest
//    public void testWriteBodyToTempPathAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
//        JinahyaResponseSpecUtils.writeBodyToTempPathAndAcceptWith(
//                responseSpec,
//                (p, u) -> {
//                    try {
//                        final long size = size(p);
//                        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
//                    } catch (final IOException ioe) {
//                        fail(ioe);
//                    }
//                }, () -> null
//        );
//    }
//
//    // -----------------------------------------------------------------------------------------------------------------
//    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
//    @ParameterizedTest
//    public void testPipeBodyToChannelAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
//        final long sum = JinahyaResponseSpecUtils.pipeBodyToChannelAndApplyWith(
//                responseSpec,
//                newFixedThreadPool(2),
//                (c, u) -> {
//                    final LongAdder adder = new LongAdder();
//                    try {
//                        for (final ByteBuffer b = allocate(65536); c.read(b) != -1; b.clear()) {
//                            adder.add(b.position());
//                        }
//                    } catch (final IOException ioe) {
//                        throw new RuntimeException(ioe);
//                    }
//                    return adder.sum();
//                }, () -> null
//        );
//        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, sum);
//    }
//
//    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
//    @ParameterizedTest
//    public void testPipeBodyToChannelAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
//        final LongAdder adder = new LongAdder();
//        JinahyaResponseSpecUtils.pipeBodyToChannelAndAcceptWith(
//                responseSpec,
//                newFixedThreadPool(2),
//                (c, u) -> {
//                    try {
//                        for (final ByteBuffer b = allocate(65536); c.read(b) != -1; b.clear()) {
//                            adder.add(b.position());
//                        }
//                    } catch (final IOException ioe) {
//                        fail(ioe);
//                    }
//                }, () -> null
//        );
//        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, adder.sum());
//    }
}

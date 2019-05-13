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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.ByteBuffer.allocate;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.slf4j.LoggerFactory.getLogger;
import static reactor.core.publisher.Flux.just;

/**
 * A class for testing {@link JinahyaResponseSpecUtils}.
 */
public class JinahyaResponseSpecUtilsTest {

    private static final Logger logger = getLogger(lookup().lookupClass());

    // -----------------------------------------------------------------------------------------------------------------

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    public static WebClient.ResponseSpec mockResponseSpecBodyToFluxOfDataBuffers(final int count, final int capacity) {
        final WebClient.ResponseSpec mock = mock(WebClient.ResponseSpec.class);
        when(mock.bodyToFlux(DataBuffer.class)).thenReturn(just(
                range(0, count)
                        .mapToObj(DATA_BUFFER_FACTORY::allocateBuffer)
                        .map(b -> b.write(new byte[capacity]))
                        .toArray(DataBuffer[]::new)));
        return mock;
    }

    private static final int DATA_BUFFER_CAPACITY = 8192;

    private static final int DATA_BUFFER_COUNT = 8192;

    public static Stream<Arguments> sourceResponseSpecBodyToFluxOfDataBuffers() {
        return Stream.of(Arguments.of(
                mockResponseSpecBodyToFluxOfDataBuffers(DATA_BUFFER_COUNT, DATA_BUFFER_CAPACITY)));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToFileAndApply(WebClient.ResponseSpec, Supplier, Supplier,
     * BiFunction)}.
     *
     * @param responseSpec a response spec parameter
     * @throws IOException if an I/O error occurs
     */
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToFileAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final File file = File.createTempFile("tmp", null);
        file.deleteOnExit();
        final long length = JinahyaResponseSpecUtils.writeBodyToFileAndApply(
                responseSpec,
                () -> file,
                () -> null,
                (f, u) -> f.length()
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, length);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToFileAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final File file = File.createTempFile("tmp", null);
        file.deleteOnExit();
        JinahyaResponseSpecUtils.writeBodyToFileAndAccept(
                responseSpec,
                () -> file,
                () -> null,
                (f, u) -> {
                    assertNull(u);
                    assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, f.length());
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndApply(WebClient.ResponseSpec, Supplier,
     * BiFunction)}.
     *
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToTempFileAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final long length = JinahyaResponseSpecUtils.writeBodyToTempFileAndApply(
                responseSpec,
                () -> null,
                (f, u) -> f.length()
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, length);
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Supplier,
     * BiConsumer)}.
     *
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToTempFileAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept(
                responseSpec,
                () -> null,
                (f, u) -> {
                    assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, f.length());
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToPathAndApply(WebClient.ResponseSpec, Supplier, Supplier,
     * BiFunction)}.
     *
     * @param responseSpec a response spec
     * @throws IOException if an I/O error occurs.
     */
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToPathAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path path = Files.createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = Files.deleteIfExists(path);
                logger.trace("deleted: {}", deleted);
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final long size = JinahyaResponseSpecUtils.writeBodyToPathAndApply(
                responseSpec,
                () -> path,
                () -> null,
                (p, u) -> {
                    try {
                        return Files.size(p);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToPathAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final Path path = Files.createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = Files.deleteIfExists(path);
                logger.trace("deleted: {}", deleted);
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        JinahyaResponseSpecUtils.writeBodyToPathAndAccept(
                responseSpec,
                () -> path,
                () -> null,
                (p, u) -> {
                    try {
                        final long size = Files.size(p);
                        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
                    } catch (final IOException ioe) {
                        fail(ioe);
                    }
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToTempPathAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final long size = JinahyaResponseSpecUtils.writeBodyToTempPathAndApply(
                responseSpec,
                () -> null,
                (p, u) -> {
                    try {
                        return Files.size(p);
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testWriteBodyToTempPathAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        JinahyaResponseSpecUtils.writeBodyToTempPathAndAccept(
                responseSpec,
                () -> null,
                (p, u) -> {
                    try {
                        final long size = Files.size(p);
                        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, size);
                    } catch (final IOException ioe) {
                        fail(ioe);
                    }
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testPipeBodyToStreamAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final int pipeSize = 65536;
        final long sum = JinahyaResponseSpecUtils.pipeBodyToStreamAndApply(
                pipeSize,
                responseSpec,
                newFixedThreadPool(1),
                () -> null,
                (s, u) -> {
                    final LongAdder adder = new LongAdder();
                    final byte[] b = new byte[pipeSize];
                    try {
                        for (int r; (r = s.read(b)) != -1; adder.add(r)) ;
                        return adder.sum();
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, sum);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testPipeBodyToStreamAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final LongAdder adder = new LongAdder();
        final int pipeSize = 65536;
        JinahyaResponseSpecUtils.pipeBodyToStreamAndAccept(
                pipeSize,
                responseSpec,
                newFixedThreadPool(1),
                () -> null,
                (s, u) -> {
                    final byte[] b = new byte[pipeSize];
                    try {
                        for (int r; (r = s.read(b)) != -1; adder.add(r)) ;
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, adder.sum());
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testPipeBodyToChannelAndApply(final WebClient.ResponseSpec responseSpec) throws IOException {
        final long sum = JinahyaResponseSpecUtils.pipeBodyToChannelAndApply(
                responseSpec,
                newFixedThreadPool(1),
                () -> null,
                (c, u) -> {
                    final LongAdder adder = new LongAdder();
                    try {
                        for (final ByteBuffer b = allocate(65536); c.read(b) != -1; b.clear()) {
                            adder.add(b.position());
                        }
                    } catch (final IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                    return adder.sum();
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, sum);
    }

    @MethodSource({"sourceResponseSpecBodyToFluxOfDataBuffers"})
    @ParameterizedTest
    public void testPipeBodyToChannelAndAccept(final WebClient.ResponseSpec responseSpec) throws IOException {
        final LongAdder adder = new LongAdder();
        JinahyaResponseSpecUtils.pipeBodyToChannelAndAccept(
                responseSpec,
                newFixedThreadPool(1),
                () -> null,
                (c, u) -> {
                    try {
                        for (final ByteBuffer b = allocate(65536); c.read(b) != -1; b.clear()) {
                            adder.add(b.position());
                        }
                    } catch (final IOException ioe) {
                        fail(ioe);
                    }
                }
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, adder.sum());
    }
}

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

import com.github.jinahya.junit.jupiter.api.extension.TempFileParameterResolver;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.reduceAsInputStreamAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.reduceAsInputStreamAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndApply;
import static java.nio.file.Files.size;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * A class for unit-testing {@link JinahyaDataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@ExtendWith({TempFileParameterResolver.class})
@Slf4j
public class JinahyaDataBufferUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    public static DataBuffer dataBuffer(final int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity(" + capacity + ") <= 0");
        }
        return DATA_BUFFER_FACTORY.allocateBuffer(capacity).writePosition(capacity);
    }

    public static Flux<DataBuffer> dataBuffers(final int expected) {
        if (expected <= 0) {
            throw new IllegalArgumentException("expected(" + expected + ") <= 0");
        }
        return Flux.generate(() -> expected, (e, s) -> {
            if (e == 0) {
                s.complete();
            } else {
                final int capacity = current().nextInt(e + 1);
                e -= capacity;
                assert e >= 0;
                final DataBuffer dataBuffer = dataBuffer(capacity);
                s.next(dataBuffer);
            }
            return e;
        });
    }

    private static Stream<Arguments> sourceDataBuffersWithExpected() {
        final int expected = current().nextInt(8192);
        final Flux<DataBuffer> buffers = dataBuffers(expected);
        return Stream.of(Arguments.of(buffers, expected));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<Path, Long> FS1 = f -> {
        try {
            return size(f);
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
    };

    /**
     * A function for getting the size of the file.
     */
    public static final BiFunction<Path, Object, Long> FS2 = (f, u) -> FS1.apply(f);

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<InputStream, Long> SR1 = s -> {
        try {
            long size = 0L;
            final byte[] buffer = new byte[128];
            for (int r; (r = s.read(buffer)) != -1; ) {
                size += r;
            }
            return size;
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
    };

    public static final BiFunction<InputStream, Object, Long> SR2 = (s, u) -> SR1.apply(s);

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<ReadableByteChannel, Long> CR1 = c -> {
        long s = 0L;
        final ByteBuffer b = ByteBuffer.allocate(128);
        try {
            for (int r; (r = c.read(b)) != -1; s += r) {
                b.clear();
            }
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return s;
    };

    /**
     * A function for getting size of the channel.
     */
    public static final BiFunction<ReadableByteChannel, Object, Long> CR = (c, u) -> CR1.apply(c);

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<ReadableByteChannel, Long> CE1 = c -> {
        long s = 0L;
        final ByteBuffer b = ByteBuffer.allocate(128);
        try {
            for (int r; (r = c.read(b)) != -1; s += r) {
                if (current().nextBoolean()) {
                    break;
                }
                b.clear();
            }
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return s;
    };

    /**
     * A function for getting size of the channel. Possibly escape.
     */
    public static final BiFunction<ReadableByteChannel, Object, Long> CE2 = (c, u) -> CE1.apply(c);

    // --------------------------------------------------------------------------------------- writeAndApplyWithFunction

    /**
     * Asserts {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, Function)} method throws a {@code
     * NullPointerException} when {@code function} is {@code null}.
     */
    @Test
    void assertWriteAndApplyWithFunctionThrowsNullPointerExceptionWhenFunctionIsNull() {
        @SuppressWarnings({"unchecked"})
        final Publisher<DataBuffer> source = (Publisher<DataBuffer>) mock(Publisher.class);
        assertThrows(NullPointerException.class, () -> writeAndApply(source, mock(Path.class), null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, Function)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndApplyWithFunction(final Flux<DataBuffer> source, final int expected,
                                       @TempFileParameterResolver.TempFile final Path destination) {
        final Long actual = writeAndApply(source, destination, FS1).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    // ------------------------------------------------------------------------------------- writeAndApplyWithBiFunction
    @Test
    void assertWriteAndApplyWithBiFunctionThrowsNullPointerExceptionWhenFunctionIsNull() {
        @SuppressWarnings({"unchecked"})
        final Publisher<DataBuffer> source = (Publisher<DataBuffer>) mock(Publisher.class);
        assertThrows(NullPointerException.class, () -> writeAndApply(source, mock(Path.class), null, () -> null));
    }

    @Test
    void assertWriteAndApplyWithBiFunctionThrowsNullPointerExceptionWhenSupplierIsNull() {
        @SuppressWarnings({"unchecked"})
        final Publisher<DataBuffer> source = (Publisher<DataBuffer>) mock(Publisher.class);
        assertThrows(NullPointerException.class, () -> writeAndApply(source, mock(Path.class), (f, u) -> f, null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, BiFunction, Supplier)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndApplyWithBiFunction(final Flux<DataBuffer> source, final int expected,
                                         @TempFileParameterResolver.TempFile final Path destination) {
        final Long actual = writeAndApply(source, destination, FS2, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    // -------------------------------------------------------------------------------------- writeAndAcceptWithConsumer
    @Test
    void assertWriteAndAcceptWithConsumerThrowsNullPointerExceptionWhenConsumerIsNull() {
        @SuppressWarnings({"unchecked"})
        final Publisher<DataBuffer> source = (Publisher<DataBuffer>) mock(Publisher.class);
        assertThrows(NullPointerException.class, () -> writeAndAccept(source, mock(Path.class), null));
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndAcceptWithConsumer(final Flux<DataBuffer> source, final int expected,
                                        @TempFileParameterResolver.TempFile final Path destination) {
        writeAndAccept(source,
                       destination,
                       f -> {
                           final Long actual = FS1.apply(f);
                           assertNotNull(actual);
                           assertEquals(expected, actual.longValue());
                       })
                .block();
    }

    // ------------------------------------------------------------------------------------ writeAndAcceptWithBiConsumer

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndAccept(Publisher, Path, BiConsumer, Supplier)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndAcceptWithBiConsumer(final Flux<DataBuffer> source, final int expected,
                                          @TempFileParameterResolver.TempFile final Path destination) {
        writeAndAccept(source, destination, (f, u) -> assertEquals(expected, FS2.apply(f, u)), () -> null).block();
    }

    // ------------------------------------------------------------------------------------ writeToTempAndWithBiConsumer

    /**
     * Test {@link JinahyaDataBufferUtils#writeToTempFileAndApply(Publisher, BiFunction, Supplier)} method.
     *
     * @param source   a stream of data buffers.
     * @param expected an expected total size of data buffers.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteToTempFileAndApply(final Flux<DataBuffer> source, final int expected) {
        final Long actual = writeToTempFileAndApply(source, CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeToTempFileAndAccept(Publisher, BiConsumer, Supplier)} method.
     *
     * @param source   a stream of data buffers.
     * @param expected an expected total size of data buffers.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteToTempFileAndAccept(final Flux<DataBuffer> source, final int expected) {
        writeToTempFileAndAccept(source, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null).block();
    }

    // --------------------------------------------------------------------------------------------------------- pipeAnd
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndApplyWithExecutor(final Flux<DataBuffer> source, final int expected) {
        final Long actual = pipeAndApply(source, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndApplyWithExecutorEscape(final Flux<DataBuffer> source, final int expected) {
        final Long actual = pipeAndApply(source, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndAcceptWithExecutor(final Flux<DataBuffer> source, final int expected) {
        pipeAndAccept(source, newSingleThreadExecutor(), (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null)
                .block();
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndAcceptWithExecutorEscape(final Flux<DataBuffer> source, final int expected) {
        pipeAndAccept(source, newSingleThreadExecutor(), (c, u) -> assertTrue(CE2.apply(c, u) <= expected), () -> null)
                .block();
    }

    // --------------------------------------------------------------------------------------------------------- pipeAnd

    /**
     * Tests {@link JinahyaDataBufferUtils#pipeAndApply(Publisher, BiFunction, Supplier)} method.
     *
     * @param source   a stream of data buffers.
     * @param expected an expected total size of data buffers.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndApplyWithCompletableFuture(final Flux<DataBuffer> source, final int expected) {
        final Long actual = pipeAndApply(source, CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndApplyWithCompletableFutureEscape(final Flux<DataBuffer> source, final int expected) {
        final Long actual = pipeAndApply(source, CE2, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#pipeAndAccept(Publisher, BiConsumer, Supplier)} method.
     *
     * @param source   a stream of data buffers.
     * @param expected an expected total size of data buffers.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndAcceptWithCompletableFuture(final Flux<DataBuffer> source, final int expected) {
        pipeAndAccept(source, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null).block();
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testPipeAndAcceptWithCompletableFutureEscape(final Flux<DataBuffer> source, final int expected) {
        pipeAndAccept(source, (c, u) -> assertTrue(CE2.apply(c, u) <= expected), () -> null).block();
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testReduceAsStreamAndApply(final Flux<DataBuffer> source, final int expected) {
        final Long actual = reduceAsInputStreamAndApply(source, SR2, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testReduceAsStreamAndAccept(final Flux<DataBuffer> source, final int expected) {
        reduceAsInputStreamAndAccept(source,
                                     (s, u) -> {
                                         final Long actual = SR2.apply(s, u);
                                         assertNotNull(actual);
                                         assertEquals(expected, actual.longValue());
                                     },
                                     () -> null)
                .block();
    }
}

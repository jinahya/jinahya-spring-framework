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

import com.github.jinahya.enterprise.inject.TempFileProducer;
import com.github.jinahya.junit.jupiter.api.extension.TempFileParameterResolver;
import lombok.extern.slf4j.Slf4j;
import org.jboss.weld.junit5.auto.AddBeanClasses;
import org.jboss.weld.junit5.auto.WeldJunit5AutoExtension;
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

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
import static java.nio.file.Files.size;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * A class for unit-testing {@link JinahyaDataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@AddBeanClasses({TempFileProducer.class})
@ExtendWith({TempFileParameterResolver.class, WeldJunit5AutoExtension.class})
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

    /**
     * Sources a stream of data buffers and an expected total number of bytes of the stream.
     *
     * @return a stream of arguments.
     */
    private static Stream<Arguments> sourceDataBuffersWithExpected() {
        final int expected = current().nextInt(8192);
        final Flux<DataBuffer> buffers = dataBuffers(expected);
        return Stream.of(Arguments.of(buffers, expected));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A function returns the {@link java.nio.file.Files#size(Path) size} of given file.
     */
    public static final Function<Path, Long> FS1 = f -> {
        try {
            return size(f);
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
    };

    /**
     * A binary function returns the {@link java.nio.file.Files#size(Path) size} of given file.
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
     * Asserts {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, OpenOption[], Function)} method throws a
     * {@code NullPointerException} when {@code function} is {@code null}.
     */
    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndApplyWithFunctionThrowsNullPointerExceptionWhenFunctionIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndApply(mock(Publisher.class), mock(Path.class), null, null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, OpenOption[], Function)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndApplyWithFunction(final Flux<DataBuffer> source, final int expected,
                                       @TempFileParameterResolver.TempFile final Path destination) {
        final Long actual = writeAndApply(source, destination, null, FS1).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    // ------------------------------------------------------------------------------------- writeAndApplyWithBiFunction
    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndApplyWithBiFunctionThrowsNullPointerExceptionWhenFunctionIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndApply(mock(Publisher.class), mock(Path.class), null, null, () -> null));
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndApplyWithBiFunctionThrowsNullPointerExceptionWhenSupplierIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndApply(mock(Publisher.class), mock(Path.class), null, (f, u) -> f, null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, OpenOption[], BiFunction, Supplier)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndApplyWithBiFunction(final Flux<DataBuffer> source, final int expected,
                                         @TempFileParameterResolver.TempFile final Path destination) {
        final Long actual = writeAndApply(source, destination, null, FS2, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    // -------------------------------------------------------------------------------------- writeAndAcceptWithConsumer
    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndAcceptWithConsumerThrowsNullPointerExceptionWhenConsumerIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndAccept(mock(Publisher.class), mock(Path.class), null, null));
    }

    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndAcceptWithConsumer(final Flux<DataBuffer> source, final int expected,
                                        @TempFileParameterResolver.TempFile final Path destination) {
        JinahyaDataBufferUtils
                .writeAndAccept(source,
                                destination,
                                null,
                                f -> {
                                    final Long actual = FS1.apply(f);
                                    assertNotNull(actual);
                                    assertEquals(expected, actual.longValue());
                                })
                .block();
    }

    // ------------------------------------------------------------------------------------ writeAndAcceptWithBiConsumer
    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndAcceptWithBiConsumerThrowsNullPointerExceptionWhenConsumerIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndAccept(mock(Publisher.class), mock(Path.class), null, null, () -> null));
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndAcceptWithBiConsumerThrowsNullPointerExceptionWhenSupplierIsNull() {
        assertThrows(NullPointerException.class,
                     () -> writeAndAccept(mock(Publisher.class), mock(Path.class), null, mock(BiConsumer.class), null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndAccept(Publisher, Path, OpenOption[], BiConsumer, Supplier)} method.
     *
     * @param source      a stream of data buffers.
     * @param expected    an expected total size of data buffers.
     * @param destination a temp file to which data buffers are written.
     */
    @MethodSource({"sourceDataBuffersWithExpected"})
    @ParameterizedTest
    void testWriteAndAcceptWithBiConsumer(final Flux<DataBuffer> source, final int expected,
                                          @TempFileParameterResolver.TempFile final Path destination) {
        JinahyaDataBufferUtils
                .writeAndAccept(
                        source,
                        destination,
                        null,
                        (f, u) -> assertEquals(expected, FS2.apply(f, u)), () -> null)
                .block();
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
        final Long actual = JinahyaDataBufferUtils.writeToTempFileAndApply(source, CR, () -> null).block();
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
        JinahyaDataBufferUtils
                .writeToTempFileAndAccept(
                        source,
                        (c, u) -> assertEquals(expected, CR.apply(c, u)),
                        () -> null)
                .block();
    }

    // -----------------------------------------------------------------------------------------------------------------
    @TempFileProducer.TempFile
    @Inject
    private Path tempFile;
}

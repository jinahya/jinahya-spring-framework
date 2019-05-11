package com.github.jinahya.springframework.web.reactive.function.client.webclient;

import org.junit.jupiter.api.Test;
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

import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyToChannelAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyToStreamAndAccept;
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

    private static final int DATA_BUFFER_CAPACITY = 1024;

    private static final int DATA_BUFFER_COUNT = 1024;

    private static final DataBufferFactory DATA_BUFFER_FACTORY
            = new DefaultDataBufferFactory(false, DATA_BUFFER_CAPACITY);

    static WebClient.ResponseSpec responseSpec() {
        final WebClient.ResponseSpec mock = mock(WebClient.ResponseSpec.class);
        when(mock.bodyToFlux(DataBuffer.class)).thenReturn(just(
                range(0, DATA_BUFFER_COUNT)
                        .mapToObj(DATA_BUFFER_FACTORY::allocateBuffer)
                        .map(b -> b.write(new byte[DATA_BUFFER_CAPACITY]))
                        .toArray(DataBuffer[]::new)));
        return mock;
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    public void testWriteBodyToFileAndApply() throws IOException {
        final File file = File.createTempFile("tmp", null);
        file.deleteOnExit();
        final long length = JinahyaResponseSpecUtils.writeBodyToFileAndApply(
                responseSpec(),
                () -> file,
                () -> null,
                (f, u) -> f.length()
        );
        assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, length);
    }

    @Test
    public void testWriteBodyToFileAndAccept() throws IOException {
        final File file = File.createTempFile("tmp", null);
        file.deleteOnExit();
        JinahyaResponseSpecUtils.writeBodyToFileAndAccept(
                responseSpec(),
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
    @Test
    public void testWriteBodyToTempFileAndApply() throws IOException {
        final long length = JinahyaResponseSpecUtils.writeBodyToTempFileAndApply(
                responseSpec(),
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
    @Test
    public void testWriteBodyToTempFileAndAccept() throws IOException {
        JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept(
                responseSpec(),
                () -> null,
                (f, u) -> {
                    assertEquals(DATA_BUFFER_CAPACITY * DATA_BUFFER_COUNT, f.length());
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    public void testWriteBodyToPathAndApply() throws IOException {
        final Path path = Files.createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = Files.deleteIfExists(path);
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        final long size = JinahyaResponseSpecUtils.writeBodyToPathAndApply(
                responseSpec(),
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

    @Test
    public void testWriteBodyToPathAndAccept() throws IOException {
        final Path path = Files.createTempFile(null, null);
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = Files.deleteIfExists(path);
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }));
        JinahyaResponseSpecUtils.writeBodyToPathAndAccept(
                responseSpec(),
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
    @Test
    public void testWriteBodyToTempPathAndApply() throws IOException {
        final long size = JinahyaResponseSpecUtils.writeBodyToTempPathAndApply(
                responseSpec(),
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

    @Test
    public void testWriteBodyToTempPathAndAccept() throws IOException {
        JinahyaResponseSpecUtils.writeBodyToTempPathAndAccept(
                responseSpec(),
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
    @Test
    public void testPipeBodyToStreamAndApply() throws IOException {
        final long sum = JinahyaResponseSpecUtils.pipeBodyToStreamAndApply(
                32768,
                responseSpec(),
                newFixedThreadPool(1),
                () -> null,
                (s, u) -> {
                    final LongAdder adder = new LongAdder();
                    final byte[] b = new byte[1048576];
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

    @Test
    public void testPipeBodyToStreamAndAccept() throws IOException {
        final LongAdder adder = new LongAdder();
        pipeBodyToStreamAndAccept(
                32768,
                responseSpec(),
                newFixedThreadPool(10),
                () -> null,
                (s, u) -> {
                    final byte[] b = new byte[1048576];
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
    @Test
    public void testPipeBodyToChannelAndApply() throws IOException {
        final long sum = JinahyaResponseSpecUtils.pipeBodyToChannelAndApply(
                responseSpec(),
                newFixedThreadPool(1),
                () -> null,
                (c, u) -> {
                    final LongAdder adder = new LongAdder();
                    try {
                        for (final ByteBuffer b = allocate(1048576); c.read(b) != -1; b.clear()) {
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

    @Test
    public void testPipeBodyToChannelAndAccept() throws IOException {
        final LongAdder adder = new LongAdder();
        pipeBodyToChannelAndAccept(
                responseSpec(),
                newFixedThreadPool(1),
                () -> null,
                (c, u) -> {
                    try {
                        for (final ByteBuffer b = allocate(1048576); c.read(b) != -1; b.clear()) {
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

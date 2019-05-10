package com.github.jinahya.springframework.web.reactive.function.client.webclient;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyToChannelAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyToStreamAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept;
import static java.nio.ByteBuffer.allocate;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.IntStream.range;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static reactor.core.publisher.Flux.just;

@Slf4j
public class JinahyaResponseSpecUtilsTest {

    static final DefaultDataBufferFactory DEFAULT_DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    static WebClient.ResponseSpec responseSpec() {
        final WebClient.ResponseSpec mock = mock(WebClient.ResponseSpec.class);
        when(mock.bodyToFlux(DataBuffer.class))
                .thenReturn(
                        just(
                                range(0, 32768)
                                        .mapToObj(DEFAULT_DATA_BUFFER_FACTORY::allocateBuffer)
                                        .map(b -> b.write(new byte[b.capacity()]))
                                        .toArray(DataBuffer[]::new)));
        return mock;
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    public void testWriteBodyToTempFileAndAccept() throws IOException {
        writeBodyToTempFileAndAccept(responseSpec(), f -> {
            log.debug("f.length: {}", f.length());
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    public void testPipeBodyToStreamAndAccept() throws IOException {
        pipeBodyToStreamAndAccept(32768, responseSpec(), newFixedThreadPool(10), s -> {
            final byte[] b = new byte[1048576];
            try {
                for (int r; (r = s.read(b)) != -1; ) {
                    log.debug("r: {}", r);
                }
            } catch (final IOException ioe) {
                throw new RuntimeException(ioe);
            }
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    public void testPipeBodyToChannelAndAccept() throws IOException {
        pipeBodyToChannelAndAccept(responseSpec(), newFixedThreadPool(10), c -> {
            try {
                for (final ByteBuffer b = allocate(1048576); c.read(b) != -1; b.clear()) {
                    log.debug("b.position: {}", b.position());
                }
            } catch (final IOException ioe) {
            }
        });
    }
}

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

import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.CE;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.CR;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.FR;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToFileAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.writeBodyToTempFileAndApply;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     * @param file     a temporary file to which the body of the response is written.
     */
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToFileAndApply(final WebClient.ResponseSpec response, final long expected,
                                     @TempFileParameterResolver.TempFile final Path file) {
        final Long actual = writeBodyToFileAndApply(response, file, FR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testWriteBodyToFileAndAccept(final WebClient.ResponseSpec response, final long expected,
                                      @TempFileParameterResolver.TempFile final Path file) {
        writeBodyToFileAndAccept(response, file, (f, u) -> assertEquals(expected, FR.apply(f, u)), () -> null).block();
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
        final Long actual = writeBodyToTempFileAndApply(responseSpec, CR, () -> null).block();
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
        writeBodyToTempFileAndAccept(responseSpec, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null)
                .block();
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithExecutor(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = pipeBodyAndApply(responseSpec, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithExecutorEscape(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = pipeBodyAndApply(responseSpec, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithExecutor(final WebClient.ResponseSpec responseSpec, final long expected) {
        pipeBodyAndAccept(responseSpec, newSingleThreadExecutor(), (c, u) -> assertEquals(expected, CR.apply(c, u)),
                          () -> null)
                .block();
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithExecutorEscape(final WebClient.ResponseSpec responseSpec, final long expected) {
        pipeBodyAndAccept(responseSpec, newSingleThreadExecutor(), (c, u) -> assertTrue(CE.apply(c, u) <= expected),
                          () -> null)
                .block();
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithCompletableFuture(final WebClient.ResponseSpec responseSpec, final long expected) {
        final Long actual = pipeBodyAndApply(responseSpec, CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithCompletableFutureEscape(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = pipeBodyAndApply(response, CE, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithCompletableFuture(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null).block();
    }

    @MethodSource({"sourceResponseSpec"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithCompletableFutureEscape(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, (c, u) -> assertTrue(CE.apply(c, u) <= expected), () -> null).block();
    }
}

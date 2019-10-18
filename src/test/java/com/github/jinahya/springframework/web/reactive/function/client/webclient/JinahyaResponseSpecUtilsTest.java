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
import com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.CE2;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.CR;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.FS2;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.SR2;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.pipeBodyAndApply;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.reduceBodyAsStreamAndAccept;
import static com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils.reduceBodyAsStreamAndApply;
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
    private static Stream<Arguments> sourceResponseSpecWithExpected() {
        final int expected = current().nextInt(8192);
        final Flux<DataBuffer> source = JinahyaDataBufferUtilsTest.dataBuffers(expected);
        final WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);
        Mockito.when(responseSpec.bodyToFlux(DataBuffer.class)).thenReturn(source);
        return Stream.of(Arguments.of(responseSpec, expected));
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
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testWriteBodyToFileAndApply(final WebClient.ResponseSpec response, final long expected,
                                     @TempFileParameterResolver.TempFile final Path file) {
        final Long actual = writeBodyToFileAndApply(response, file, FS2, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToFileAndAccept(WebClient.ResponseSpec, Path, BiConsumer,
     * Supplier)} method.
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     * @param file     a temporary file to which the body of the response is written.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testWriteBodyToFileAndAccept(final WebClient.ResponseSpec response, final long expected,
                                      @TempFileParameterResolver.TempFile final Path file) {
        writeBodyToFileAndAccept(response, file, (f, u) -> assertEquals(expected, FS2.apply(f, u)), () -> null).block();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndApply(WebClient.ResponseSpec, BiFunction, Supplier)}
     * method.
     *
     * @param response a response spec to test with.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testWriteBodyToTempFileAndApply(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = writeBodyToTempFileAndApply(response, CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)}
     * method.
     *
     * @param response a response spec to test with.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testWriteBodyToTempFileAndAccept(final WebClient.ResponseSpec response, final long expected) {
        writeBodyToTempFileAndAccept(response, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null).block();
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithExecutor(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = pipeBodyAndApply(response, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithExecutorEscape(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = pipeBodyAndApply(response, newSingleThreadExecutor(), CR, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithExecutor(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, newSingleThreadExecutor(), (c, u) -> assertEquals(expected, CR.apply(c, u)),
                          () -> null)
                .block();
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithExecutorEscape(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, newSingleThreadExecutor(), (c, u) -> assertTrue(CE2.apply(c, u) <= expected),
                          () -> null)
                .block();
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithCompletableFuture(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = pipeBodyAndApply(response, CR, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndApplyWithCompletableFutureEscape(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = pipeBodyAndApply(response, CE2, () -> null).block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithCompletableFuture(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, (c, u) -> assertEquals(expected, CR.apply(c, u)), () -> null).block();
    }

    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeBodyAndAcceptWithCompletableFutureEscape(final WebClient.ResponseSpec response, final long expected) {
        pipeBodyAndAccept(response, (c, u) -> assertTrue(CE2.apply(c, u) <= expected), () -> null).block();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#reduceBodyAsStreamAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)}
     * method.
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testReduceBodyAsStreamAndApply(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = reduceBodyAsStreamAndApply(response, SR2, () -> null).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#reduceBodyAsStreamAndAccept(WebClient.ResponseSpec, BiConsumer, Supplier)}
     * method.
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testReduceBodyAsStreamAndAccept(final WebClient.ResponseSpec response, final long expected) {
        reduceBodyAsStreamAndAccept(response,
                                    (s, u) -> {
                                        final Long actual = SR2.apply(s, u);
                                        assertNotNull(actual);
                                        assertEquals(expected, actual.longValue());
                                    },
                                    () -> null)
                .block();
    }
}

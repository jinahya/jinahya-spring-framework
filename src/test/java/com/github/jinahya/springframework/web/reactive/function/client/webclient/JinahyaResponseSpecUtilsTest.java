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
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.GET_CHANNEL_SIZE;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.GET_STREAM_SIZE;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtilsTest.dataBuffers;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * A class for unit-testing {@link JinahyaResponseSpecUtils}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@ExtendWith({TempFileParameterResolver.class})
@Slf4j
class JinahyaResponseSpecUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------
    private static Stream<Arguments> sourceResponseSpecWithExpected() {
        final int expected = current().nextInt(1, 8192);
        final Flux<DataBuffer> source = dataBuffers(expected);
        final WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);
        Mockito.when(responseSpec.bodyToFlux(DataBuffer.class)).thenReturn(source);
        return Stream.of(Arguments.of(responseSpec, expected));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaResponseSpecUtils#writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)} method.
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testWriteBodyAndApply(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = JinahyaResponseSpecUtils.writeBodyToTempFileAndApply(response, GET_CHANNEL_SIZE).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#pipeBodyAndApply(WebClient.ResponseSpec, Function)} method.
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testPipeAndApply(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = JinahyaResponseSpecUtils.pipeBodyAndApply(response, GET_CHANNEL_SIZE).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaResponseSpecUtils#reduceBodyAndApply(WebClient.ResponseSpec, Function)} method
     *
     * @param response a response spec whose body is written.
     * @param expected an expected total size of bytes.
     */
    @MethodSource({"sourceResponseSpecWithExpected"})
    @ParameterizedTest
    void testReduceAndApply(final WebClient.ResponseSpec response, final long expected) {
        final Long actual = JinahyaResponseSpecUtils.reduceBodyAndApply(response, GET_STREAM_SIZE).block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }
}

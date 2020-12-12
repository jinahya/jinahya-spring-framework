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
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

import java.nio.channels.ReadableByteChannel;
import java.util.function.Function;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
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
class JinahyaDataBufferUtils_WriteAndApply_Test extends JinahyaDataBufferUtilsTest {

    @Test
    void assertWriteAndApplyThrowsNullPointerExceptionWhenSourceIsNull() {
        final Function<ReadableByteChannel, Void> function = c -> null;
        assertThrows(NullPointerException.class, () -> writeAndApply(null, function));
    }

    /**
     * Asserts {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Function)} method throws a {@code
     * NullPointerException} when {@code function} is {@code null}.
     */
    @Test
    @SuppressWarnings({"unchecked"})
    void assertWriteAndApplyThrowsNullPointerExceptionWhenFunctionIsNull() {
        final Publisher<DataBuffer> source = mock(Publisher.class);
        assertThrows(NullPointerException.class, () -> writeAndApply(source, null));
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Function)}  method.
     *
     * @param dataBuffers   a flux of data buffers.
     * @param totalCapacity the total size of data in buffers.
     */
    @MethodSource({"sourceDataBuffersWithTotalCapacity"})
    @ParameterizedTest
    void testWriteAndApply(final Flux<DataBuffer> dataBuffers, final int totalCapacity) {
        final Long actual = writeAndApply(dataBuffers, GET_CHANNEL_SIZE).block();
        assertNotNull(actual);
        assertEquals(totalCapacity, actual.longValue());
    }
}

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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * A class for unit-testing {@link JinahyaDataBufferUtils} class.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@AddBeanClasses({TempFileProducer.class})
@ExtendWith({TempFileParameterResolver.class, WeldJunit5AutoExtension.class})
@Slf4j
public abstract class JinahyaDataBufferUtilsTest {

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    public static DataBuffer dataBuffer(final int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity(" + capacity + ") <= 0");
        }
        return DATA_BUFFER_FACTORY.allocateBuffer(capacity).writePosition(capacity);
    }

    public static Flux<DataBuffer> dataBuffers(final int totalCapacity) {
        if (totalCapacity <= 0) {
            throw new IllegalArgumentException("expected(" + totalCapacity + ") <= 0");
        }
        return Flux.generate(
                () -> totalCapacity,
                (remaining, s) -> {
                    if (remaining == 0) {
                        s.complete();
                    } else {
                        final int capacity = current().nextInt(remaining + 1);
                        remaining -= capacity;
                        assert remaining >= 0;
                        final DataBuffer dataBuffer = dataBuffer(capacity);
                        s.next(dataBuffer);
                    }
                    return remaining;
                }
        );
    }

    /**
     * Sources a stream of data buffers and an expected total number of bytes of the stream.
     *
     * @return a stream of arguments.
     */
    private static Stream<Arguments> sourceDataBuffersWithTotalCapacity() {
        final int totalCapacity = current().nextInt(8192);
        final Flux<DataBuffer> buffers = dataBuffers(totalCapacity);
        return Stream.of(Arguments.of(buffers, totalCapacity));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<InputStream, Long> GET_STREAM_SIZE = s -> {
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

    // -----------------------------------------------------------------------------------------------------------------
    public static final Function<ReadableByteChannel, Long> GET_CHANNEL_SIZE = c -> {
        log.debug("channel: {}", c);
        long size = 0L;
        try {
            final ByteBuffer buffer = ByteBuffer.allocate(128);
            for (int r; (r = c.read(buffer)) != -1; buffer.clear()) {
                size += r;
            }
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return size;
    };
}

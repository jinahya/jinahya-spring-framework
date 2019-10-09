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
import com.github.jinahya.springframework.web.reactive.function.client.webclient.JinahyaResponseSpecUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.pipeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeAndApply;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndAccept;
import static com.github.jinahya.springframework.core.io.buffer.JinahyaDataBufferUtils.writeToTempFileAndApply;
import static java.nio.ByteBuffer.allocate;
import static java.nio.file.Files.size;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A class for testing {@link JinahyaResponseSpecUtils}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@ExtendWith({TempFileParameterResolver.class})
@Slf4j
class JinahyaDataBufferUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------

    private static final DataBufferFactory DATA_BUFFER_FACTORY = new DefaultDataBufferFactory();

    private static Stream<Arguments> sourceDataBuffers() {
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
        return Stream.of(Arguments.of(buffers, adder.sum()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndApply(Publisher, Path, Function)} method.
     *
     * @param buffers a stream of data buffers.
     */
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteAndApply(final Flux<DataBuffer> buffers, final long expected,
                           @TempFileParameterResolver.TempFile final Path file) {
        final Long actual = writeAndApply(buffers,
                                          file,
                                          (f, u) -> {
                                              try {
                                                  return size(f);
                                              } catch (final IOException ioe) {
                                                  throw new RuntimeException(ioe);
                                              }
                                          },
                                          () -> null)
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    /**
     * Tests {@link JinahyaDataBufferUtils#writeAndAccept(Publisher, Path, BiConsumer, Supplier)} method.
     *
     * @param buffers  a stream of data buffers.
     * @param expected a total number of bytes of data buffers.
     */
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteAndAccept(final Flux<DataBuffer> buffers, final long expected,
                            @TempFileParameterResolver.TempFile final Path file) {
        final Void v = writeAndAccept(buffers,
                                      file,
                                      (f, u) -> {
                                          try {
                                              final long actual = size(f);
                                              assertEquals(expected, actual);
                                          } catch (final IOException ioe) {
                                              Assertions.fail(ioe);
                                          }
                                      },
                                      () -> null)
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteToTempFileAndApply(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = writeToTempFileAndApply(buffers,
                                                    (c, u) -> {
                                                        assertNotNull(c);
                                                        assertNull(u);
                                                        long count = 0L;
                                                        final ByteBuffer b = allocate(1024);
                                                        try {
                                                            for (int r; (r = c.read(b)) != -1; count += r) {
                                                                b.clear();
                                                            }
                                                        } catch (final IOException ioe) {
                                                            throw new RuntimeException(ioe);
                                                        }
                                                        return count;
                                                    },
                                                    () -> null)
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testWriteToTempFileAndAccept(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = writeToTempFileAndAccept(buffers,
                                                (c, u) -> {
                                                    assertNotNull(c);
                                                    assertNull(u);
                                                    long actual = 0L;
                                                    final ByteBuffer b = allocate(1024);
                                                    try {
                                                        for (int r; (r = c.read(b)) != -1; actual += r) {
                                                            b.clear();
                                                        }
                                                    } catch (final IOException ioe) {
                                                        throw new RuntimeException(ioe);
                                                    }
                                                    assertEquals(expected, actual);
                                                },
                                                () -> null)
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApplyWithExecutor(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(buffers,
                                         newSingleThreadExecutor(),
                                         (c, u) -> {
                                             assertNotNull(c);
                                             assertNull(u);
                                             long count = 0L;
                                             final ByteBuffer b = allocate(128);
                                             try {
                                                 for (int r; (r = c.read(b)) != -1; count += r) {
                                                     b.clear();
                                                 }
                                             } catch (final IOException ioe) {
                                                 throw new RuntimeException(ioe);
                                             }
                                             return count;
                                         },
                                         () -> null)
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApplyWithExecutorEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(buffers,
                                         newSingleThreadExecutor(),
                                         (c, u) -> {
                                             assertNotNull(c);
                                             assertNull(u);
                                             long count = 0L;
                                             final ByteBuffer b = allocate(128);
                                             try {
                                                 for (int r; (r = c.read(b)) != -1; count += r) {
                                                     if (current().nextBoolean()) {
                                                         break;
                                                     }
                                                     b.clear();
                                                 }
                                             } catch (final IOException ioe) {
                                                 throw new RuntimeException(ioe);
                                             }
                                             return count;
                                         },
                                         () -> null)
                .block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAcceptWithExecutor(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(buffers,
                                     newSingleThreadExecutor(),
                                     (c, u) -> {
                                         assertNotNull(c);
                                         assertNull(u);
                                         long actual = 0L;
                                         final ByteBuffer b = allocate(128);
                                         try {
                                             for (int r; (r = c.read(b)) != -1; actual += r) {
//                                                 log.debug("r: {}, actual: {}", r, actual);
                                                 b.clear();
                                             }
                                         } catch (final IOException ioe) {
                                             throw new RuntimeException(ioe);
                                         }
                                         assertEquals(expected, actual);
                                     },
                                     () -> null)
                .block();
        assertNull(v);
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAcceptWithExecutorEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(buffers,
                                     newSingleThreadExecutor(),
                                     (c, u) -> {
                                         assertNotNull(c);
                                         assertNull(u);
                                         long actual = 0L;
                                         final ByteBuffer b = allocate(128);
                                         try {
                                             for (int r; (r = c.read(b)) != -1; actual += r) {
                                                 if (current().nextBoolean()) {
                                                     break;
                                                 }
                                                 b.clear();
                                             }
                                         } catch (final IOException ioe) {
                                             throw new RuntimeException(ioe);
                                         }
                                         assertTrue(actual <= expected);
                                     },
                                     () -> null)
                .block();
        assertNull(v);
    }

    // -----------------------------------------------------------------------------------------------------------------'
    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApplyWithoutExecutor(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(buffers,
                                         (c, u) -> {
                                             assertNotNull(c);
                                             assertNull(u);
                                             long count = 0L;
                                             final ByteBuffer b = allocate(128);
                                             try {
                                                 for (int r; (r = c.read(b)) != -1; count += r) {
                                                     b.clear();
                                                 }
                                             } catch (final IOException ioe) {
                                                 throw new RuntimeException(ioe);
                                             }
                                             return count;
                                         },
                                         () -> null)
                .block();
        assertNotNull(actual);
        assertEquals(expected, actual.longValue());
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndApplyWithoutExecutorEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Long actual = pipeAndApply(buffers,
                                         (c, u) -> {
                                             assertNotNull(c);
                                             assertNull(u);
                                             long count = 0L;
                                             final ByteBuffer b = allocate(128);
                                             try {
                                                 for (int r; (r = c.read(b)) != -1; count += r) {
                                                     log.debug("r: {}", r);
                                                     if (current().nextBoolean()) {
                                                         break;
                                                     }
                                                     b.clear();
                                                 }
                                             } catch (final IOException ioe) {
                                                 throw new RuntimeException(ioe);
                                             }
                                             return count;
                                         },
                                         () -> null)
                .block();
        assertNotNull(actual);
        assertTrue(actual <= expected);
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAcceptWithoutExecutor(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(buffers,
                                     (c, u) -> {
                                         assertNotNull(c);
                                         assertNull(u);
                                         long actual = 0L;
                                         final ByteBuffer b = allocate(128);
                                         try {
                                             for (int r; (r = c.read(b)) != -1; actual += r) {
//                                                 log.debug("r: {}, actual: {}", r, actual);
                                                 b.clear();
                                             }
                                         } catch (final IOException ioe) {
                                             throw new RuntimeException(ioe);
                                         }
                                         assertEquals(expected, actual);
                                     },
                                     () -> null)
                .block();
        assertNull(v);
    }

    @MethodSource({"sourceDataBuffers"})
    @ParameterizedTest
    void testPipeAndAcceptWithoutExecutorEscape(final Flux<DataBuffer> buffers, final long expected) {
        final Void v = pipeAndAccept(buffers,
                                     (c, u) -> {
                                         assertNotNull(c);
                                         assertNull(u);
                                         long actual = 0L;
                                         final ByteBuffer b = allocate(128);
                                         try {
                                             for (int r; (r = c.read(b)) != -1; actual += r) {
                                                 log.debug("r: {}", r);
                                                 if (current().nextBoolean()) {
                                                     break;
                                                 }
                                                 b.clear();
                                             }
                                         } catch (final IOException ioe) {
                                             throw new RuntimeException(ioe);
                                         }
                                         assertTrue(actual <= expected);
                                     },
                                     () -> null)
                .block();
        assertNull(v);
    }
}

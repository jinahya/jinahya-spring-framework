package com.github.jinahya.springframework.web.reactive.function.client.webclient;

/*-
 * #%L
 * jinahya-springframework
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

import org.slf4j.Logger;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Utilities and constants for {@link org.springframework.web.reactive.function.client.WebClient.ResponseSpec}.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
public final class JinahyaResponseSpecUtils {

    private static final Logger logger = getLogger(lookup().lookupClass());

    /**
     * Maps specified flux of data buffers to write bytes to specified stream.
     *
     * @param flux   the flux of data buffers whose bytes are written to specified stream.
     * @param stream the stream to which bytes are written
     * @param <U>    data buffer type parameter
     * @return a flux of data buffers whose bytes are written to specified stream
     * @see Flux#map(Function)
     * @see DataBuffer#readableByteCount()
     * @see DataBuffer#read(byte[])
     */
    public static <U extends DataBuffer> Flux<U> mapToWrite(final Flux<U> flux, final OutputStream stream) {
        return flux.map(b -> {
            final byte[] d = new byte[b.readableByteCount()];
            b.read(d);
            try {
                stream.write(d);
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to write", ioe);
            }
            return b;
        });
    }

    /**
     * Maps specified flux of data buffers to write bytes to specified channel.
     *
     * @param flux    the flux of data buffers whose bytes are written to specified channel
     * @param channel the channel to which bytes are written
     * @param <U>     data buffer type parameter
     * @return a flux of data buffers whose bytes are written to specified channel
     * @see Flux#map(Function)
     * @see DataBuffer#asByteBuffer()
     */
    public static <U extends DataBuffer> Flux<U> mapToWrite(final Flux<U> flux, final WritableByteChannel channel) {
        return flux.map(b -> {
            for (final ByteBuffer s = b.asByteBuffer(); s.hasRemaining(); ) {
                try {
                    final int written = channel.write(s);
                    logger.trace("written: {}", written);
                } catch (final IOException ioe) {
                    throw new RuntimeException("failed to write", ioe);
                }
            }
            return b;
        });
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes specified response spec's body to a file supplied by specified supplier and returns the result of
     * specified function applied with the file.
     *
     * @param responseSpec the response spec whose body is written
     * @param fileSupplier the supplier for the file
     * @param fileFunction the function to be supplied with the file.
     * @param <R>          result type parameter
     * @return the value the function results
     * @throws IOException if an I/O error occurs.
     * @see org.springframework.web.reactive.function.client.WebClient.ResponseSpec#bodyToFlux(Class)
     */
    public static <R> R writeBodyToFileAndApply(final WebClient.ResponseSpec responseSpec,
                                                final Supplier<? extends File> fileSupplier,
                                                final Function<? super File, ? extends R> fileFunction)
            throws IOException {
        final File file = fileSupplier.get();
        try (OutputStream stream = new FileOutputStream(file, true)) {
            mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), stream).map(DataBufferUtils::release).blockLast();
            stream.flush();
        }
        return fileFunction.apply(file);
    }

    /**
     * Writes given response spec's body to a file supplied by specified supplier and returns the result of specified
     * function applied with the file along with the second argument supplied by specified supplier.
     *
     * @param responseSpec     the response spec whose body is written to the file
     * @param fileSupplier     the supplier for the file
     * @param argumentSupplier the supplier for the second argument
     * @param fileFunction     the function to be applied
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @return the value the function results
     * @throws IOException if an I/O error occurs.
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Supplier, Supplier, BiFunction)
     */
    public static <U, R> R writeBodyToFileAndApply(final WebClient.ResponseSpec responseSpec,
                                                   final Supplier<? extends File> fileSupplier,
                                                   final Supplier<? extends U> argumentSupplier,
                                                   final BiFunction<? super File, ? super U, ? extends R> fileFunction)
            throws IOException {
        return writeBodyToFileAndApply(responseSpec, fileSupplier, f -> fileFunction.apply(f, argumentSupplier.get()));
    }

    /**
     * Writes given response spec's body to a file supplied by specified file supplier and accepts the file to specified
     * file consumer.
     *
     * @param responseSpec the response spec whose body is written to the file
     * @param fileSupplier the supplier for the file
     * @param fileConsumer the consumer accepts the file
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToFileAndApply(WebClient.ResponseSpec, Supplier, Function)
     */
    public static void writeBodyToFileAndAccept(final WebClient.ResponseSpec responseSpec,
                                                final Supplier<? extends File> fileSupplier,
                                                final Consumer<? super File> fileConsumer)
            throws IOException {
        writeBodyToFileAndApply(responseSpec, fileSupplier, f -> {
            fileConsumer.accept(f);
            return null;
        });
    }

    /**
     * Writes given response spec's body to a file supplied by specified file supplier and accepts the file to specified
     * file consumer along with a second argument supplied by specified argument supplier.
     *
     * @param responseSpec     the response spec whose body is written to the file
     * @param fileSupplier     the file supplier
     * @param argumentSupplier the second argument supplier
     * @param fileConsumer     the file consumer
     * @param <U>              second argument type parameter
     * @throws IOException if an I/O error occurs.
     */
    public static <U> void writeBodyToFileAndAccept(final WebClient.ResponseSpec responseSpec,
                                                    final Supplier<? extends File> fileSupplier,
                                                    final Supplier<? extends U> argumentSupplier,
                                                    final BiConsumer<? super File, ? super U> fileConsumer)
            throws IOException {
        writeBodyToFileAndAccept(responseSpec, fileSupplier, f -> fileConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to a temporary file and returns the result of specified function applied with
     * the file.
     *
     * @param responseSpec the response spec whose body is written to the temporary file
     * @param fileFunction the function to be applied with the temporary file
     * @param <R>          result type parameter
     * @return the result of the function
     * @throws IOException if an I/O error occurs
     */
    public static <R> R writeBodyToTempFileAndApply(final WebClient.ResponseSpec responseSpec,
                                                    final Function<? super File, ? extends R> fileFunction)
            throws IOException {
        final File file = File.createTempFile("tmp", null);
        try {
            logger.trace("temporary file: {}", file);
            return writeBodyToFileAndApply(responseSpec, () -> file, fileFunction);
        } finally {
            final boolean deleted = file.delete();
            logger.trace("deleted: {}", deleted);
            if (!deleted && file.exists()) {
                logger.error("failed to delete the temporary file: {}", file);
            }
        }
    }

    /**
     * Writes given response spec's body to a temporary file and returns the result of specified function applied with
     * the file along with an argument supplied by specified supplier.
     *
     * @param responseSpec     the response spec whose's body is written to the file
     * @param argumentSupplier the supplier for the second argument
     * @param fileFunction     the function to be applied with the file and the second argument
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @return the value the function results
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static <U, R> R writeBodyToTempFileAndApply(
            final WebClient.ResponseSpec responseSpec, final Supplier<? extends U> argumentSupplier,
            final BiFunction<? super File, ? super U, ? extends R> fileFunction)
            throws IOException {
        return writeBodyToTempFileAndApply(responseSpec, f -> fileFunction.apply(f, argumentSupplier.get()));
    }

    /**
     * Writes given response spec's body to a temporary file and accepts the file to specified consumer.
     *
     * @param responseSpec the response spec whose body is written to the temporary file
     * @param fileConsumer the consumer to be accepted with the temporary file
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToTempFileAndApply(WebClient.ResponseSpec, Function)
     */
    public static void writeBodyToTempFileAndAccept(final WebClient.ResponseSpec responseSpec,
                                                    final Consumer<? super File> fileConsumer)
            throws IOException {
        writeBodyToTempFileAndApply(responseSpec, f -> {
            fileConsumer.accept(f);
            return null;
        });
    }

    /**
     * Writes given response spec's body to a temporary file and accepts the file to specified consumer along with an
     * argument supplied by specified supplier.
     *
     * @param responseSpec     the response spec whose body is written to the temporary file
     * @param argumentSupplier a supplier for the second argument of the consumer
     * @param fileConsumer     the consumer to be accepted with the temporary file along with the second argument
     * @param <U>              second argument type parameter
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToTempFileAndAccept(WebClient.ResponseSpec, Consumer)
     */
    public static <U> void writeBodyToTempFileAndAccept(final WebClient.ResponseSpec responseSpec,
                                                        final Supplier<? extends U> argumentSupplier,
                                                        final BiConsumer<? super File, ? super U> fileConsumer)
            throws IOException {
        writeBodyToTempFileAndAccept(responseSpec, f -> fileConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Writes given response spec's body to a path supplied by specified supplier and returns the result of specified
     * function applied with the path.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathSupplier the supplier for the path
     * @param pathFunction the function to be applied with the path
     * @param <R>          result type parameter
     * @return the value the function results
     * @throws IOException if an I/O error occurs.
     * @see #mapToWrite(Flux, WritableByteChannel)
     */
    public static <R> R writeBodyToPathAndApply(final WebClient.ResponseSpec responseSpec,
                                                final Supplier<? extends Path> pathSupplier,
                                                final Function<? super Path, ? extends R> pathFunction)
            throws IOException {
        final Path path = pathSupplier.get();
        try (FileChannel channel = open(path, WRITE, APPEND)) {
            mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), channel).map(DataBufferUtils::release).blockLast();
            channel.force(false);
        }
        return pathFunction.apply(path);
    }

    /**
     * Writes given response spec's body to a path supplied by specified supplier and returns the result of specified
     * function applied with the path along with an argument supplied by specified supplier.
     *
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathSupplier     the supplier for the path
     * @param argumentSupplier the supplier for the second argument of the function
     * @param pathFunction     the function to be applied with the path and the second argument
     * @param <U>              second argument type parameter
     * @param <R>              result type parameter
     * @return the value the function results
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToPathAndApply(WebClient.ResponseSpec, Supplier, Function)
     */
    public static <U, R> R writeBodyToPathAndApply(final WebClient.ResponseSpec responseSpec,
                                                   final Supplier<? extends Path> pathSupplier,
                                                   final Supplier<? extends U> argumentSupplier,
                                                   final BiFunction<? super Path, ? super U, ? extends R> pathFunction)
            throws IOException {
        return writeBodyToPathAndApply(responseSpec, pathSupplier, f -> pathFunction.apply(f, argumentSupplier.get()));
    }

    /**
     * Writes given response spec's body to a path supplied by specified supplier and accepts the path to specified
     * consumer.
     *
     * @param responseSpec the response spec whose body is written to the path
     * @param pathSupplier the supplier for the path
     * @param pathConsumer the consumer to be accepted with the path
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToPathAndApply(WebClient.ResponseSpec, Supplier, Function)
     */
    public static void writeBodyToPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                final Supplier<? extends Path> pathSupplier,
                                                final Consumer<? super Path> pathConsumer)
            throws IOException {
        writeBodyToPathAndApply(responseSpec, pathSupplier, f -> {
            pathConsumer.accept(f);
            return null;
        });
    }

    /**
     * Writes given response spec's body to a path supplied by specified path supplier and accepts the path to specified
     * path consumer along with an argument supplied by specified argument supplier.
     *
     * @param responseSpec     the response spec whose body is written to the path
     * @param pathSupplier     the path supplier
     * @param argumentSupplier the second argument supplier
     * @param pathConsumer     the path consumer to be accepted with the path along with the second argument
     * @param <U>              second argument type parameter
     * @throws IOException if an I/O error occurs
     * @see #writeBodyToPathAndAccept(WebClient.ResponseSpec, Supplier, Consumer)
     */
    public static <U> void writeBodyToPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                    final Supplier<? extends Path> pathSupplier,
                                                    final Supplier<? extends U> argumentSupplier,
                                                    final BiConsumer<? super Path, ? super U> pathConsumer)
            throws IOException {
        writeBodyToPathAndAccept(responseSpec, pathSupplier, f -> pathConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R writeBodyToTempPathAndApply(final WebClient.ResponseSpec responseSpec,
                                                    final Function<? super Path, ? extends R> pathFunction)
            throws IOException {
        final Path path = Files.createTempFile(null, null);
        try {
            return writeBodyToPathAndApply(responseSpec, () -> path, pathFunction);
        } finally {
            final boolean deleted = Files.deleteIfExists(path);
            if (!deleted && Files.exists(path)) {
                logger.warn("failed to delete the temp path: {}", path);
            }
        }
    }

    public static <U, R> R writeBodyToTempPathAndApply(
            final WebClient.ResponseSpec responseSpec, final Supplier<? extends U> argumentSupplier,
            final BiFunction<? super Path, ? super U, ? extends R> pathFunction)
            throws IOException {
        return writeBodyToTempPathAndApply(responseSpec, f -> pathFunction.apply(f, argumentSupplier.get()));
    }

    public static void writeBodyToTempPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                    final Consumer<? super Path> pathConsumer)
            throws IOException {
        writeBodyToTempPathAndApply(responseSpec, p -> {
            pathConsumer.accept(p);
            return null;
        });
    }

    public static <U> void writeBodyToTempPathAndAccept(final WebClient.ResponseSpec responseSpec,
                                                        final Supplier<? extends U> argumentSupplier,
                                                        final BiConsumer<? super Path, ? super U> pathConsumer)
            throws IOException {
        writeBodyToTempPathAndAccept(responseSpec, f -> pathConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R pipeBodyToStreamAndApply(final int pipeSize, final WebClient.ResponseSpec responseSpec,
                                                 final Executor taskExecutor,
                                                 final Function<? super InputStream, ? extends R> streamFunction)
            throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        try (PipedInputStream input = new PipedInputStream(output, pipeSize)) {
            final Flux<DataBuffer> flux = mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), output);
            taskExecutor.execute(() -> {
                flux.map(DataBufferUtils::release).blockLast();
                logger.trace("blocked: {}", flux);
                try {
                    output.flush();
                    logger.trace("flushed: {}", output);
                    output.close();
                    logger.trace("closed: {}", output);
                } catch (final IOException ioe) {
                    throw new RuntimeException("failed to flush and close the piped output stream", ioe);
                }
            });
            return streamFunction.apply(input);
        }
    }

    public static <U, R> R pipeBodyToStreamAndApply(
            final int pipeSize, final WebClient.ResponseSpec responseSpec,
            final Executor taskExecutor, final Supplier<? extends U> argumentSupplier,
            final BiFunction<? super InputStream, ? super U, ? extends R> streamFunction)
            throws IOException {
        return pipeBodyToStreamAndApply(pipeSize, responseSpec, taskExecutor,
                                        s -> streamFunction.apply(s, argumentSupplier.get()));
    }

    public static void pipeBodyToStreamAndAccept(final int pipeSize, final WebClient.ResponseSpec responseSpec,
                                                 final Executor taskExecutor,
                                                 final Consumer<? super InputStream> streamConsumer)
            throws IOException {
        pipeBodyToStreamAndApply(pipeSize, responseSpec, taskExecutor, s -> {
            streamConsumer.accept(s);
            return null;
        });
    }

    public static <U> void pipeBodyToStreamAndAccept(final int pipeSize, final WebClient.ResponseSpec responseSpec,
                                                     final Executor taskExecutor,
                                                     final Supplier<? extends U> argumentSupplier,
                                                     final BiConsumer<? super InputStream, ? super U> streamConsumer)
            throws IOException {
        pipeBodyToStreamAndAccept(pipeSize, responseSpec, taskExecutor,
                                  s -> streamConsumer.accept(s, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R pipeBodyToChannelAndApply(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final Function<? super ReadableByteChannel, ? extends R> channelFunction)
            throws IOException {
        final Pipe pipe = Pipe.open();
        final Flux<DataBuffer> flux = mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), pipe.sink());
        taskExecutor.execute(() -> {
            flux.map(DataBufferUtils::release).blockLast();
            logger.trace("blocked: {}", flux);
            try {
                pipe.sink().close();
                logger.trace("closed: {}", pipe.sink());
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to close the pipe.sink", ioe);
            }
        });
        return channelFunction.apply(pipe.source());
    }

    public static <U, R> R pipeBodyToChannelAndApply(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final Supplier<? extends U> argumentSupplier,
            final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> channelFunction)
            throws IOException {
        return pipeBodyToChannelAndApply(responseSpec, taskExecutor,
                                         c -> channelFunction.apply(c, argumentSupplier.get()));
    }

    /**
     * Pipes the body of given response spec to a channel and accepts specified consumer with the channel.
     *
     * @param responseSpec    the response spec whose body is piped
     * @param taskExecutor    an executor for blocking the flux
     * @param channelConsumer a consumer accepts the piped channel
     * @throws IOException if an I/O error occurs
     */
    public static void pipeBodyToChannelAndAccept(final WebClient.ResponseSpec responseSpec,
                                                  final Executor taskExecutor,
                                                  final Consumer<? super ReadableByteChannel> channelConsumer)
            throws IOException {
        pipeBodyToChannelAndApply(responseSpec, taskExecutor, c -> {
            channelConsumer.accept(c);
            return null;
        });
    }

    /**
     * Pipes the body of given response spec to a channel and accepts specified consumer with the channel along with the
     * value from specified supplier.
     *
     * @param responseSpec     the response spec whose body is piped
     * @param taskExecutor     an executor for a task of blocking the flux
     * @param argumentSupplier a supplier for the second argument of the consumer
     * @param channelConsumer  the consumer accepts the channel along with the value from {@code argumentSupplier}
     * @param <U>              second argument type parameter
     * @throws IOException if an I/O error occurs
     * @see #pipeBodyToChannelAndAccept(WebClient.ResponseSpec, Executor, Consumer)
     */
    public static <U> void pipeBodyToChannelAndAccept(
            final WebClient.ResponseSpec responseSpec, final Executor taskExecutor,
            final Supplier<? extends U> argumentSupplier,
            final BiConsumer<? super ReadableByteChannel, ? super U> channelConsumer)
            throws IOException {
        pipeBodyToChannelAndAccept(responseSpec, taskExecutor, c -> channelConsumer.accept(c, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a new instance.
     */
    private JinahyaResponseSpecUtils() {
        super();
    }
}

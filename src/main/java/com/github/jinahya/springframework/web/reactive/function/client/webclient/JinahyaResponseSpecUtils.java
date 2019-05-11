package com.github.jinahya.springframework.web.reactive.function.client.webclient;

import org.slf4j.Logger;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.lang.NonNull;
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
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.invoke.MethodHandles.lookup;
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
     */
    public static <U extends DataBuffer> Flux<U> mapToWrite(@NonNull final Flux<U> flux,
                                                            @NonNull final OutputStream stream) {
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
     */
    public static <U extends DataBuffer> Flux<U> mapToWrite(@NonNull final Flux<U> flux,
                                                            @NonNull final WritableByteChannel channel) {
        return flux.map(b -> {
            for (final ByteBuffer s = b.asByteBuffer(); s.hasRemaining(); ) {
                try {
                    final int written = channel.write(s);
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
    public static <R> R writeBodyToFileAndApply(@NonNull final WebClient.ResponseSpec responseSpec,
                                                @NonNull final Supplier<? extends File> fileSupplier,
                                                @NonNull final Function<? super File, ? extends R> fileFunction)
            throws IOException {
        final File file = fileSupplier.get();
        try (OutputStream stream = new FileOutputStream(file, true)) {
            mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), stream).map(DataBufferUtils::release).blockLast();
            stream.flush();
        }
        return fileFunction.apply(file);
    }

    public static <U, R> R writeBodyToFileAndApply(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Supplier<? extends File> fileSupplier,
            @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super File, ? super U, ? extends R> fileFunction)
            throws IOException {
        return writeBodyToFileAndApply(responseSpec, fileSupplier, f -> fileFunction.apply(f, argumentSupplier.get()));
    }

    public static void writeBodyToFileAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                @NonNull final Supplier<? extends File> fileSupplier,
                                                @NonNull final Consumer<? super File> fileConsumer)
            throws IOException {
        writeBodyToFileAndApply(responseSpec, fileSupplier, f -> {
            fileConsumer.accept(f);
            return null;
        });
    }

    public static <U> void writeBodyToFileAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Supplier<? extends File> fileSupplier,
                                                    @NonNull final Supplier<? extends U> argumentSupplier,
                                                    @NonNull final BiConsumer<? super File, ? super U> fileConsumer)
            throws IOException {
        writeBodyToFileAndAccept(responseSpec, fileSupplier, f -> fileConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R writeBodyToTempFileAndApply(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Function<? super File, ? extends R> fileFunction)
            throws IOException {
        final File file = File.createTempFile("tmp", null);
        try {
            return writeBodyToFileAndApply(responseSpec, () -> file, fileFunction);
        } finally {
            final boolean deleted = file.delete();
            if (!deleted && file.exists()) {
                logger.error("failed to delete the temp file: {}", file);
            }
        }
    }

    public static <U, R> R writeBodyToTempFileAndApply(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super File, ? super U, ? extends R> fileFunction)
            throws IOException {
        return writeBodyToTempFileAndApply(responseSpec, f -> fileFunction.apply(f, argumentSupplier.get()));
    }

    public static void writeBodyToTempFileAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Consumer<? super File> fileConsumer)
            throws IOException {
        writeBodyToTempFileAndApply(responseSpec, f -> {
            fileConsumer.accept(f);
            return null;
        });
    }

    public static <U> void writeBodyToTempFileAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                        @NonNull final Supplier<? extends U> argumentSupplier,
                                                        @NonNull final BiConsumer<? super File, ? super U> fileConsumer)
            throws IOException {
        writeBodyToTempFileAndAccept(responseSpec, f -> fileConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------

    public static <R> R writeBodyToPathAndApply(@NonNull final WebClient.ResponseSpec responseSpec,
                                                @NonNull final Supplier<? extends Path> pathSupplier,
                                                @NonNull final Function<? super Path, ? extends R> pathFunction)
            throws IOException {
        final Path path = pathSupplier.get();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), channel).map(DataBufferUtils::release).blockLast();
            channel.force(false);
        }
        return pathFunction.apply(path);
    }

    public static <U, R> R writeBodyToPathAndApply(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Supplier<? extends Path> pathSupplier,
            @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super Path, ? super U, ? extends R> pathFunction)
            throws IOException {
        return writeBodyToPathAndApply(responseSpec, pathSupplier, f -> pathFunction.apply(f, argumentSupplier.get()));
    }

    public static void writeBodyToPathAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                @NonNull final Supplier<? extends Path> pathSupplier,
                                                @NonNull final Consumer<? super Path> pathConsumer)
            throws IOException {
        writeBodyToPathAndApply(responseSpec, pathSupplier, f -> {
            pathConsumer.accept(f);
            return null;
        });
    }

    public static <U> void writeBodyToPathAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Supplier<? extends Path> pathSupplier,
                                                    @NonNull final Supplier<? extends U> argumentSupplier,
                                                    @NonNull final BiConsumer<? super Path, ? super U> pathConsumer)
            throws IOException {
        writeBodyToPathAndAccept(responseSpec, pathSupplier, f -> pathConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R writeBodyToTempPathAndApply(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Function<? super Path, ? extends R> pathFunction)
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
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super Path, ? super U, ? extends R> pathFunction)
            throws IOException {
        return writeBodyToTempPathAndApply(responseSpec, f -> pathFunction.apply(f, argumentSupplier.get()));
    }

    public static void writeBodyToTempPathAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                    @NonNull final Consumer<? super Path> pathConsumer)
            throws IOException {
        writeBodyToTempPathAndApply(responseSpec, p -> {
            pathConsumer.accept(p);
            return null;
        });
    }

    public static <U> void writeBodyToTempPathAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                        @NonNull final Supplier<? extends U> argumentSupplier,
                                                        @NonNull final BiConsumer<? super Path, ? super U> pathConsumer)
            throws IOException {
        writeBodyToTempPathAndAccept(responseSpec, f -> pathConsumer.accept(f, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R pipeBodyToStreamAndApply(
            final int pipeSize, @NonNull final WebClient.ResponseSpec responseSpec,
            @NonNull final Executor taskExecutor,
            @NonNull final Function<? super InputStream, ? extends R> streamFunction)
            throws IOException {
        final PipedOutputStream output = new PipedOutputStream();
        final PipedInputStream input = new PipedInputStream(output, pipeSize);
        final Flux<DataBuffer> flux = mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), output);
        taskExecutor.execute(() -> {
            final Boolean released = flux.map(DataBufferUtils::release).blockLast();
            logger.debug("blocked: {}, {}", flux, released);
            try {
                output.flush();
                logger.debug("flushed: {}", output);
                output.close();
                logger.debug("closed: {}", output);
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to flush and close the piped output stream", ioe);
            }
        });
        return streamFunction.apply(input);
    }

    public static <U, R> R pipeBodyToStreamAndApply(
            final int pipeSize, @NonNull final WebClient.ResponseSpec responseSpec,
            @NonNull final Executor taskExecutor, @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super InputStream, ? super U, ? extends R> streamFunction)
            throws IOException {
        return pipeBodyToStreamAndApply(pipeSize, responseSpec, taskExecutor,
                                        s -> streamFunction.apply(s, argumentSupplier.get()));
    }

    public static void pipeBodyToStreamAndAccept(final int pipeSize, @NonNull final WebClient.ResponseSpec responseSpec,
                                                 @NonNull final Executor taskExecutor,
                                                 @NonNull final Consumer<? super InputStream> streamConsumer)
            throws IOException {
        pipeBodyToStreamAndApply(pipeSize, responseSpec, taskExecutor, s -> {
            streamConsumer.accept(s);
            return null;
        });
    }

    public static <U> void pipeBodyToStreamAndAccept(
            final int pipeSize, @NonNull final WebClient.ResponseSpec responseSpec,
            @NonNull final Executor taskExecutor, @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiConsumer<? super InputStream, ? super U> streamConsumer)
            throws IOException {
        pipeBodyToStreamAndAccept(pipeSize, responseSpec, taskExecutor,
                                  s -> streamConsumer.accept(s, argumentSupplier.get()));
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R pipeBodyToChannelAndApply(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Executor taskExecutor,
            @NonNull final Function<? super ReadableByteChannel, ? extends R> channelFunction)
            throws IOException {
        final Pipe pipe = Pipe.open();
        final Flux<DataBuffer> flux = mapToWrite(responseSpec.bodyToFlux(DataBuffer.class), pipe.sink());
        taskExecutor.execute(() -> {
            final Boolean released = flux.map(DataBufferUtils::release).blockLast();
            logger.debug("blocked: {} {}", flux, released);
            try {
                pipe.sink().close();
                logger.debug("closed: {}", pipe.sink());
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to close the pipe.sink", ioe);
            }
        });
        return channelFunction.apply(pipe.source());
    }

    public static <U, R> R pipeBodyToChannelAndApply(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Executor taskExecutor,
            @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiFunction<? super ReadableByteChannel, ? super U, ? extends R> channelFunction)
            throws IOException {
        return pipeBodyToChannelAndApply(responseSpec, taskExecutor,
                                         c -> channelFunction.apply(c, argumentSupplier.get()));
    }

    /**
     * Pipes the body of given response spec to a channel and accepts specified consumer with the channel.
     *
     * @param responseSpec    the response spec whose body is piped
     * @param taskExecutor    an executor for clocking the flux
     * @param channelConsumer a consumer accepts the piped channel
     * @throws IOException if an I/O error occurs
     */
    public static void pipeBodyToChannelAndAccept(@NonNull final WebClient.ResponseSpec responseSpec,
                                                  @NonNull final Executor taskExecutor,
                                                  @NonNull final Consumer<? super ReadableByteChannel> channelConsumer)
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
     * @param taskExecutor     an executor for blocking the flux.
     * @param argumentSupplier a supplier for the second argument of the consumer.
     * @param channelConsumer  the consumer accepts the channel along with the value from {@code argumentSupplier}
     * @param <U>              second argument type parameter.
     * @throws IOException if an I/O error occurs.
     * @see #pipeBodyToChannelAndAccept(WebClient.ResponseSpec, Executor, Consumer)
     */
    public static <U> void pipeBodyToChannelAndAccept(
            @NonNull final WebClient.ResponseSpec responseSpec, @NonNull final Executor taskExecutor,
            @NonNull final Supplier<? extends U> argumentSupplier,
            @NonNull final BiConsumer<? super ReadableByteChannel, ? super U> channelConsumer)
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

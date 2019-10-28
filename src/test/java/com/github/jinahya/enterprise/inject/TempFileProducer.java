package com.github.jinahya.enterprise.inject;

import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Qualifier;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;

public class TempFileProducer {

    // -----------------------------------------------------------------------------------------------------------------
    @Qualifier
    @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface TempFile {

    }

    // -----------------------------------------------------------------------------------------------------------------
    @TempFile
    @Produces
    public Path produceTempFile(final InjectionPoint injectionPoint) throws IOException {
        return createTempFile(null, null);
    }

    public void disposeTempFile(@TempFile @Disposes final Path path) throws IOException {
        final boolean deleted = deleteIfExists(path);
    }
}

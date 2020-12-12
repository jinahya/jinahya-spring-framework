package com.github.jinahya.enterprise.inject;

/*-
 * #%L
 * jinahya-spring-framework
 * %%
 * Copyright (C) 2019 - 2020 Jinahya, Inc.
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

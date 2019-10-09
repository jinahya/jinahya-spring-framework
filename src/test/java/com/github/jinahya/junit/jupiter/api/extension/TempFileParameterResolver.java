package com.github.jinahya.junit.jupiter.api.extension;

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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class TempFileParameterResolver implements ParameterResolver {

    // -----------------------------------------------------------------------------------------------------------------
    @Target({ElementType.FIELD, ElementType.PARAMETER})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface TempFile {

    }

    // -----------------------------------------------------------------------------------------------------------------
    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Parameter parameter = parameterContext.getParameter();
        return parameter.isAnnotationPresent(TempFile.class)
               && (parameter.getType() == Path.class || parameter.getType() == File.class);
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Path path;
        try {
            path = Files.createTempFile(null, null);
        } catch (final IOException ioe) {
            throw new RuntimeException("failed to create a temp file", ioe);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = Files.deleteIfExists(path);
                log.trace("deleted: {}", path);
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to delete temp file; " + path, ioe);
            }
        }));
        if (parameterContext.getParameter().getType() == File.class) {
            return path.toFile();
        }
        return path;
    }
}

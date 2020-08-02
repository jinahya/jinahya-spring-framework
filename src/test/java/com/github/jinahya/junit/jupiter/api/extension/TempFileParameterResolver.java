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

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Parameter;
import java.nio.file.Path;

import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;

/**
 * A parameter resolver for a temporarily created file which is going to be deleted at the end of the runtime.
 *
 * @author Jin Kwon &lt;onacit_at_gmail.com&gt;
 */
@Slf4j
public class TempFileParameterResolver implements ParameterResolver {

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A marker annotation for temporary file parameters.
     */
    @Target({ElementType.PARAMETER})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    public @interface TempFile {

    }

    // -----------------------------------------------------------------------------------------------------------------
    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Parameter parameter = parameterContext.getParameter();
        return parameter.isAnnotationPresent(TempFile.class) && parameter.getType() == Path.class;
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        final Path argument;
        try {
            argument = createTempFile(null, null);
        } catch (final IOException ioe) {
            throw new RuntimeException("failed to create a temp file", ioe);
        }
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                final boolean deleted = deleteIfExists(argument);
                log.trace("deleted: {} {}", deleted, argument);
            } catch (final IOException ioe) {
                throw new RuntimeException("failed to delete file; " + argument, ioe);
            }
        }));
        return argument;
    }
}

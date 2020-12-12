package com.github.jinahya.springframework.util;

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

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class JinahyaMethodHandleUtilsTest {

    private static class PrivateConstructor {

        private PrivateConstructor() {
            super();
        }

        private PrivateConstructor(final String name) {
            super();
        }
    }

    /**
     * Tests {@link JinahyaMethodHandleUtils#accessibleConstructorHandle(Class, Class[])} method.
     *
     * @throws Throwable as thrown.
     */
    @Test
    void testAccessibleConstructorHandle() throws Throwable {
        {
            final MethodHandle handle = JinahyaMethodHandleUtils.accessibleConstructorHandle(PrivateConstructor.class);
            for (int i = 0; i < 10; i++) {
                final PrivateConstructor instance = (PrivateConstructor) handle.invoke();
                assertThat(instance).isNotNull();
            }
        }
        {
            final MethodHandle handle
                    = JinahyaMethodHandleUtils.accessibleConstructorHandle(PrivateConstructor.class, String.class);
            for (int i = 0; i < 10; i++) {
                final PrivateConstructor instance = (PrivateConstructor) handle.invokeExact((String) null);
                assertThat(instance).isNotNull();
            }
        }
    }

    /**
     * Tests {@link JinahyaMethodHandleUtils#accessibleDefaultConstructorHandleInvoker(Class)} method.
     */
    @Test
    void testAccessibleDefaultConstructorHandleInvoker() {
        final Supplier<PrivateConstructor> supplier
                = JinahyaMethodHandleUtils.accessibleDefaultConstructorHandleInvoker(PrivateConstructor.class);
        for (int i = 0; i < 10; i++) {
            final PrivateConstructor instance = supplier.get();
            assertThat(instance).isNotNull();
        }
    }
}

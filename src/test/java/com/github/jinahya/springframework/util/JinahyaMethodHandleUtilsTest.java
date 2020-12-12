package com.github.jinahya.springframework.util;

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
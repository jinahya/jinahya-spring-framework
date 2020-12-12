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

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.function.Supplier;

@Slf4j
public final class JinahyaMethodHandleUtils {

    /**
     * Returns an {@link MethodHandles.Lookup#unreflectConstructor(Constructor) unreflected} {@link
     * ReflectionUtils#accessibleConstructor(Class, Class[]) (accessible) constructor} of specified class for specified
     * parameter types.
     *
     * @param clazz          the class whose constructor is obtained.
     * @param parameterTypes the parameter types of the desired constructor.
     * @return the unreflected (accessible) constructor.
     * @throws NoSuchMethodException  if failed to find the constructor.
     * @throws IllegalAccessException if access checking fails or if the method's variable arity modifier bit is set and
     *                                asVarargsCollector fails
     * @see ReflectionUtils#accessibleConstructor(Class, Class[])
     * @see java.lang.invoke.MethodHandles.Lookup#unreflectConstructor(Constructor)
     */
    public static MethodHandle accessibleConstructorHandle(final Class<?> clazz, final Class<?>... parameterTypes)
            throws NoSuchMethodException, IllegalAccessException {
        final Constructor<?> constructor = ReflectionUtils.accessibleConstructor(clazz, parameterTypes);
        return MethodHandles.lookup().unreflectConstructor(constructor);
    }

    public static <T> Supplier<T> accessibleDefaultConstructorHandleInvoker(final Class<T> clazz) {
        final MethodHandle[] handle = new MethodHandle[1];
        return () -> {
            MethodHandle h = handle[0];
            if (h == null) {
                try {
                    handle[0] = h = accessibleConstructorHandle(clazz);
                } catch (final ReflectiveOperationException roe) {
                    throw new RuntimeException("failed to get default constructor handle", roe);
                }
            }
            try {
                @SuppressWarnings({"unchecked"})
                final T invoke = (T) h.invoke();
                return invoke;
            } catch (final Throwable t) {
                throw new RuntimeException("failed to invoke default constructor handle", t);
            }
        };
    }

    protected JinahyaMethodHandleUtils() {
        throw new AssertionError("instantiation is not allowed");
    }
}

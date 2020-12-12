package com.github.jinahya.springframework.util;

import org.springframework.util.ReflectionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.function.Supplier;

public abstract class JinahyaMethodHandleUtils {

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
        return new Supplier<T>() {
            @SuppressWarnings({"unchecked"})
            @Override
            public T get() {
                MethodHandle h = handle;
                if (h == null) {
                    try {
                        handle = h = accessibleConstructorHandle(clazz);
                    } catch (final ReflectiveOperationException roe) {
                        throw new RuntimeException("failed to get default constructor handle", roe);
                    }
                }
                try {
                    return (T) h.invoke();
                } catch (final Throwable t) {
                    throw new RuntimeException("failed to invoke default constructor handle", t);
                }
            }

            private MethodHandle handle;
        };
    }

    protected JinahyaMethodHandleUtils() {
        super();
    }
}

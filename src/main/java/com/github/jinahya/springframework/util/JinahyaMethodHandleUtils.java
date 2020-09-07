package com.github.jinahya.springframework.util;

import jdk.internal.jline.internal.Nullable;
import org.springframework.lang.NonNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.function.UnaryOperator;

import static java.util.Collections.synchronizedMap;
import static java.util.Optional.ofNullable;
import static org.springframework.util.Assert.notNull;
import static org.springframework.util.ReflectionUtils.findField;

public abstract class JinahyaMethodHandleUtils {

    /**
     * A map of classes and maps of field names and unreflected getters.
     */
    private static final Map<Class<?>, Map<String, MethodHandle>> UNREFLECTED_GETTERS_BY_NAMES
            = synchronizedMap(new WeakHashMap<>());

    /**
     * A map of classes and maps of field names and unreflected setters.
     */
    private static final Map<Class<?>, Map<String, MethodHandle>> UNREFLECTED_SETTERS_BY_NAMES
            = synchronizedMap(new WeakHashMap<>());

    /**
     * Returns an unreflected getter of the field, searched in specified class and its superclasses, whose name matches
     * to specified name.
     *
     * @param clazz    the class whose fields are searched.
     * @param name     the name of the field.
     * @param operator an operator for handling access control.
     * @param lookup   a lookup whose {@link java.lang.invoke.MethodHandles.Lookup#unreflectGetter(Field)} method is
     *                 invoked with the field.
     * @return an optional of the unreflected getter; {@code empty} when the field is not found.
     */
    public static Optional<MethodHandle> getUnreflectedGetter(
            @NonNull final Class<?> clazz, @NonNull final String name,
            @Nullable final UnaryOperator<Field> operator, @NonNull final MethodHandles.Lookup lookup) {
        notNull(lookup, "lookup must be not null");
        final Map<String, MethodHandle> gettersByNames
                = UNREFLECTED_GETTERS_BY_NAMES.computeIfAbsent(clazz, c -> synchronizedMap(new WeakHashMap<>()));
        return ofNullable(
                gettersByNames.computeIfAbsent(
                        name,
                        n -> ofNullable(findField(clazz, name))
                                .map(f -> operator != null ? operator.apply(f) : f)
                                .map(f -> {
                                    try {
                                        return lookup.unreflectGetter(f);
                                    } catch (final IllegalAccessException iae) {
                                        throw new RuntimeException("unable to unreflect getter from " + f, iae);
                                    }
                                })
                                .orElse(null)

                )
        );
    }

    public static Optional<MethodHandle> getUnreflectedSetter(
            @NonNull final Class<?> clazz, @NonNull final String name,
            @Nullable final UnaryOperator<Field> operator, @NonNull final MethodHandles.Lookup lookup) {
        notNull(lookup, "lookup must be not null");
        final Map<String, MethodHandle> settersByNames
                = UNREFLECTED_SETTERS_BY_NAMES.computeIfAbsent(clazz, c -> synchronizedMap(new WeakHashMap<>()));
        return ofNullable(
                settersByNames.computeIfAbsent(
                        name,
                        n -> ofNullable(findField(clazz, name))
                                .map(f -> operator != null ? operator.apply(f) : f)
                                .map(f -> {
                                    try {
                                        return lookup.unreflectSetter(f);
                                    } catch (final IllegalAccessException iae) {
                                        throw new RuntimeException("unable to unreflect setter from " + f, iae);
                                    }
                                })
                                .orElse(null)

                )
        );
    }

    protected JinahyaMethodHandleUtils() {
        super();
    }
}

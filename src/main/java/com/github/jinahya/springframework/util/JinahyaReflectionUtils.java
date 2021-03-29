package com.github.jinahya.springframework.util;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;
import static org.springframework.util.ReflectionUtils.makeAccessible;
import static org.springframework.util.ReflectionUtils.setField;

public abstract class JinahyaReflectionUtils {

    /**
     * Attempt to find a {@link java.lang.reflect.Field field} on the supplied {@link java.lang.Class Class} with the
     * supplied name and/or {@link java.lang.Class type} and gets the field on the specified {@link java.lang.Object
     * target object}.
     *
     * @param clazz  the class to introspect.
     * @param name   the name of the field (may be null if type is specified).
     * @param type   the type of the field (may be null if name is specified).
     * @param target the target object on which to set the field (or {@code null} for a static field).
     * @param <T>    field type parameter
     * @return the field's current value.
     * @see ReflectionUtils#findField(Class, String, Class)
     * @see ReflectionUtils#getField(Field, Object)
     */
    @Nullable
    @SuppressWarnings({"unchecked"})
    public static <T> T findAndGetField(@NonNull final Class<?> clazz, @Nullable final String name,
                                        @Nullable final Class<T> type, @Nullable final Object target) {
        final Field field = findField(clazz, name, type);
        if (field == null) {
            throw new RuntimeException("no field found for " + clazz + ", " + name + ", " + type);
        } else if (!field.isAccessible()) {
            makeAccessible(field);
        }
        return (T) getField(field, target);
    }

    /**
     * Attempt to find a {@link java.lang.reflect.Field field} on the supplied {@link java.lang.Class Class} with the
     * supplied name and/or {@link java.lang.Class type} and set the field on the specified {@link java.lang.Object
     * target object} to the specified {@code value}.
     *
     * @param clazz  the class to introspect.
     * @param name   the name of the field (may be null if type is specified).
     * @param type   the type of the field (may be null if name is specified).
     * @param target the target object on which to set the field (or {@code null} for a static field).
     * @param value  the value to set (may be {@code null}).
     * @param <T>    field type parameter
     * @see ReflectionUtils#findField(Class, String, Class)
     * @see ReflectionUtils#setField(Field, Object, Object)
     */
    public static <T> void findAndSetField(@NonNull final Class<?> clazz, @Nullable final String name,
                                           @Nullable final Class<T> type, @Nullable final Object target,
                                           @Nullable final T value) {
        final Field field = findField(clazz, name, type);
        if (field == null) {
            throw new RuntimeException("no field found for " + clazz + ", " + name + ", " + type);
        } else if (!field.isAccessible()) {
            makeAccessible(field);
        }
        setField(field, target, value);
    }
}

package com.github.jinahya.springframework.util;

import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class JinahyaReflectionUtils {

    public static void doWithFields(final Class<?> clazz, final Consumer<? super Field> consumer) {
        ReflectionUtils.doWithFields(clazz, consumer::accept);
    }

    public static void doWithFields(final Class<?> clazz, final Consumer<? super Field> consumer,
                                    @Nullable final Predicate<? super Field> predicate) {
        if (predicate == null) {
            doWithFields(clazz, consumer);
            return;
        }
        ReflectionUtils.doWithFields(clazz, consumer::accept, predicate::test);
    }

    public static void doWithLocalFields(final Class<?> clazz, final Consumer<? super Field> consumer) {
        ReflectionUtils.doWithLocalFields(clazz, consumer::accept);
    }

    public static void doWithLocalMethods(final Class<?> clazz, final Consumer<? super Method> consumer) {
        ReflectionUtils.doWithLocalMethods(clazz, consumer::accept);
    }

    public static void doWithMethods(final Class<?> clazz, final Consumer<? super Method> consumer) {
        ReflectionUtils.doWithMethods(clazz, consumer::accept);
    }

    public static void doWithMethods(final Class<?> clazz, final Consumer<? super Method> consumer,
                                     @Nullable final Predicate<? super Method> predicate) {
        if (predicate == null) {
            doWithMethods(clazz, consumer);
            return;
        }
        ReflectionUtils.doWithMethods(clazz, consumer::accept, predicate::test);
    }

    public static Optional<Field> findField(final Class<?> clazz, final String name) {
        return Optional.ofNullable(ReflectionUtils.findField(clazz, name));
    }

    public static Optional<Field> findField(final Class<?> clazz, @Nullable final String name,
                                            @Nullable final Class<?> type) {
        return Optional.ofNullable(ReflectionUtils.findField(clazz, name, type));
    }

    public static Optional<Method> findMethod(final Class<?> clazz, final String name) {
        return Optional.ofNullable(ReflectionUtils.findMethod(clazz, name));
    }

    public static Optional<Method> findMethod(final Class<?> clazz, final String name, final Class<?> type) {
        return Optional.ofNullable(ReflectionUtils.findMethod(clazz, name, type));
    }

    public static Optional<Object> getField(final Field field, @Nullable final Object target) {
        return Optional.ofNullable(ReflectionUtils.getField(field, target));
    }
}

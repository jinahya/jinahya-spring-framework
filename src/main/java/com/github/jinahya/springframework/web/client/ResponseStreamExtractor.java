package com.github.jinahya.springframework.web.client;

import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class ResponseStreamExtractor<T> implements ResponseExtractor<T> {

    // -----------------------------------------------------------------------------------------------------------------
    public ResponseStreamExtractor(
            final BiFunction<? super ClientHttpResponse, ? super InputStream, ? extends T> streamMapper) {
        super();
        this.streamMapper = requireNonNull(streamMapper, "streamMapper is null");
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Override
    public T extractData(final ClientHttpResponse response) throws IOException {
        return streamMapper.apply(response, response.getBody());
    }

    // -----------------------------------------------------------------------------------------------------------------
    private final BiFunction<? super ClientHttpResponse, ? super InputStream, ? extends T> streamMapper;
}

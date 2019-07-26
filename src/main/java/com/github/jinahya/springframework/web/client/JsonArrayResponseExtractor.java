package com.github.jinahya.springframework.web.client;

import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;

import java.io.IOException;

public class JsonArrayResponseExtractor<T> implements ResponseExtractor<HttpStatus> {

    // -----------------------------------------------------------------------------------------------------------------
    @Override
    public HttpStatus extractData(ClientHttpResponse response) throws IOException {
        return null;
    }
    // -----------------------------------------------------------------------------------------------------------------
}

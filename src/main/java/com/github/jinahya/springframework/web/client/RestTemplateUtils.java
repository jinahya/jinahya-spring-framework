package com.github.jinahya.springframework.web.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@Slf4j
public final class RestTemplateUtils {

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R applyBodyStream(
            final RestTemplate restTemplate, final URI url, HttpMethod method, final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super InputStream, ? extends R> bodyMapper) {
        log.debug("applyBodyStream({}, {}, {}, {}, {})", restTemplate, url, method, requestCallback, bodyMapper);
        return restTemplate.execute(
                url,
                method,
                requestCallback,
                r -> {
                    log.debug("response: {}", r);
                    try (InputStream body = r.getBody()) {
                        log.debug("body: {}", body);
                        return bodyMapper.apply(r, body);
                    }
                }
        );
    }

    public static <R> Optional<R> readBodyStream(
            final RestTemplate restTemplate, final URI url, final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super InputStream, ? extends R> bodyMapper) {
        return applyBodyStream(
                restTemplate,
                url,
                HttpMethod.GET,
                requestCallback,
                (r, b) -> {
                    log.debug("response: {}, body: {}", r, b);
                    try {
                        final HttpStatus statusCode = r.getStatusCode();
                        if (HttpStatus.OK != statusCode) {
                            log.warn("status code is not ok: {}", statusCode);
                            return Optional.empty();
                        }
                        return Optional.ofNullable(bodyMapper.apply(r, b));
                    } catch (final IOException ioe) {
                        throw new RestClientException(ioe.getMessage());
                    }
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static ClientHttpResponse acceptBodyStream(
            final RestTemplate restTemplate, final URI url, final HttpMethod method,
            final RequestCallback requestCallback,
            final BiConsumer<? super ClientHttpResponse, ? super InputStream> bodyConsumer) {
        log.debug("acceptBodyStream({}, {}, {}, {}, {})", restTemplate, url, method, requestCallback, bodyConsumer);
        return applyBodyStream(
                restTemplate,
                url,
                method,
                requestCallback,
                (r, b) -> {
                    log.debug("{}, {}", r, b);
                    bodyConsumer.accept(r, b);
                    return r;
                }
        );
    }

    public static ClientHttpResponse readBodyStream(
            final RestTemplate restTemplate, final URI url, final RequestCallback requestCallback,
            final BiConsumer<? super ClientHttpResponse, ? super InputStream> bodyConsumer) {
        return acceptBodyStream(
                restTemplate,
                url,
                HttpMethod.GET,
                requestCallback,
                (r, b) -> {
                    log.debug("{}, {}", r, b);
                    try {
                        final HttpStatus statusCode = r.getStatusCode();
                        if (HttpStatus.OK != statusCode) {
                            log.warn("status code is not ok: {}", statusCode);
                        }
                        bodyConsumer.accept(r, b);
                    } catch (final IOException ioe) {
                        throw new RestClientException(ioe.getMessage());
                    }
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <R> R applyBodyStored(
            final RestTemplate restTemplate, final URI url, final HttpMethod method,
            final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super Path, ? extends R> pathFunction) {
        return applyBodyStream(restTemplate, url, method, requestCallback, (r, b) -> {
            try {
                final Path path = Files.createTempFile(null, null);
                try {
                    final long length = Files.copy(b, path);
                    log.debug("path length: {}", length);
                    return pathFunction.apply(r, path);
                } finally {
                    final boolean deleted = Files.deleteIfExists(path);
                    log.debug("path deleted: {}", deleted);
                }
            } catch (final IOException ioe) {
                throw new RestClientException(ioe.getMessage());
            }
        });
    }

    public static <R> Optional<R> readBodyStored(
            final RestTemplate restTemplate, final URI url, final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super Path, ? extends R> pathFunction) {
        return applyBodyStored(restTemplate, url, HttpMethod.GET, requestCallback, (r, p) -> {
            try {
                final HttpStatus statusCode = r.getStatusCode();
                if (HttpStatus.OK == statusCode) {
                    log.warn("status code is not ok: {}", statusCode);
                    return Optional.empty();
                }
                return Optional.ofNullable(pathFunction.apply(r, p));
            } catch (final IOException ioe) {
                throw new RestClientException(ioe.getMessage());
            }
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    public static <T> ClientHttpResponse acceptJsonArrayElements(
            final RestTemplate restTemplate, final URI url, final HttpMethod method,
            final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super InputStream, Optional<JsonParser>> parserMapper,
            final BiFunction<? super ClientHttpResponse, ? super JsonParser, Optional<T>> elementMapper,
            final BiConsumer<? super ClientHttpResponse, ? super T> elementConsumer) {
        log.debug("acceptJsonArrayElements({}, {}, {}, {}, {}, {}, {})", restTemplate, url, method, requestCallback, parserMapper, elementConsumer, elementConsumer);
        return acceptBodyStream(
                restTemplate,
                url,
                method,
                requestCallback,
                (r, b) -> {
                    final Optional<JsonParser> parser = parserMapper.apply(r, b);
                    log.debug("parser: {}", parser);
                    parser.ifPresent(p -> {
                        try {
                            final JsonToken expectedStartArray = p.nextToken();
                            assert expectedStartArray == JsonToken.START_ARRAY;
                            while (p.nextToken() == JsonToken.START_OBJECT) {
                                elementMapper.apply(r, p).ifPresent(v -> elementConsumer.accept(r, v));
                            }
                            final JsonToken expectedEndArray = p.currentToken();
                            assert expectedEndArray == JsonToken.END_ARRAY;
                        } catch (final IOException ioe) {
                            throw new RestClientException(ioe.getMessage());
                        }
                    });
                }
        );
    }

    public static <T> ClientHttpResponse readJsonArrayElements(
            final RestTemplate restTemplate, final URI url, final RequestCallback requestCallback,
            final BiFunction<? super ClientHttpResponse, ? super InputStream, Optional<JsonParser>> parserMapper,
            final BiFunction<? super ClientHttpResponse, ? super JsonParser, Optional<T>> elementMapper,
            final BiConsumer<? super ClientHttpResponse, ? super T> elementConsumer) {
        log.debug("readJsonArrayElements({}, {}, {}, {}, {}, {})", restTemplate, url, requestCallback, parserMapper, elementConsumer, elementConsumer);
        return acceptJsonArrayElements(
                restTemplate,
                url,
                HttpMethod.GET,
                requestCallback,
                (r, b) -> {
                    log.debug("{}, {}", r, b);
                    try {
                        final HttpStatus statusCode = r.getStatusCode();
                        if (HttpStatus.OK != statusCode) {
                            log.warn("status code is not ok: {}", statusCode);
                            return Optional.empty();
                        }
                    } catch (final IOException ioe) {
                        throw new RestClientException(ioe.getMessage());
                    }
                    return parserMapper.apply(r, b);
                },
                elementMapper,
                elementConsumer
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    private RestTemplateUtils() {
        super();
    }
}

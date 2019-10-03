package com.github.jinahya.springframework.web.reactive.function.client.webclient;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.jinahya.springframework.web.client.RestTemplateUtils.acceptBodyStream;
import static com.github.jinahya.springframework.web.client.RestTemplateUtils.readJsonArrayElements;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith({MockitoExtension.class})
@Slf4j
class RestTemplateUtilsTest {

    // -----------------------------------------------------------------------------------------------------------------
    @Test
    void testAcceptBodyStream() throws IOException {
        final InputStream body = mock(InputStream.class);
        doReturn(body).when(clientHttpResponse).getBody();
        doAnswer(i -> i.getArgument(3, ResponseExtractor.class).extractData(clientHttpResponse))
                .when(restTemplate)
                .execute(any(), any(), any(), any())
        ;
        acceptBodyStream(restTemplate, null, null, null, (r, b) -> {
            assertThat(b).isEqualTo(body);
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE)
    static class Item {

        public String name;

        //public int age;
    }

    @Test
    void testReadJsonArrayElements() throws IOException {
        final byte[] bytes = IntStream.range(0, 8)
                .mapToObj(i -> "{\"name\": \"" + Integer.toString(i) + "\",\"age\": " + i + "}")
                .collect(Collectors.joining(",", "[", "]"))
                .getBytes(StandardCharsets.UTF_8);
        log.debug("{}", new String(bytes));
        when(clientHttpResponse.getStatusCode()).thenReturn(HttpStatus.OK);
        when(clientHttpResponse.getBody()).thenReturn(new ByteArrayInputStream(bytes));
        when(restTemplate.execute(any(), any(), any(), any()))
                .thenAnswer(i -> {
                    log.debug("invocation: {}", i);
                    return i.getArgument(3, ResponseExtractor.class).extractData(clientHttpResponse);
                })
        ;
        final JsonFactory factory = new JsonFactory();
        final ObjectMapper mapper = new ObjectMapper();
        log.debug("factory: {}", factory);
        final ClientHttpResponse response = readJsonArrayElements(
                restTemplate,
                null,
                null,
                (r, b) -> {
                    log.debug("response: {}, body: {}", r, b);
                    try {
                        final JsonParser parser = factory.createParser(b);
                        log.debug("parser: {}", parser);
                        return Optional.of(parser);
                    } catch (final IOException ioe) {
                        throw new RestClientException(ioe.getMessage());
                    }
                },
                (r, p) -> {
                    try {
                        final Item value = mapper.readValue(p, Item.class);
                        return Optional.of(value);
                    } catch (final IOException ioe) {
                        throw new RestClientException(ioe.getMessage());
                    }
                },
                (r, v) -> {
                    log.debug("array element: {}", v);
                }
        );
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ClientHttpResponse clientHttpResponse;
}

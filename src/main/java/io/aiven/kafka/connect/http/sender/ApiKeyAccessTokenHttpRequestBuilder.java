package io.aiven.kafka.connect.http.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Objects;

public class ApiKeyAccessTokenHttpRequestBuilder implements HttpRequestBuilder {

    ApiKeyAccessTokenHttpRequestBuilder() {
    }

    @Override
    public HttpRequest.Builder build(HttpSinkConfig config) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(config.apikeyAccessTokenUri(), "apikeyAccessTokenUri");

        final var accessTokenRequestBuilder = HttpRequest
                .newBuilder(config.apikeyAccessTokenUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()))
                .header(HEADER_CONTENT_TYPE, "application/json");

        AccessTokenRequest accessTokenRequest = new AccessTokenRequest();
        accessTokenRequest.setType("api-key");
        accessTokenRequest.setKey(config.apikeyAccessTokenKey());
        accessTokenRequest.setSecret(config.apikeyAccessTokenSecret());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String requestBody = objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(accessTokenRequest);
            return accessTokenRequestBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static class AccessTokenRequest {
        String type;
        String key;
        String secret;

        public AccessTokenRequest() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }
    }

}

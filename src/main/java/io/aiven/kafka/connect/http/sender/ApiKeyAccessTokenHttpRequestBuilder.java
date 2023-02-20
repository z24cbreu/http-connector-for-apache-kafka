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
        Objects.requireNonNull(config.oauth2AccessTokenUri(), "oauth2AccessTokenUri");
        Objects.requireNonNull(config.oauth2ClientId(), "oauth2ClientId");
        Objects.requireNonNull(config.oauth2ClientSecret(), "oauth2ClientSecret");

        final var accessTokenRequestBuilder = HttpRequest
                .newBuilder(config.oauth2AccessTokenUri())
                .timeout(Duration.ofSeconds(config.httpTimeout()))
                .header(HEADER_CONTENT_TYPE, "application/json");

        AccessTokenRequest accessTokenRequest = new AccessTokenRequest();
        accessTokenRequest.setType("api-key");
        accessTokenRequest.setKey(config.oauth2ClientId());
        accessTokenRequest.setSecret(config.oauth2ClientSecret().value());

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

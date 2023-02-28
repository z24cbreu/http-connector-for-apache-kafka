package io.aiven.kafka.connect.http.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
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
                .header(HEADER_CONTENT_TYPE, "application/x-www-form-urlencoded");

        AccessTokenRequest accessTokenRequest = new AccessTokenRequest("api-key",
                config.oauth2ClientId(), config.oauth2ClientSecret().value());

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String requestBody = objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(accessTokenRequest);
            return accessTokenRequestBuilder
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody));
        } catch (JsonProcessingException e) {
            throw new ConnectException("Cannot get OAuth2 ApiKey access token", e);
        }
    }

    private static class AccessTokenRequest {
        final String type;
        final String key;
        final String secret;

        public AccessTokenRequest(String type, String key, String secret) {
            this.type = type;
            this.key = key;
            this.secret = secret;
        }

        //Getters needed for serdes
        public String getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        public String getSecret() {
            return secret;
        }
    }

}

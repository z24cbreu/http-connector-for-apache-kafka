package io.aiven.kafka.connect.http.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class ApiKeyHttpSender extends HttpSender {

    private final Logger logger = LoggerFactory.getLogger(ApiKeyHttpSender.class);
    private String accessTokenAuthHeader;
    private final ObjectMapper objectMapper = new ObjectMapper();

    protected ApiKeyHttpSender(HttpSinkConfig config) {
        super(config);
    }

    protected ApiKeyHttpSender(HttpSinkConfig config, HttpClient httpClient) {
        super(config, httpClient);
    }

    @Override
    protected HttpResponse<String> sendWithRetries(HttpRequest.Builder requestBuilder, HttpResponseHandler httpResponseHandler) {
        final var accessTokenAwareRequestBuilder = loadOrRefreshAccessToken(requestBuilder);
        final HttpResponseHandler handler = response -> {
            if (response.statusCode() == 401) { // access denied or refresh of a token is needed
                this.accessTokenAuthHeader = null;
                this.sendWithRetries(requestBuilder, httpResponseHandler);
            } else {
                httpResponseHandler.onResponse(response);
            }
        };
        return super.sendWithRetries(accessTokenAwareRequestBuilder, handler);
    }

    private HttpRequest.Builder loadOrRefreshAccessToken(final HttpRequest.Builder requestBuilder) {
        if (accessTokenAuthHeader == null) {
            logger.info("Configure ApiKey for URI: {} and Key: {}",
                    config.apikeyAccessTokenUri(), config.apikeyAccessTokenKey());
            try {
                final var response =
                        super.sendWithRetries(
                                new ApiKeyAccessTokenHttpRequestBuilder().build(config),
                                HttpResponseHandler.ON_HTTP_ERROR_RESPONSE_HANDLER
                        );
                accessTokenAuthHeader = buildAccessTokenAuthHeader(response.body());
            } catch (final IOException e) {
                throw new ConnectException("Couldn't get OAuth2 access token", e);
            }
        }
        return requestBuilder.setHeader(HttpRequestBuilder.HEADER_AUTHORIZATION, accessTokenAuthHeader);
    }

    private String buildAccessTokenAuthHeader(final String responseBody) throws JsonProcessingException {
        final var accessTokenResponse =
                objectMapper.readValue(responseBody, new TypeReference<Map<String, String>>() {});
        if (!accessTokenResponse.containsKey("access_token")) {
            throw new ConnectException(
                    "Couldn't find access token property "
                            + config.oauth2ResponseTokenProperty()
                            + " in response properties: "
                            + accessTokenResponse.keySet());
        }
        final var accessToken = accessTokenResponse.get("access_token");
        return String.format("%s %s", "Bearer", accessToken);
    }
}

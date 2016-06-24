package io.fullstack.common;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * Create a Pubsub client using portable credentials.
 */
public class PortableConfiguration {

    // Default factory method.
    public static Pubsub.Builder createPubsubClient() throws IOException {
        return createPubsubClient(Utils.getDefaultTransport(),
                Utils.getDefaultJsonFactory());
    }

    // A factory method that allows you to use your own HttpTransport
    // and JsonFactory.
    public static Pubsub.Builder createPubsubClient(HttpTransport httpTransport,
            JsonFactory jsonFactory) throws IOException {
        Preconditions.checkNotNull(httpTransport);
        Preconditions.checkNotNull(jsonFactory);
        GoogleCredential credential = GoogleCredential.getApplicationDefault(
                httpTransport, jsonFactory);
        // In some cases, you need to add the scope explicitly.
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(PubsubScopes.all());
        }
        // Please use custom HttpRequestInitializer for automatic
        // retry upon failures.  We provide a simple reference
        // implementation in the "Retry Handling" section.
        HttpRequestInitializer initializer =
                new RetryHttpInitializerWrapper(credential);
        return new Pubsub.Builder(httpTransport, jsonFactory, initializer);
    }
}

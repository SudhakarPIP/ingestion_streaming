package com.example.ingestion.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.IOException;

@Configuration
@Slf4j
public class BigQueryConfig {

    @Value("${bigquery.project-id}")
    private String projectId;

    @Value("${bigquery.credentials-path:}")
    private String credentialsPath;

    @Bean
    public BigQuery bigQuery() throws IOException {
        // Validate project ID
        if (projectId == null || projectId.isEmpty() || projectId.equals("your-gcp-project-id")) {
            log.warn("⚠️  BigQuery project-id is not configured!");
            log.warn("   Please set bigquery.project-id in application.yml or BIGQUERY_PROJECT_ID environment variable.");
            log.warn("   BigQuery operations will fail until credentials are properly configured.");
            log.warn("   See BIGQUERY_SETUP.md for detailed setup instructions.");
        }

        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
                .setProjectId(projectId);

        if (credentialsPath != null && !credentialsPath.isEmpty()) {
            try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);
                builder.setCredentials(credentials);
                log.info("✅ Using BigQuery credentials from file: {}", credentialsPath);
            } catch (IOException e) {
                log.error("❌ Failed to load BigQuery credentials from file: {}", credentialsPath);
                log.error("   Error: {}", e.getMessage());
                log.error("   Please verify the file path is correct and the file is readable.");
                throw new IOException("Failed to load BigQuery credentials from file: " + credentialsPath, e);
            }
        } else {
            log.info("ℹ️  Using default application credentials (GOOGLE_APPLICATION_CREDENTIALS environment variable or GCP default)");
            log.info("   If running locally, you may need to set GOOGLE_APPLICATION_CREDENTIALS environment variable.");
        }

        BigQuery bigQuery = builder.build().getService();
        log.info("✅ BigQuery client initialized for project: {}", projectId);
        return bigQuery;
    }

    @Bean
    public Storage storage() throws IOException {
        StorageOptions.Builder builder = StorageOptions.newBuilder()
                .setProjectId(projectId);

        if (credentialsPath != null && !credentialsPath.isEmpty()) {
            try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
                GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsStream);
                builder.setCredentials(credentials);
                log.info("✅ Using Cloud Storage credentials from file: {}", credentialsPath);
            } catch (IOException e) {
                log.error("❌ Failed to load Cloud Storage credentials from file: {}", credentialsPath);
                log.error("   Error: {}", e.getMessage());
                throw new IOException("Failed to load Cloud Storage credentials from file: " + credentialsPath, e);
            }
        } else {
            log.info("ℹ️  Using default application credentials for Cloud Storage");
        }

        Storage storage = builder.build().getService();
        log.info("✅ Cloud Storage client initialized for project: {}", projectId);
        return storage;
    }
}


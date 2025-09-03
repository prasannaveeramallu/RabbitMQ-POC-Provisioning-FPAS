package com.example.RabbitMQ_POC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import io.github.cdimascio.dotenv.Dotenv;

public class DockerWorkerLauncher {
    private static final Logger log = LoggerFactory.getLogger(DockerWorkerLauncher.class);
    private static final AtomicBoolean imageBuilt = new AtomicBoolean(false);
    private static final ExecutorService EXEC = Executors.newCachedThreadPool();
    private static final String IMAGE_NAME = "provision-worker";
    private static final String DOCKERFILE = "Dockerfile.worker";
    private static final int MAX_ATTEMPTS = 3;
    private static final long BASE_BACKOFF_MS = 1000; // 1s
    // JOB_FILE_POLICY: KEEP, DELETE, ARCHIVE (default KEEP)
    private static final String JOB_FILE_POLICY = System.getenv().getOrDefault("JOB_FILE_POLICY", "KEEP").toUpperCase();
    private static final Dotenv dotenv = Dotenv.load();

    public static void launchAsync(Path jobFile, String jobId) {
        CompletableFuture.runAsync(() -> runWorker(jobFile, jobId), EXEC)
                .exceptionally(ex -> { log.error("Worker launch failed for job {}", jobId, ex); return null; });
    }

    private static void runWorker(Path jobFile, String jobId) {
        try {
            ensureImageBuilt();
            if (!Files.exists(jobFile)) {
                log.error("Job file {} missing; aborting worker launch for job {}", jobFile, jobId);
                return;
            }
            String abs = jobFile.toAbsolutePath().toString();
            // Windows path adjustments not required for ProcessBuilder args.
            List<String> baseCmd = new ArrayList<>();
            baseCmd.addAll(List.of("docker", "run", "--rm"));
            // Add environment variables
            addEnvVar(baseCmd, "MONGODB_URI");
            addEnvVar(baseCmd, "AWS_REGION");
            addEnvVar(baseCmd, "AWS_ACCESS_KEY_ID");
            addEnvVar(baseCmd, "AWS_SECRET_ACCESS_KEY");
            addEnvVar(baseCmd, "AZURE_CLIENT_ID");
            addEnvVar(baseCmd, "AZURE_CLIENT_SECRET");
            addEnvVar(baseCmd, "AZURE_TENANT_ID");
            addEnvVar(baseCmd, "AZURE_SUBSCRIPTION_ID");
            addEnvVar(baseCmd, "AZURE_SECRET_ID");
            addEnvVar(baseCmd, "AZURE_RESOURCE_GROUP");
            addEnvVar(baseCmd, "AZURE_LOCATION");
            addEnvVar(baseCmd, "RABBITMQ_HOST");
            addEnvVar(baseCmd, "RABBITMQ_USERNAME");
            addEnvVar(baseCmd, "RABBITMQ_PASSWORD");
            baseCmd.addAll(List.of("-v", abs + ":/app/job.json", IMAGE_NAME, "/app/job.json"));

            if (!dockerAvailable()) {
                log.warn("Docker not available. Falling back to local JVM execution for job {}.", jobId);
                runLocalFallback(jobFile, jobId);
                handleJobFile(jobFile);
                return;
            }

            int attempt = 1;
            while (attempt <= MAX_ATTEMPTS) {
                log.info("Launching worker (attempt {}/{} ) for job {}: {}", attempt, MAX_ATTEMPTS, jobId, String.join(" ", baseCmd));
                int exit = runProcessStreaming(jobId, baseCmd);
                if (exit == 0) {
                    log.info("Worker finished for job {} with exit code 0 on attempt {} (business status in Mongo)", jobId, attempt);
                    handleJobFile(jobFile);
                    return;
                }
                if (attempt == MAX_ATTEMPTS) {
                    log.error("Worker failed for job {} after {} attempts (last exit code {}).", jobId, attempt, exit);
                    handleJobFile(jobFile);
                    break;
                }
                long backoff = BASE_BACKOFF_MS * (1L << (attempt - 1));
                log.warn("Worker attempt {} failed for job {} (exit {}). Retrying after {} ms.", attempt, jobId, exit, backoff);
                try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                attempt++;
            }
        } catch (Exception e) {
            log.error("Error running worker for job {}", jobId, e);
            handleJobFile(jobFile);
        }
    }

    private static void addEnvVar(List<String> cmd, String key) {
        String value = dotenv.get(key, null);
        if (value != null) {
            cmd.add("-e");
            cmd.add(key + "=" + value);
        }
    }

    private static int runProcessStreaming(String jobId, List<String> cmd) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .start();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String line; while ((line = br.readLine()) != null) { log.info("[worker:{}] {}", jobId, line); }
        }
        return p.waitFor();
    }

    private static void ensureImageBuilt() throws IOException, InterruptedException {
        if (imageBuilt.get()) return;
        //quick existence check: docker image inspection
        Process inspect = new ProcessBuilder("docker", "image", "inspect", IMAGE_NAME).start();
        int inspectCode = inspect.waitFor();
        if (inspectCode != 0) {
            log.info("Building Docker image '{}' using {}", IMAGE_NAME, DOCKERFILE);
            Process build = new ProcessBuilder("docker", "build", "-f", DOCKERFILE, "-t", IMAGE_NAME, ".")
                    .inheritIO()
                    .start();
            int buildCode = build.waitFor();
            if (buildCode != 0) {
                throw new IOException("Docker build failed with exit code " + buildCode);
            }
        } else {
            log.debug("Image '{}' already exists; skipping build", IMAGE_NAME);
        }
        imageBuilt.set(true);
    }

    private static boolean dockerAvailable() {
        try {
            Process p = new ProcessBuilder("docker", "version").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void runLocalFallback(Path jobFile, String jobId) {
        try {
            //run worker main directly in same JVM (separate process not required) for POC.
            log.info("Starting local fallback worker for job {}", jobId);
            com.example.RabbitMQ_POC.worker.ProvisionWorker.main(new String[]{jobFile.toAbsolutePath().toString()});
        } catch (Exception e) {
            log.error("Local fallback execution failed for job {}", jobId, e);
        }
    }

    private static void handleJobFile(Path jobFile) {
        switch (JOB_FILE_POLICY) {
            case "DELETE" -> {
                try {
                    Files.deleteIfExists(jobFile);
                    log.debug("Deleted job file {}", jobFile);
                } catch (IOException ioe) {
                    log.warn("Failed to delete job file {}: {}", jobFile, ioe.getMessage());
                }
            }
            case "ARCHIVE" -> {
                try {
                    Path archiveDir = jobFile.getParent().resolve("archive");
                    Files.createDirectories(archiveDir);
                    Path target = archiveDir.resolve(jobFile.getFileName());
                    Files.move(jobFile, target, StandardCopyOption.REPLACE_EXISTING);
                    log.debug("Archived job file {} -> {}", jobFile, target);
                } catch (IOException ioe) {
                    log.warn("Failed to archive job file {}: {}", jobFile, ioe.getMessage());
                }
            }
            default -> { /* KEEP: do nothing */ }
        }
    }
}
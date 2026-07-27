package newpipestreamextractor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;

import org.schabi.newpipe.extractor.NewPipe;
import org.schabi.newpipe.extractor.ServiceList;
import org.schabi.newpipe.extractor.downloader.Downloader;
import org.schabi.newpipe.extractor.exceptions.ExtractionException;
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException;
import org.schabi.newpipe.extractor.stream.AudioStream;
import org.schabi.newpipe.extractor.MediaFormat;
import org.schabi.newpipe.extractor.stream.StreamExtractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Long-running background daemon that fetches YouTube m4a audio stream URLs
 * using NewPipeExtractor.
 *
 * Protocol (line-delimited JSON over stdin/stdout, UTF-8):
 *
 *   IN  (one per line):  {"id":"1","url":"https://youtube.com/watch?v=..."}
 *   OUT success:         {"id":"1","ok":true,"streamUrl":"...","format":"M4A",
 *                          "bitrate":128,"itag":140,"title":"...","durationSeconds":213}
 *   OUT failure:         {"id":"1","ok":false,"error":"message"}
 *
 * - Requests are processed concurrently on a thread pool; slow extractions
 *   never block others.
 * - Each response is written as a single atomic line so concurrent workers
 *   can never interleave/corrupt each other's output.
 * - All logging/diagnostics go to stderr, never stdout, to keep the stdout
 *   protocol clean for the parent process to parse.
 * - The process exits cleanly once stdin is closed (EOF) and all
 *   in-flight requests have finished.
 */
public final class NewPipeStreamExtractor {
    private static final Gson GSON = new Gson();
    private static final Object STDOUT_LOCK = new Object();
    private static final PrintStream OUT =
            new PrintStream(System.out, true, StandardCharsets.UTF_8);

    public static void main(String[] args) throws IOException {
        // NewPipeExtractor needs a Downloader implementation installed once, globally.
        NewPipe.init(DownloaderImpl.init(null));

        int poolSize = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        log("daemon started, pool size=" + poolSize);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                final String raw = line.trim();
                if (raw.isEmpty()) {
                    continue;
                }
                if (raw.equals("__shutdown__")) {
                    shuttingDown.set(true);
                    break;
                }

                pool.submit(() -> handleRequest(raw));
            }
        } finally {
            // Stop accepting new work, let in-flight extractions finish, then exit.
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    log("timed out waiting for in-flight extractions, forcing shutdown");
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
            log("daemon exiting");
        }
    }

    private static void handleRequest(String raw) {
        String id = null;
        try {
            JsonObject req = GSON.fromJson(raw, JsonObject.class);
            if (req == null || !req.has("id") || !req.has("url")) {
                writeError(null, "malformed request, expected {\"id\":..,\"url\":..}");
                return;
            }
            id = req.get("id").getAsString();
            String url = req.get("url").getAsString();

            AudioStream best = extractBestM4a(url);
            JsonObject extras = extractMetadata(url);

            if (best == null) {
                writeError(id, "no m4a audio stream found for this video");
                return;
            }

            JsonObject resp = new JsonObject();
            resp.addProperty("id", id);
            resp.addProperty("ok", true);
            resp.addProperty("streamUrl", best.getContent());
            resp.addProperty("format", "M4A");
            resp.addProperty("bitrate", best.getAverageBitrate());
            resp.addProperty("itag", best.getItag());
            if (extras != null) {
                if (extras.has("title")) resp.add("title", extras.get("title"));
                if (extras.has("durationSeconds")) resp.add("durationSeconds", extras.get("durationSeconds"));
            }
            writeLine(resp);

        } catch (JsonSyntaxException e) {
            writeError(id, "invalid JSON: " + e.getMessage());
        } catch (ReCaptchaException e) {
            writeError(id, "reCAPTCHA / rate limited: " + e.getMessage());
        } catch (ExtractionException e) {
            writeError(id, "extraction failed: " + e.getMessage());
        } catch (IOException e) {
            writeError(id, "network error: " + e.getMessage());
        } catch (Exception e) {
            // Never let a single bad request kill the pool thread / daemon.
            writeError(id, "unexpected error: " + e);
        }
    }

    /**
     * Runs extraction twice (streams + basic metadata) is wasteful, so in
     * practice you'll likely want to fetch the page once and read both off
     * the same StreamExtractor. Kept separate here for clarity; see the
     * "single fetch" note below for the optimization.
     */
    private static AudioStream extractBestM4a(String url) throws ExtractionException, IOException {
        StreamExtractor extractor = ServiceList.YouTube.getStreamExtractor(url);
        extractor.fetchPage();

        List<AudioStream> streams = extractor.getAudioStreams();
        AudioStream best = null;
        for (AudioStream s : streams) {
            if (s.getFormat() == MediaFormat.M4A) {
                if (best == null || s.getAverageBitrate() > best.getAverageBitrate()) {
                    best = s;
                }
            }
        }
        return best;
    }

    private static JsonObject extractMetadata(String url) {
        try {
            StreamExtractor extractor = ServiceList.YouTube.getStreamExtractor(url);
            extractor.fetchPage();
            JsonObject o = new JsonObject();
            o.addProperty("title", extractor.getName());
            o.addProperty("durationSeconds", extractor.getLength());
            return o;
        } catch (Exception e) {
            return null; // metadata is best-effort; don't fail the whole request over it
        }
    }

    private static void writeError(String id, String message) {
        JsonObject resp = new JsonObject();
        resp.addProperty("id", id);
        resp.addProperty("ok", false);
        resp.addProperty("error", message);
        writeLine(resp);
    }

    private static void writeLine(JsonObject obj) {
        String json = GSON.toJson(obj);
        synchronized (STDOUT_LOCK) {
            OUT.println(json);
            OUT.flush();
        }
    }

    private static void log(String msg) {
        System.err.println("[yt-daemon] " + msg);
    }
}

/**
 * OkHttp-backed Downloader implementation, required by NewPipeExtractor.
 * Adapted from NewPipeExtractor's own reference demo downloader.
 */
final class DownloaderImpl extends Downloader {

    private static DownloaderImpl instance;
    private final OkHttpClient client;

    private DownloaderImpl(OkHttpClient.Builder builder) {
        this.client = builder
                .readTimeout(30, TimeUnit.SECONDS)
                .connectTimeout(15, TimeUnit.SECONDS)
                .build();
    }

    static DownloaderImpl init(OkHttpClient.Builder builder) {
        instance = new DownloaderImpl(builder != null ? builder : new OkHttpClient.Builder());
        return instance;
    }

    static DownloaderImpl getInstance() {
        return instance;
    }

    @Override
    public org.schabi.newpipe.extractor.downloader.Response execute(
            org.schabi.newpipe.extractor.downloader.Request request)
            throws IOException, ReCaptchaException {

        final String httpMethod = request.httpMethod();
        final String url = request.url();
        final Map<String, List<String>> headers = request.headers();
        final byte[] dataToSend = request.dataToSend();

        RequestBody requestBody = null;
        if (dataToSend != null) {
            requestBody = RequestBody.create(dataToSend);
        }

        okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder()
                .method(httpMethod, requestBody)
                .url(url)
                .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");

        for (Map.Entry<String, List<String>> pair : headers.entrySet()) {
            String headerName = pair.getKey();
            List<String> values = pair.getValue();
            if (values.size() > 1) {
                requestBuilder.removeHeader(headerName);
                for (String v : values) {
                    requestBuilder.addHeader(headerName, v);
                }
            } else if (values.size() == 1) {
                requestBuilder.header(headerName, values.get(0));
            }
        }

        try (okhttp3.Response response = client.newCall(requestBuilder.build()).execute()) {
            if (response.code() == 429) {
                throw new ReCaptchaException("reCAPTCHA challenge requested", url);
            }

            ResponseBody body = response.body();
            String responseBody = body != null ? body.string() : null;
            String latestUrl = response.request().url().toString();

            return new org.schabi.newpipe.extractor.downloader.Response(
                    response.code(),
                    response.message(),
                    response.headers().toMultimap(),
                    responseBody,
                    latestUrl
            );
        }
    }
}
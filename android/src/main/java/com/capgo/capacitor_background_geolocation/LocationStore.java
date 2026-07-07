package com.capgo.capacitor_background_geolocation;

import android.content.Context;
import android.content.SharedPreferences;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import com.getcapacitor.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

// Persists the configuration for a native location watcher and delivers
// location updates to a configured URL directly from native code. This mirrors
// GeofenceStore and exists so that background location delivery keeps working
// after the WebView (and its JavaScript callback) has been destroyed.
//
// Delivery is buffered: a fix that cannot be POSTed right now (no connectivity,
// server hiccup) is appended to an on-disk queue and retried before the next
// fix, so driving through a dead zone loses nothing. Permanent rejections
// (4xx — e.g. the watcher's token was rotated or the trip ended server-side)
// are dropped, not retried forever.
final class LocationStore {

    private static final String PREFS_NAME = "CapgoBackgroundGeolocationWatcher";
    private static final String KEY_ENABLED = "enabled";
    private static final String KEY_URL = "url";
    private static final String KEY_TITLE = "title";
    private static final String KEY_MESSAGE = "message";
    private static final String KEY_DISTANCE_FILTER = "distanceFilter";

    // One JSON entry per line: {"url": "...", "body": {...}}. The url is stored
    // per entry so a mid-watch url change never misroutes older fixes.
    private static final String QUEUE_FILE = "capgo_bgloc_pending.jsonl";
    // ~2000 fixes ≈ 50 km of driving at a 25 m distance filter — a long dead zone.
    private static final int MAX_QUEUE_ENTRIES = 2000;
    // The common offline case appends one line without reading the file. The
    // expensive read-trim-rewrite only runs when the file crosses this size, so
    // the queue can briefly overshoot MAX_QUEUE_ENTRIES before being trimmed back.
    private static final long MAX_QUEUE_BYTES = 1024L * 1024L; // ~1 MB
    // At trim time, shed fixes older than this. Guards the head-of-line case: a
    // permanently 5xx-ing entry can otherwise wedge the queue forever. Consumers
    // treat the odometer as distance ground truth, so day-old points are useless.
    private static final long MAX_QUEUE_AGE_MS = 24L * 60L * 60L * 1000L; // 24 h
    private static final int HTTP_TIMEOUT_MS = 10000;
    private static final Object QUEUE_LOCK = new Object();

    // Threading contract:
    //   * deliverLocation / flushQueue / queueLocation are invoked ONLY from the
    //     service's single-thread postExecutor, so they never run concurrently
    //     with one another — appends and flushes are naturally serialized.
    //   * clear() is the one exception: it runs on the main thread (via stop()).
    //   * QUEUE_LOCK guards every file mutation so clear() can safely race the
    //     executor. flushQueue deliberately performs its network POSTs OUTSIDE
    //     the lock so a main-thread clear() never blocks behind a slow request.

    private LocationStore() {}

    private static SharedPreferences prefs(Context context) {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
    }

    // Persists the watcher config. A null or empty url disables native delivery.
    static void saveSetup(Context context, String url, String title, String message, float distanceFilter) {
        SharedPreferences.Editor editor = prefs(context).edit();
        if (url == null || url.isEmpty()) {
            editor.clear();
        } else {
            editor
                .putBoolean(KEY_ENABLED, true)
                .putString(KEY_URL, url)
                .putString(KEY_TITLE, title)
                .putString(KEY_MESSAGE, message)
                .putFloat(KEY_DISTANCE_FILTER, distanceFilter);
        }
        editor.apply();
    }

    static void clear(Context context) {
        prefs(context).edit().clear().apply();
        synchronized (QUEUE_LOCK) {
            //noinspection ResultOfMethodCallIgnored
            queueFile(context).delete();
        }
    }

    static boolean isEnabled(Context context) {
        SharedPreferences prefs = prefs(context);
        return prefs.getBoolean(KEY_ENABLED, false) && prefs.getString(KEY_URL, null) != null;
    }

    static String getUrl(Context context) {
        return prefs(context).getString(KEY_URL, null);
    }

    static String getTitle(Context context) {
        return prefs(context).getString(KEY_TITLE, "Using your location");
    }

    static String getMessage(Context context) {
        return prefs(context).getString(KEY_MESSAGE, "");
    }

    static float getDistanceFilter(Context context) {
        return prefs(context).getFloat(KEY_DISTANCE_FILTER, 0f);
    }

    // Delivers one fix: drain any backlog first (preserves order), then POST the
    // fresh fix, buffering it when delivery is currently impossible. Runs
    // synchronously — callers must invoke it off the main thread.
    static void deliverLocation(Context context, JSONObject data) {
        String urlString = getUrl(context);
        if (urlString == null || urlString.isEmpty()) {
            return;
        }
        // Fast path: when there is clearly no connectivity, skip the network
        // attempt entirely (which would otherwise stall for the full connect +
        // read timeout) and buffer the fix in milliseconds. Under a sustained
        // dead zone this keeps the postExecutor from building an unbounded
        // backlog of doomed, timeout-bound tasks.
        if (!isConnected(context)) {
            queueLocation(context, urlString, data);
            return;
        }
        boolean pathClear = flushQueue(context);
        if (!pathClear || !attemptPost(urlString, data)) {
            queueLocation(context, urlString, data);
        }
    }

    /**
     * Retries every buffered fix in order. Stops at the first transient failure
     * (still offline). Returns true when the queue is empty afterwards.
     */
    static boolean flushQueue(Context context) {
        // Snapshot the queue under the lock, then release it for the (slow)
        // network phase — see the threading contract above. This is safe because
        // only the postExecutor thread appends, so `lines` cannot change beneath
        // us while we post; the sole concurrent mutator is clear(), which just
        // deletes the file (handled when we re-acquire the lock to rewrite).
        List<String> lines;
        synchronized (QUEUE_LOCK) {
            File file = queueFile(context);
            if (!file.exists()) {
                return true;
            }
            // No connectivity: don't burn a per-entry timeout confirming what we
            // already know. Leave the queue intact for the next attempt.
            if (!isConnected(context)) {
                return false;
            }
            lines = readLines(file);
            if (lines.isEmpty()) {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
                return true;
            }
        }

        int handled = 0;
        for (String line : lines) {
            String url;
            JSONObject body;
            try {
                JSONObject entry = new JSONObject(line);
                url = entry.getString("url");
                body = entry.getJSONObject("body");
            } catch (Exception e) {
                handled++; // corrupt line — drop it
                continue;
            }
            if (!attemptPost(url, body)) {
                break; // still offline/transient — keep this one and the rest
            }
            handled++;
        }
        if (handled == 0) {
            return false;
        }

        synchronized (QUEUE_LOCK) {
            File file = queueFile(context);
            if (!file.exists()) {
                // clear() ran while we were posting (the trip was stopped): the
                // queue is intentionally gone, so drop the un-posted remainder.
                return true;
            }
            List<String> remaining = lines.subList(handled, lines.size());
            if (remaining.isEmpty()) {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
                Logger.debug("Location queue drained (" + handled + " handled)");
                return true;
            }
            writeLines(file, remaining);
            return false;
        }
    }

    private static void queueLocation(Context context, String url, JSONObject body) {
        synchronized (QUEUE_LOCK) {
            try {
                JSONObject entry = new JSONObject();
                entry.put("url", url);
                entry.put("body", body);
                File file = queueFile(context);
                // Common case: append one line without reading the whole file.
                // A 2000-entry queue would otherwise cost a ~500 KB read+rewrite
                // per fix while offline; appending is O(1).
                appendLine(file, entry.toString());
                // Only pay the read-trim-rewrite when the file has actually grown
                // large. length() is a cheap stat, checked on every append.
                if (file.length() > MAX_QUEUE_BYTES) {
                    trimQueue(file);
                }
            } catch (Exception e) {
                Logger.error("Could not queue location for retry", e);
            }
        }
    }

    // Appends a single line to the queue file, creating it if needed. Caller
    // holds QUEUE_LOCK.
    private static void appendLine(File file, String line) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file, true)) {
            out.write(line.getBytes(StandardCharsets.UTF_8));
            out.write('\n');
        }
    }

    // Bounds the queue two ways: drops fixes older than MAX_QUEUE_AGE_MS (which
    // also unwedges a head-of-line entry the server keeps 5xx-ing), then sheds
    // the OLDEST fixes beyond MAX_QUEUE_ENTRIES. Caller holds QUEUE_LOCK.
    private static void trimQueue(File file) {
        List<String> lines = readLines(file);
        long cutoff = System.currentTimeMillis() - MAX_QUEUE_AGE_MS;
        List<String> kept = new ArrayList<>(lines.size());
        for (String line : lines) {
            if (!isExpired(line, cutoff)) {
                kept.add(line);
            }
        }
        if (kept.size() > MAX_QUEUE_ENTRIES) {
            kept = new ArrayList<>(kept.subList(kept.size() - MAX_QUEUE_ENTRIES, kept.size()));
        }
        if (kept.size() != lines.size()) {
            Logger.debug("Location queue trimmed: " + lines.size() + " -> " + kept.size() + " entries");
        }
        writeLines(file, kept);
    }

    // True when the entry's captured fix time is older than the cutoff. Unparseable
    // or timeless lines are kept here and left for the normal drain/drop path.
    private static boolean isExpired(String line, long cutoff) {
        try {
            long time = new JSONObject(line).getJSONObject("body").optLong("time", 0L);
            return time > 0L && time < cutoff;
        } catch (Exception e) {
            return false;
        }
    }

    // Best-effort connectivity check. Returns true when unknown (no permission /
    // no service) so we never wrongly skip a delivery that could have succeeded —
    // a false "connected" just falls through to attemptPost, which handles the
    // resulting IOException as a normal transient failure.
    private static boolean isConnected(Context context) {
        try {
            ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) {
                return true;
            }
            Network network = cm.getActiveNetwork();
            if (network == null) {
                return false;
            }
            NetworkCapabilities caps = cm.getNetworkCapabilities(network);
            return caps != null && caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET);
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * true = handled (delivered, or rejected permanently and dropped);
     * false = transient failure (offline, 429, 5xx) — worth retrying later.
     */
    private static boolean attemptPost(String urlString, JSONObject body) {
        try {
            int code = postJson(urlString, body);
            if (code >= 200 && code < 300) {
                return true;
            }
            if (code == 429 || code >= 500) {
                return false;
            }
            // Other 4xx: the watcher's token was rotated or the target is gone —
            // retrying can never succeed.
            Logger.error("Location POST rejected permanently with " + code + "; dropping fix");
            return true;
        } catch (MalformedURLException e) {
            // A bad url can never become valid — retrying would loop forever and
            // head-of-line-block every fix behind it. Drop it.
            Logger.error("Location POST url is malformed; dropping fix", e);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // POSTs JSON to the url, returning the response code. Throws only on
    // network-level failure (no connectivity, timeout).
    private static int postJson(String urlString, JSONObject data) throws IOException {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(urlString);
            byte[] body = data.toString().getBytes(StandardCharsets.UTF_8);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            // 10s (was 15s): with the offline fast path most stalls are now brief
            // radio flaps, not dead zones, so a shorter ceiling frees the executor
            // and the mobile radio sooner while still tolerating a slow handshake.
            connection.setConnectTimeout(HTTP_TIMEOUT_MS);
            connection.setReadTimeout(HTTP_TIMEOUT_MS);
            connection.setDoOutput(true);
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", String.valueOf(body.length));
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(body);
            }
            int responseCode = connection.getResponseCode();
            Logger.debug("Location POST finished with response code: " + responseCode);
            return responseCode;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static File queueFile(Context context) {
        return new File(context.getFilesDir(), QUEUE_FILE);
    }

    private static List<String> readLines(File file) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            Logger.error("Could not read the pending-location queue", e);
        }
        return lines;
    }

    private static void writeLines(File file, List<String> lines) {
        File tmp = new File(file.getParentFile(), QUEUE_FILE + ".tmp");
        try (FileOutputStream out = new FileOutputStream(tmp)) {
            for (String line : lines) {
                out.write(line.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
        } catch (IOException e) {
            Logger.error("Could not persist the pending-location queue", e);
            return;
        }
        if (!tmp.renameTo(file)) {
            Logger.error("Could not replace the pending-location queue file");
        }
    }
}

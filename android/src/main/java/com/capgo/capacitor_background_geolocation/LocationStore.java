package com.capgo.capacitor_background_geolocation;

import android.content.Context;
import android.content.SharedPreferences;
import com.getcapacitor.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
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
    private static final Object QUEUE_LOCK = new Object();

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
        synchronized (QUEUE_LOCK) {
            File file = queueFile(context);
            if (!file.exists()) {
                return true;
            }
            List<String> lines = readLines(file);
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
                List<String> lines = file.exists() ? readLines(file) : new ArrayList<>();
                lines.add(entry.toString());
                // Bounded: shed the OLDEST fixes beyond the cap. Consumers keep the
                // odometer as distance ground truth, so ancient points matter least.
                if (lines.size() > MAX_QUEUE_ENTRIES) {
                    lines = new ArrayList<>(lines.subList(lines.size() - MAX_QUEUE_ENTRIES, lines.size()));
                }
                writeLines(file, lines);
            } catch (Exception e) {
                Logger.error("Could not queue location for retry", e);
            }
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
            connection.setConnectTimeout(15000);
            connection.setReadTimeout(15000);
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

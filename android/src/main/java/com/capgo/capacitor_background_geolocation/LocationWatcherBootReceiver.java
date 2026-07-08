package com.capgo.capacitor_background_geolocation;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.os.Build;
import com.getcapacitor.Logger;

/**
 * Restores native location delivery after a device reboot or an app update.
 *
 * START_STICKY only survives process kills — a reboot clears the started
 * service entirely, and ACTION_MY_PACKAGE_REPLACED (an APK update) kills it
 * without a sticky restart. The watcher config lives in SharedPreferences
 * (see LocationStore), so all that's needed is to start the foreground
 * service again; its onStartCommand fully re-initializes from the persisted
 * config with no app UI involved.
 *
 * Platform notes:
 *  - BOOT_COMPLETED / MY_PACKAGE_REPLACED receivers are exempt from the
 *    Android 12+ restriction on starting foreground services from the
 *    background.
 *  - Android 15's list of FGS types banned from BOOT_COMPLETED starts does
 *    NOT include `location`; a location FGS may start here provided the app
 *    holds ACCESS_BACKGROUND_LOCATION ("Allow all the time").
 *  - BOOT_COMPLETED is delivered after the user's first unlock on
 *    file-based-encryption devices, so tracking resumes at unlock, not the
 *    literal boot instant.
 *  - Some OEMs (ColorOS/OxygenOS "auto-launch") may still suppress the
 *    broadcast unless the user allows it — nothing code can do about that,
 *    so failures are logged, never thrown.
 */
public class LocationWatcherBootReceiver extends BroadcastReceiver {

    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent == null || !GeofenceBootReceiver.shouldRestoreAction(intent.getAction())) {
            return;
        }
        if (!LocationStore.isEnabled(context)) {
            return; // no watcher was running — nothing to restore
        }
        try {
            Intent serviceIntent = new Intent(context, BackgroundGeolocationService.class);
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(serviceIntent);
            } else {
                context.startService(serviceIntent);
            }
            Logger.debug("Location watcher restarted after " + intent.getAction());
        } catch (Exception exception) {
            // e.g. an OEM denying the FGS start — the watcher resumes on next app open.
            Logger.error("Could not restart the location watcher after " + intent.getAction(), exception);
        }
    }
}

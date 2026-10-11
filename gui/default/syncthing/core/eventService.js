var debugEvents = !true;

angular.module('syncthing.core')
    .service('Events', ['$http', '$rootScope', '$timeout', function ($http, $rootScope, $timeout) {
        'use strict';

        var lastID = 0;
        var self = this;

        // Transport note: the legacy path below polls with $http. Every $http
        // completion is scheduled through $q -> $evalAsync -> $browser.defer ->
        // setTimeout, and browsers clamp setTimeout in hidden tabs (1s at
        // first, then budget throttling down to roughly one wakeup per minute).
        // Measured effect on this GUI: StateChanged events observed in batches
        // 27-188 s late in a background tab while the server long-poll flushed
        // instantly. The fetch-based path therefore drives the event loop:
        // fetch promises resolve as microtasks, which are not timer-throttled,
        // and the batch is processed in one synchronous $apply. $http remains
        // as a fallback for browsers without fetch.

        function dispatchEvents(data) {
            // Caller arranges digest context ($http: already in one; fetch:
            // wrapped in $apply by the caller).
            if (lastID > 0) {   // not emit events from first response
                data.forEach(function (event) {
                    if (debugEvents) {
                        console.log("event", event.id, event.type, event.data);
                    }
                    $rootScope.$broadcast(event.type, event);
                });
            }

            var lastEvent = data[data.length - 1];
            if (lastEvent) {
                lastID = lastEvent.id;
            }
        }

        // ---- fetch transport (throttle-immune) ------------------------------

        function fetchPoll(url) {
            // The CSRF middleware rejects any /rest/ request without a valid
            // token header, GETs included. Angular's $http adds it
            // automatically via its XSRF config; fetch does not, so set it
            // explicitly from the cookie. Missing header -> 403 -> the
            // handler below reloads the page, forever.
            var headers = {};
            var csrf = document.cookie.match(/(?:^|; )CSRF-Token-([^=]+)=([^;]+)/);
            if (csrf) {
                headers['X-CSRF-Token-' + csrf[1]] = csrf[2];
            }
            fetch(url, { credentials: 'same-origin', headers: headers })
                .then(function (response) {
                    if (response.status === 403) {
                        // Auth error - reload login page
                        location.reload();
                        return null;
                    }
                    if (!response.ok) {
                        throw new Error('events: HTTP ' + response.status);
                    }
                    // An empty body (Syncthing restarted mid-poll) makes
                    // response.json() throw, landing in fetchErrorFn below.
                    return response.json();
                })
                .then(function (data) {
                    if (data === null) {
                        return; // 403 already handled
                    }
                    if (!data) {
                        throw new Error('events: empty response');
                    }
                    $rootScope.$apply(function () {
                        $rootScope.$broadcast(self.ONLINE);
                        dispatchEvents(data);
                    });
                    fetchPoll(urlbase + '/events?since=' + lastID);
                })
                .catch(fetchErrorFn);
        }

        function fetchErrorFn(err) {
            $rootScope.$apply(function () {
                $rootScope.$broadcast(self.OFFLINE);
            });
            $timeout(function () {
                fetchPoll(urlbase + '/events?limit=1');
            }, 1000, false);
        }

        // ---- $http transport (legacy fallback) ------------------------------

        function successFn(data) {
            // When Syncthing restarts while the long polling connection is in
            // progress the browser on some platforms returns a 200 (since the
            // headers has been flushed with the return code 200), with no data.
            // This basically means that the connection has been reset, and the
            // call was not actually successful.
            if (!data) {
                errorFn(data);
                return;
            }
            $rootScope.$broadcast(self.ONLINE);

            dispatchEvents(data);

            $http.get(urlbase + '/events?since=' + lastID)
                .success(successFn)
                .error(errorFn);
        }

        function errorFn(statusString, status) {
            if (status === 403) {
                // Auth error - reload login page
                location.reload();
                return;
            }

            $rootScope.$broadcast(self.OFFLINE);

            $timeout(function () {
                $http.get(urlbase + '/events?limit=1')
                    .success(successFn)
                    .error(errorFn);
            }, 1000, false);
        }

        angular.extend(self, {
            // emitted by this

            ONLINE: 'UIOnline',
            OFFLINE: 'UIOffline',

            // emitted by syncthing process

            CONFIG_SAVED: 'ConfigSaved',   // Emitted after the config has been saved by the user or by Syncthing itself
            DEVICE_CONNECTED: 'DeviceConnected',   // Generated each time a connection to a device has been established
            DEVICE_DISCONNECTED: 'DeviceDisconnected',   // Generated each time a connection to a device has been terminated
            DEVICE_DISCOVERED: 'DeviceDiscovered',   // Emitted when a new device is discovered using local discovery
            DEVICE_REJECTED: 'DeviceRejected',   // DEPRECATED: Emitted when there is a connection from a device we are not configured to talk to
            PENDING_DEVICES_CHANGED: 'PendingDevicesChanged',   // Emitted when pending devices were added / updated (connection from unknown ID) or removed (device is ignored or added)
            DEVICE_PAUSED: 'DevicePaused',   // Emitted when a device has been paused
            DEVICE_RESUMED: 'DeviceResumed',   // Emitted when a device has been resumed
            CLUSTER_CONFIG_RECEIVED: 'ClusterConfigReceived',   // Emitted when receiving a remote device's cluster config
            DOWNLOAD_PROGRESS: 'DownloadProgress',   // Emitted during file downloads for each folder for each file
            FAILURE: 'Failure',   // Specific errors sent to the usage reporting server for diagnosis
            UPGRADE_RESTART_SCHEDULED: 'UpgradeRestartScheduled',   // Auto-upgrade completed, restart scheduled
            FOLDER_COMPLETION: 'FolderCompletion',   //Emitted when the local or remote contents for a folder changes
            FOLDER_REJECTED: 'FolderRejected',   // DEPRECATED: Emitted when a device sends index information for a folder we do not have, or have but do not share with the device in question
            PENDING_FOLDERS_CHANGED: 'PendingFoldersChanged',   // Emitted when pending folders were added / updated (offered by some device, but not shared to them) or removed (folder ignored or added or no longer offered from the remote device)
            FOLDER_SUMMARY: 'FolderSummary',   // Emitted when folder contents have changed locally
            ITEM_FINISHED: 'ItemFinished',   // Generated when Syncthing ends synchronizing a file to a newer version
            ITEM_STARTED: 'ItemStarted',   // Generated when Syncthing begins synchronizing a file to a newer version
            LISTEN_ADDRESSES_CHANGED: 'ListenAddressesChanged',   // Listen address resolution has changed.
            LOCAL_CHANGE_DETECTED: 'LocalChangeDetected',   // Generated upon scan whenever the local disk has discovered an updated file from the previous scan.
            LOCAL_INDEX_UPDATED: 'LocalIndexUpdated',   // Generated when the local index information has changed, due to synchronizing one or more items from the cluster or discovering local changes during a scan
            LOGIN_ATTEMPT: 'LoginAttempt',   // Emitted on every login attempt when authentication is enabled for the GUI.
            REMOTE_CHANGE_DETECTED: 'RemoteChangeDetected',   // Generated upon scan whenever a file is locally updated due to a remote change.
            REMOTE_DOWNLOAD_PROGRESS: 'RemoteDownloadProgress',   // DownloadProgress message received from a connected remote device.
            REMOTE_INDEX_UPDATED: 'RemoteIndexUpdated',   // Generated each time new index information is received from a device
            STARTING: 'Starting',   // Emitted exactly once, when Syncthing starts, before parsing configuration etc
            STARTUP_COMPLETED: 'StartupCompleted',   // Emitted exactly once, when initialization is complete and Syncthing is ready to start exchanging data with other devices
            STATE_CHANGED: 'StateChanged',   // Emitted when a folder changes state
            FOLDER_ERRORS: 'FolderErrors',   // Emitted when a folder has errors preventing a full sync
            FOLDER_WATCH_STATE_CHANGED: 'FolderWatchStateChanged',   // Watcher routine encountered a new error, or a previous error disappeared after retrying.
            FOLDER_SCAN_PROGRESS: 'FolderScanProgress',   // Emitted every ScanProgressIntervalS seconds, indicating how far into the scan it is at.
            FOLDER_PAUSED: 'FolderPaused',   // Emitted when a folder is paused
            FOLDER_RESUMED: 'FolderResumed',   // Emitted when a folder is resumed

            start: function () {
                if (typeof fetch === 'function') {
                    fetchPoll(urlbase + '/events?limit=1');
                } else {
                    $http.get(urlbase + '/events?limit=1')
                        .success(successFn)
                        .error(errorFn);
                }
            }
        });
    }]);

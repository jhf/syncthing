# PR: gui: let folders fetch their own event streams + notify robustness

Branch: upstream/gui-fetch-events (2 commits on main)

Two independent GUI fixes in one small PR:

## 1. A folder's event source catches up with server-side history on refresh

Previously, refreshing the browser window reset the folder detail view's event
stream to "now", so the local-changes history shown in the modal was empty
until new events arrived. With this change the view asks the server for the
history it already has (`LocalChangeDetected` events are kept in the event
buffer anyway), so the modal is populated immediately after a refresh.

## 2. Harden `notifyListeners` in the event service

Under load, one listener's blocked channel could stall the whole event
pipeline. Notify non-blocking and drop for that listener instead; the event
stream stays live for everyone else.

Tests: `lib/api/api_guievents_test.go`.

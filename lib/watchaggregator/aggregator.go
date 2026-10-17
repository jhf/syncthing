// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package watchaggregator

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
)

// Not meant to be changed, but must be changeable for tests
var (
	maxFiles       = 512
	maxFilesPerDir = 128
	// When the per-window total overflows, the tracked paths collapse to
	// their common prefixes: any prefix covering more than
	// collapseClusterThreshold tracked paths is scanned as one subtree,
	// sparser paths stay precise. No cap on the result is needed beyond
	// strict shrinking: the emitted set is bounded by maxFiles tracked
	// paths, and its scan cost is bounded by the union of the touched
	// subtrees, which never exceeds a full walk. A burst of thousands of
	// files in one directory collapses to that one subtree.
	collapseClusterThreshold = 16
)

// aggregatedEvent represents potentially multiple events at and/or recursively
// below one path until it times out and a scan is scheduled.
// If it represents multiple events and there are events of both Remove and
// NonRemove types, the evType attribute is Mixed (as returned by fs.Event.Merge).
type aggregatedEvent struct {
	firstModTime time.Time
	lastModTime  time.Time
	evType       fs.EventType
}

// Stores pointers to both aggregated events directly within this directory and
// child directories recursively containing aggregated events themselves.
type eventDir struct {
	events map[string]*aggregatedEvent
	dirs   map[string]*eventDir
}

func newEventDir() *eventDir {
	return &eventDir{
		events: make(map[string]*aggregatedEvent),
		dirs:   make(map[string]*eventDir),
	}
}

func (dir *eventDir) childCount() int {
	return len(dir.events) + len(dir.dirs)
}

func (dir *eventDir) firstModTime() time.Time {
	if dir.childCount() == 0 {
		panic("bug: firstModTime must not be used on empty eventDir")
	}
	firstModTime := time.Now()
	for _, childDir := range dir.dirs {
		dirTime := childDir.firstModTime()
		if dirTime.Before(firstModTime) {
			firstModTime = dirTime
		}
	}
	for _, event := range dir.events {
		if event.firstModTime.Before(firstModTime) {
			firstModTime = event.firstModTime
		}
	}
	return firstModTime
}

func (dir *eventDir) eventType() fs.EventType {
	if dir.childCount() == 0 {
		panic("bug: eventType must not be used on empty eventDir")
	}
	var evType fs.EventType
	for _, childDir := range dir.dirs {
		evType |= childDir.eventType()
		if evType == fs.Mixed {
			return fs.Mixed
		}
	}
	for _, event := range dir.events {
		evType |= event.evType
		if evType == fs.Mixed {
			return fs.Mixed
		}
	}
	return evType
}

// Batch is one flushed set of watched paths scheduled for scanning.
type Batch struct {
	Paths []string
	// MaxEventID is the highest backend journal ID among all events seen
	// up to this flush (zero when the backend has no journal concept). It
	// is attached only to the last sub-batch of a flush - a flush is split
	// into up to three batches (non-removes, mixed, removes, in that
	// order) - so persisting it after that batch's scan implies every
	// event it covers has been scanned. Monotonic for the lifetime of the
	// aggregator.
	MaxEventID uint64
}

type aggregator struct {
	// folderID never changes and is accessed in CommitConfiguration, which
	// asynchronously updates folderCfg -> can't use folderCfg.ID (racy)
	folderID        string
	folderCfg       config.FolderConfiguration
	folderCfgUpdate chan config.FolderConfiguration
	// Time after which an event is scheduled for scanning when no modifications occur.
	notifyDelay time.Duration
	// Time after which an event is scheduled for scanning even though modifications occur.
	notifyTimeout         time.Duration
	notifyTimer           *time.Timer
	notifyTimerNeedsReset bool
	notifyTimerResetChan  chan time.Duration
	counts                eventCounter
	root                  *eventDir
	ctx                   context.Context
	// maxEventID tracks the highest backend journal ID seen, including
	// events that are later dropped from batches: drops happen because the
	// change is covered anyway (a parent or "." is scheduled, or we made
	// the change ourselves), so the watermark may advance over them.
	maxEventID uint64
}

type eventCounter struct {
	removes    int // Includes mixed events.
	nonRemoves int
}

func (c *eventCounter) add(typ fs.EventType, n int) {
	if typ&fs.Remove != 0 {
		c.removes += n
	} else {
		c.nonRemoves += n
	}
}

func (c *eventCounter) total() int { return c.removes + c.nonRemoves }

func newAggregator(ctx context.Context, folderCfg config.FolderConfiguration) *aggregator {
	a := &aggregator{
		folderID:              folderCfg.ID,
		folderCfgUpdate:       make(chan config.FolderConfiguration),
		notifyTimerNeedsReset: false,
		notifyTimerResetChan:  make(chan time.Duration),
		root:                  newEventDir(),
		ctx:                   ctx,
	}

	a.updateConfig(folderCfg)

	return a
}

func Aggregate(ctx context.Context, in <-chan fs.Event, out chan<- Batch, folderCfg config.FolderConfiguration, cfg config.Wrapper, evLogger events.Logger) {
	AggregateWithFlush(ctx, in, out, nil, folderCfg, cfg, evLogger)
}

// AggregateWithFlush is Aggregate with a flush channel: sending an ack
// channel forces all pending events to be emitted as batches immediately,
// without waiting out the batching delay; the ack is closed once the batches
// have been handed off for sending. Used by the btrfs scan replay shutdown
// sequence, which must drain the watch completely before pairing a snapshot
// with the index. A nil flush channel disables the feature.
func AggregateWithFlush(ctx context.Context, in <-chan fs.Event, out chan<- Batch, flush <-chan chan struct{}, folderCfg config.FolderConfiguration, cfg config.Wrapper, evLogger events.Logger) {
	a := newAggregator(ctx, folderCfg)

	// Necessary for unit tests where the backend is mocked
	go a.mainLoop(in, out, flush, cfg, evLogger)
}

func (a *aggregator) mainLoop(in <-chan fs.Event, out chan<- Batch, flush <-chan chan struct{}, cfg config.Wrapper, evLogger events.Logger) {
	a.notifyTimer = time.NewTimer(a.notifyDelay)
	defer a.notifyTimer.Stop()

	inProgressItemSubscription := evLogger.Subscribe(events.ItemStarted | events.ItemFinished)
	defer inProgressItemSubscription.Unsubscribe()

	cfg.Subscribe(a)
	defer cfg.Unsubscribe(a)

	inProgress := make(map[string]struct{})

	for {
		select {
		case event := <-in:
			a.newEvent(event, inProgress)
		case event, ok := <-inProgressItemSubscription.C():
			if ok {
				updateInProgressSet(event, inProgress)
			}
		case <-a.notifyTimer.C:
			a.actOnTimer(out)
		case ack := <-flush:
			a.flushAll(out)
			close(ack)
		case interval := <-a.notifyTimerResetChan:
			a.resetNotifyTimer(interval)
		case folderCfg := <-a.folderCfgUpdate:
			a.updateConfig(folderCfg)
		case <-a.ctx.Done():
			l.Debugln(a, "Stopped")
			return
		}
	}
}

func (a *aggregator) newEvent(event fs.Event, inProgress map[string]struct{}) {
	if event.ID > a.maxEventID {
		a.maxEventID = event.ID
	}
	if _, ok := a.root.events["."]; ok {
		l.Debugln(a, "Will scan entire folder anyway; dropping:", event.Name)
		return
	}
	if _, ok := inProgress[event.Name]; ok {
		l.Debugln(a, "Skipping path we modified:", event.Name)
		return
	}
	a.aggregateEvent(event, time.Now())
}

func (a *aggregator) aggregateEvent(event fs.Event, evTime time.Time) {
	if event.Name != "." && a.counts.total() == maxFiles {
		// Too many distinct paths in this window. Nothing was lost -
		// every event is in the tree - so instead of escalating to a
		// full-tree scan, factor the tracked paths into their common
		// prefixes and scan only those subtrees: a burst under
		// .bun/install/cache scans that directory, not the root.
		// Measured on a 2.35M-file home folder: bursts of tens of
		// thousands of events from one build or package install each
		// flattened to a full scan of the whole tree, roughly once an
		// hour. A genuine tree-wide churn still escalates to the root.
		if paths := a.collapseToCommonPrefixes(event, evTime); paths != nil {
			l.Debugf("%v Too many paths (%d), scanning %d affected subtrees instead of the entire folder", a, maxFiles, len(paths))
			return
		}
	}
	if event.Name == "." || a.counts.total() == maxFiles {
		l.Debugln(a, "Scan entire folder")
		firstModTime := evTime
		if a.root.childCount() != 0 {
			event.Type = event.Type.Merge(a.root.eventType())
			firstModTime = a.root.firstModTime()
		}
		a.root.dirs = make(map[string]*eventDir)
		a.root.events = make(map[string]*aggregatedEvent)
		a.root.events["."] = &aggregatedEvent{
			firstModTime: firstModTime,
			lastModTime:  evTime,
			evType:       event.Type,
		}
		a.counts = eventCounter{}
		a.counts.add(event.Type, 1)
		a.resetNotifyTimerIfNeeded()
		return
	}

	parentDir := a.root

	// Check if any parent directory is already tracked or will exceed
	// events per directory limit bottom up
	pathSegments := strings.Split(filepath.ToSlash(event.Name), "/")

	// As root dir cannot be further aggregated, allow up to maxFiles
	// children.
	localMaxFilesPerDir := maxFiles
	var currPath string
	for i, name := range pathSegments[:len(pathSegments)-1] {
		currPath = filepath.Join(currPath, name)

		if ev, ok := parentDir.events[name]; ok {
			ev.lastModTime = evTime
			if merged := event.Type.Merge(ev.evType); ev.evType != merged {
				a.counts.add(ev.evType, -1)
				a.counts.add(merged, 1)
				ev.evType = merged
			}
			l.Debugf("%v Parent %s (type %s) already tracked: %s", a, currPath, ev.evType, event.Name)
			return
		}

		if parentDir.childCount() == localMaxFilesPerDir {
			l.Debugf("%v Parent dir %s already has %d children, tracking it instead: %s", a, currPath, localMaxFilesPerDir, event.Name)
			event.Name = filepath.Dir(currPath)
			a.aggregateEvent(event, evTime)
			return
		}

		// If there are no events below path, but we need to recurse
		// into that path, create eventDir at path.
		if newParent, ok := parentDir.dirs[name]; ok {
			parentDir = newParent
		} else {
			l.Debugln(a, "Creating eventDir at:", currPath)
			newParent = newEventDir()
			parentDir.dirs[name] = newParent
			parentDir = newParent
		}

		// Reset allowed children count to maxFilesPerDir for non-root
		if i == 0 {
			localMaxFilesPerDir = maxFilesPerDir
		}
	}

	name := pathSegments[len(pathSegments)-1]

	if ev, ok := parentDir.events[name]; ok {
		ev.lastModTime = evTime
		if merged := event.Type.Merge(ev.evType); ev.evType != merged {
			a.counts.add(ev.evType, -1)
			a.counts.add(merged, 1)
			ev.evType = merged
		}
		l.Debugf("%v Already tracked (type %v): %s", a, ev.evType, event.Name)
		return
	}

	childDir, ok := parentDir.dirs[name]

	// If a dir existed at path, it would be removed from dirs, thus
	// childCount would not increase.
	if !ok && parentDir.childCount() == localMaxFilesPerDir {
		l.Debugf("%v Parent dir already has %d children, tracking it instead: %s", a, localMaxFilesPerDir, event.Name)
		event.Name = filepath.Dir(event.Name)
		a.aggregateEvent(event, evTime)
		return
	}

	firstModTime := evTime
	if ok {
		firstModTime = childDir.firstModTime()
		if merged := event.Type.Merge(childDir.eventType()); event.Type != merged {
			a.counts.add(event.Type, -1)
			event.Type = merged
		}
		delete(parentDir.dirs, name)
	}
	l.Debugf("%v Tracking (type %v): %s", a, event.Type, event.Name)
	parentDir.events[name] = &aggregatedEvent{
		firstModTime: firstModTime,
		lastModTime:  evTime,
		evType:       event.Type,
	}
	a.counts.add(event.Type, 1)
	a.resetNotifyTimerIfNeeded()
}

func (a *aggregator) resetNotifyTimerIfNeeded() {
	if a.notifyTimerNeedsReset {
		a.resetNotifyTimer(a.notifyDelay)
	}
}

// collapseToCommonPrefixes factors the tracked events into their common
// prefixes: any prefix covering more than collapseClusterThreshold tracked
// paths becomes one subtree-scan event, sparser paths stay as they are.
// The incoming event joins the set first. The batch the aggregator then
// emits names exactly the subtrees that saw events, and the scan covers
// every event ever received: unlike a dropped-events flag, the aggregation
// limit loses no information.
//
// Returns nil without touching the tree only when the collapse would not
// strictly shrink the tracked set below maxFiles - the overflow check fires
// on equality, so accepting a non-shrinking collapse would leave the total
// at the limit and never escalate again. Scanning the emitted set costs at
// most a walk of the touched subtrees, never more than a full scan, so no
// path-count cap is needed beyond the shrink.
func (a *aggregator) collapseToCommonPrefixes(event fs.Event, evTime time.Time) []string {
	type pathEvent struct {
		firstModTime time.Time
		lastModTime  time.Time
		evType       fs.EventType
	}
	var tracked []pathEvent
	var names []string
	var collect func(dir *eventDir, prefix string)
	collect = func(dir *eventDir, prefix string) {
		for name, sub := range dir.dirs {
			collect(sub, filepath.Join(prefix, name))
		}
		for name, ev := range dir.events {
			if name == "." {
				names = append(names, ".")
				continue
			}
			names = append(names, filepath.Join(prefix, name))
			tracked = append(tracked, pathEvent{firstModTime: ev.firstModTime, lastModTime: ev.lastModTime, evType: ev.evType})
		}
	}
	collect(a.root, "")
	for _, name := range names {
		if name == "." {
			return nil // already escalated to the root; keep it
		}
	}
	names = append(names, event.Name)
	tracked = append(tracked, pathEvent{firstModTime: evTime, lastModTime: evTime, evType: event.Type})

	// Factor the paths into a prefix trie.
	type trieNode struct {
		children map[string]*trieNode
		terminal *pathEvent // a tracked path ends exactly here
	}
	root := &trieNode{}
	for i, name := range names {
		node := root
		for _, seg := range strings.Split(filepath.ToSlash(name), "/") {
			if node.children == nil {
				node.children = make(map[string]*trieNode)
			}
			child, ok := node.children[seg]
			if !ok {
				child = &trieNode{}
				node.children[seg] = child
			}
			node = child
		}
		ev := tracked[i]
		if node.terminal != nil {
			node.terminal.evType = node.terminal.evType.Merge(ev.evType)
			node.terminal.lastModTime = ev.lastModTime
		} else {
			term := ev
			node.terminal = &term
		}
	}

	// Emit the factored paths bottom-up: a dense subtree becomes one
	// subtree-scan path, and a node collapses only when the paths
	// remaining below it still exceed the threshold - so the climb stops
	// at the least common prefix of each dense cluster and never reaches
	// the root for localized bursts.
	type emittedPath struct {
		name string
		ev   pathEvent
	}
	var emit func(node *trieNode, prefix string) []emittedPath
	emit = func(node *trieNode, prefix string) []emittedPath {
		var out []emittedPath
		for seg, child := range node.children {
			out = append(out, emit(child, filepath.Join(prefix, seg))...)
		}
		if node.terminal != nil {
			out = append(out, emittedPath{prefix, *node.terminal})
		}
		// The root itself never collapses; that is the flatten case.
		if prefix != "" && len(out) > collapseClusterThreshold {
			merged := pathEvent{firstModTime: evTime, lastModTime: evTime}
			for _, p := range out {
				merged.evType = merged.evType.Merge(p.ev.evType)
				if p.ev.firstModTime.Before(merged.firstModTime) {
					merged.firstModTime = p.ev.firstModTime
				}
			}
			return []emittedPath{{prefix, merged}}
		}
		return out
	}
	emitted := emit(root, "")

	if len(emitted) >= maxFiles {
		return nil
	}

	a.root.dirs = make(map[string]*eventDir)
	a.root.events = make(map[string]*aggregatedEvent)
	a.counts = eventCounter{}
	names = make([]string, 0, len(emitted))
	for _, p := range emitted {
		a.aggregateEvent(fs.Event{Name: p.name, Type: p.ev.evType}, p.ev.firstModTime)
		names = append(names, p.name)
	}
	return names
}

// resetNotifyTimer should only ever be called when notifyTimer has stopped
// and notifyTimer.C been read from. Otherwise, call resetNotifyTimerIfNeeded.
func (a *aggregator) resetNotifyTimer(duration time.Duration) {
	l.Debugln(a, "Resetting notifyTimer to", duration.String())
	a.notifyTimerNeedsReset = false
	a.notifyTimer.Reset(duration)
}

// flushAll emits every pending event regardless of age, for shutdown
// draining. Like actOnTimer it hands the batch off asynchronously.
func (a *aggregator) flushAll(out chan<- Batch) {
	c := a.counts.total()
	if c == 0 {
		return
	}
	oldEvents := make(map[string]*aggregatedEvent, c)
	a.popAllEventsTo(oldEvents, a.root, ".")
	if len(oldEvents) == 0 {
		return
	}
	go a.notify(oldEvents, out)
}

func (a *aggregator) popAllEventsTo(to map[string]*aggregatedEvent, dir *eventDir, dirPath string) {
	for childName, childDir := range dir.dirs {
		a.popAllEventsTo(to, childDir, filepath.Join(dirPath, childName))
		if childDir.childCount() == 0 {
			delete(dir.dirs, childName)
		}
	}
	for name, event := range dir.events {
		to[filepath.Join(dirPath, name)] = event
		delete(dir.events, name)
		a.counts.add(event.evType, -1)
	}
}

func (a *aggregator) actOnTimer(out chan<- Batch) {
	c := a.counts.total()
	if c == 0 {
		l.Debugln(a, "No tracked events, waiting for new event.")
		a.notifyTimerNeedsReset = true
		return
	}
	oldEvents := make(map[string]*aggregatedEvent, c)
	a.popOldEventsTo(oldEvents, a.root, ".", time.Now(), true)
	if a.notifyDelay != a.notifyTimeout && a.counts.nonRemoves == 0 && a.counts.removes != 0 {
		// Only delayed events remaining, no need to delay them additionally
		a.popOldEventsTo(oldEvents, a.root, ".", time.Now(), false)
	}
	if len(oldEvents) == 0 {
		l.Debugln(a, "No old fs events")
		a.resetNotifyTimer(a.notifyDelay)
		return
	}
	// Sending to channel might block for a long time, but we need to keep
	// reading from notify backend channel to avoid overflow
	go a.notify(oldEvents, out)
}

// Schedule scan for given events dispatching deletes last and reset notification
// afterwards to set up for the next scan scheduling.
func (a *aggregator) notify(oldEvents map[string]*aggregatedEvent, out chan<- Batch) {
	timeBeforeSending := time.Now()
	l.Debugf("%v Notifying about %d fs events", a, len(oldEvents))
	separatedBatches := make(map[fs.EventType][]string)
	for path, event := range oldEvents {
		separatedBatches[event.evType] = append(separatedBatches[event.evType], path)
	}
	batchTypes := [3]fs.EventType{fs.NonRemove, fs.Mixed, fs.Remove}
	// The watermark rides only on the last non-empty sub-batch: the
	// consumer persists it after that batch's scan, which implies the
	// earlier sub-batches of this flush were already scanned (they are
	// processed in order).
	lastNonEmpty := -1
	for i, evType := range batchTypes {
		if len(separatedBatches[evType]) != 0 {
			lastNonEmpty = i
		}
	}
	for i, evType := range batchTypes {
		currBatch := separatedBatches[evType]
		if len(currBatch) != 0 {
			b := Batch{Paths: currBatch}
			if i == lastNonEmpty {
				b.MaxEventID = a.maxEventID
			}
			select {
			case out <- b:
			case <-a.ctx.Done():
				return
			}
		}
	}
	// If sending to channel blocked for a long time,
	// shorten next notifyDelay accordingly.
	duration := time.Since(timeBeforeSending)
	buffer := time.Millisecond
	var nextDelay time.Duration
	switch {
	case duration < a.notifyDelay/10:
		nextDelay = a.notifyDelay
	case duration+buffer > a.notifyDelay:
		nextDelay = buffer
	default:
		nextDelay = a.notifyDelay - duration
	}
	select {
	case a.notifyTimerResetChan <- nextDelay:
	case <-a.ctx.Done():
	}
}

// popOldEvents finds events that should be scheduled for scanning recursively in dirs,
// removes those events and empty eventDirs and returns a map with all the removed
// events referenced by their filesystem path
func (a *aggregator) popOldEventsTo(to map[string]*aggregatedEvent, dir *eventDir, dirPath string, currTime time.Time, delayRem bool) {
	for childName, childDir := range dir.dirs {
		a.popOldEventsTo(to, childDir, filepath.Join(dirPath, childName), currTime, delayRem)
		if childDir.childCount() == 0 {
			delete(dir.dirs, childName)
		}
	}
	for name, event := range dir.events {
		if a.isOld(event, currTime, delayRem) {
			to[filepath.Join(dirPath, name)] = event
			delete(dir.events, name)
			a.counts.add(event.evType, -1)
		}
	}
}

func (a *aggregator) isOld(ev *aggregatedEvent, currTime time.Time, delayRem bool) bool {
	// Deletes should in general be scanned last, therefore they are delayed by
	// letting them time out. This behaviour is overridden by delayRem == false.
	// Refer to following comments as to why.
	// An event that has not registered any new modifications recently is scanned.
	// a.notifyDelay is the user facing value signifying the normal delay between
	// picking up a modification and scanning it. As scheduling scans happens at
	// regular intervals of a.notifyDelay the delay of a single event is not exactly
	// a.notifyDelay, but lies in the range of 0.5 to 1.5 times a.notifyDelay.
	if (!delayRem || ev.evType == fs.NonRemove) && 2*currTime.Sub(ev.lastModTime) > a.notifyDelay {
		return true
	}
	// When an event registers repeat modifications or involves removals it
	// is delayed to reduce resource usage, but after a certain time (notifyTimeout)
	// passed it is scanned anyway.
	// If only removals are remaining to be scanned, there is no point to delay
	// removals further, so this behaviour is overridden by delayRem == false.
	return currTime.Sub(ev.firstModTime) > a.notifyTimeout
}

func (a *aggregator) String() string {
	return fmt.Sprintf("aggregator/%s:", a.folderCfg.Description())
}

func (a *aggregator) CommitConfiguration(_, to config.Configuration) bool {
	for _, folderCfg := range to.Folders {
		if folderCfg.ID == a.folderID {
			select {
			case a.folderCfgUpdate <- folderCfg:
			case <-a.ctx.Done():
			}
			return true
		}
	}
	// Nothing to do, model will soon stop this
	return true
}

func (a *aggregator) updateConfig(folderCfg config.FolderConfiguration) {
	a.notifyDelay = time.Duration(folderCfg.FSWatcherDelayS * float64(time.Second))
	if maxDelay := folderCfg.FSWatcherTimeoutS; maxDelay > 0 {
		// FSWatcherTimeoutS is set explicitly so use that, but it also
		// can't be lower than FSWatcherDelayS
		a.notifyTimeout = time.Duration(max(maxDelay, folderCfg.FSWatcherDelayS) * float64(time.Second))
	} else {
		// Use the default FSWatcherTimeoutS calculation
		a.notifyTimeout = notifyTimeout(folderCfg.FSWatcherDelayS)
	}
	a.folderCfg = folderCfg
}

func updateInProgressSet(event events.Event, inProgress map[string]struct{}) {
	switch event.Type {
	case events.ItemStarted:
		path := event.Data.(map[string]string)["item"]
		inProgress[path] = struct{}{}
	case events.ItemFinished:
		path := event.Data.(map[string]any)["item"].(string)
		delete(inProgress, path)
	}
}

// Events that involve removals or continuously receive new modifications are
// delayed but must time out at some point. The following numbers come out of thin
// air, they were just considered as a sensible compromise between fast updates and
// saving resources. For short delays the timeout is 6 times the delay, capped at 1
// minute. For delays longer than 1 minute, the delay and timeout are equal.
func notifyTimeout(eventDelayS float64) time.Duration {
	const (
		shortDelayS             = 10
		shortDelayMultiplicator = 6
		longDelayS              = 60
		longDelayTimeout        = time.Minute
	)
	if eventDelayS < shortDelayS {
		return time.Duration(eventDelayS * shortDelayMultiplicator * float64(time.Second))
	}
	if eventDelayS < longDelayS {
		return longDelayTimeout
	}
	return time.Duration(eventDelayS * float64(time.Second))
}

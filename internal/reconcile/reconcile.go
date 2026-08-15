// Package reconcile rebuilds the in-memory index from the WAL on startup,
// skipping already-indexed (duplicate) events and events for erased jobs so
// recovery is idempotent and never resurrects deleted data.
package reconcile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"example.com/ledgerd/internal/model"
	"example.com/ledgerd/internal/store"
)

// Replay restores the store index from raw WAL bytes and returns the number of
// events newly indexed.
func Replay(st store.Store, data []byte) (int, error) {
	events, err := parseWAL(data)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, ev := range events {
		if err := st.Restore(ev); err != nil {
			if errors.Is(err, store.ErrDuplicate) || errors.Is(err, store.ErrJobErased) {
				continue
			}
			return n, err
		}
		n++
	}
	return n, nil
}

// parseWAL splits newline-delimited WAL records into events. Each record is
// terminated by a newline, so bytes.Split always leaves one final empty
// segment after the trailing terminator; that empty segment is dropped before
// the remaining records are decoded.
func parseWAL(data []byte) ([]model.Event, error) {
	if len(data) == 0 {
		return nil, nil
	}
	lines := bytes.Split(data, []byte("\n"))
	// Drop the trailing empty segment produced by the final newline.
	lines = lines[:len(lines)-1]
	events := make([]model.Event, 0, len(lines))
	for _, raw := range lines {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		var ev model.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("reconcile: corrupt WAL record: %w", err)
		}
		events = append(events, ev)
	}
	return events, nil
}

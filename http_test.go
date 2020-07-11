package exdgo

import (
	"testing"
	"time"
)

func TestHTTPFilter(t *testing.T) {
	minute, serr := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	if serr != nil {
		t.Fatalf("testing error: %v", serr)
	}
	lines, serr := HTTPFilter(ClientParam{
		APIKey: "demo",
	}, FilterParam{
		Exchange: "bitmex",
		Channels: []string{"orderBookL2"},
		Minute:   minute,
	})
	if serr != nil {
		t.Fatal(serr)
	}
	if len(lines) == 0 {
		t.Fatal("len(lines) == 0")
	}
}

func TestHTTPSnapshot(t *testing.T) {
	at, serr := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	if serr != nil {
		t.Fatalf("testing error: %v", serr)
	}
	ss, serr := HTTPSnapshot(ClientParam{
		APIKey: "demo",
	}, SnapshotParam{
		Exchange: "bitmex",
		Channels: []string{"orderBookL2"},
		At:       at,
	})
	if serr != nil {
		t.Fatal(serr)
	}
	if len(ss) == 0 {
		t.Fatal("len(ss) == 0")
	}
}

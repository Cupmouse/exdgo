package exdgo

import (
	"context"
	"testing"
	"time"
)

func TestDownload(t *testing.T) {
	cli := ClientParam{
		APIKey: "demo",
	}
	start, serr := time.Parse(time.RFC3339, "2020-01-01T00:00:10Z")
	if serr != nil {
		t.Fatalf("testing error: %v", serr)
	}
	end, serr := time.Parse(time.RFC3339, "2020-01-01T00:04:50Z")
	if serr != nil {
		t.Fatalf("testing error: %v", serr)
	}
	set := RawRequestParam{
		Filter: map[string][]string{
			"bitmex":   []string{"orderBookL2"},
			"bitfinex": []string{"trades_tBTCUSD"},
		},
		Start: start,
		End:   end,
	}
	req, serr := Raw(cli, set)
	if serr != nil {
		t.Fatal(serr)
	}
	shards, serr := req.downloadAllShards(context.Background(), downloadBatchSize)
	if serr != nil {
		t.Fatal(serr)
	}
	for _, shard := range shards {
		if len(shard) == 0 {
			t.Fatal("shard len 0")
		}
	}
	lines, serr := req.Download()
	if serr != nil {
		t.Fatal(serr)
	}
	if len(lines) == 0 {
		t.Fatal("lines len 0")
	}
}

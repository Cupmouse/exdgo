package exdgo

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func prepareRawRequest(t *testing.T) *RawRequest {
	cli := ClientParam{
		APIKey: "demo",
	}
	start, serr := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
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
	return req
}

func TestDownloadAllShards(t *testing.T) {
	req := prepareRawRequest(t)
	shards, serr := req.downloadAllShards(context.Background(), downloadBatchSize)
	if serr != nil {
		t.Fatal(serr)
	}
	for _, shard := range shards {
		if len(shard) == 0 {
			t.Fatal("shard len 0")
		}
	}
}

func TestRawDownloadAndStream(t *testing.T) {
	req := prepareRawRequest(t)

	lines, serr := req.Download()
	if serr != nil {
		t.Fatal(serr)
	}
	if len(lines) == 0 {
		t.Fatal("lines len 0")
	}
	itr, serr := req.Stream()
	if serr != nil {
		t.Fatal(serr)
	}
	defer itr.Close()
	i := 0
	for {
		line, ok, serr := itr.Next()
		if !ok {
			if serr != nil {
				t.Fatal(serr)
			}
			break
		}
		if *line.Channel != *lines[i].Channel {
			t.Fatal("channel differ")
		}
		if line.Exchange != lines[i].Exchange {
			t.Fatal("exchange differ")
		}
		if bytes.Compare(line.Message, lines[i].Message) != 0 {
			t.Fatal("message differ")
		}
		if line.Timestamp != lines[i].Timestamp {
			t.Fatal("timestamp differ")
		}
		if line.Type != lines[i].Type {
			t.Fatal("type differ")
		}
		i++
	}
	if len(lines) != i {
		t.Fatal("len(lines) != i")
	}
}

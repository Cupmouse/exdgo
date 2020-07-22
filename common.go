package exdgo

import (
	"regexp"
	"time"
)

var (
	regexName   = regexp.MustCompile("^[a-zA-Z0-9_]+$")
	regexAPIKey = regexp.MustCompile("^[A-Za-z0-9\\-_]+$")
)

const (
	urlAPI                  = "https://api.exchangedataset.cc/v1/"
	defaultBufferSize       = 20
	downloadBatchSize       = 20
	clientDefaultTimeout    = 30 * time.Second
	snapshotTopicSubscribed = "!subscribed"
)

// LineType is enum of Line Type.
//
// Line Type shows what type of a line is, such as message line or start line.
//
// Lines with different types contain different information and have to be treated accordingly.
type LineType string

const (
	// LineTypeMessage is a one of the LineTypes.
	//
	// Message of line of this type contains message sent from exchanges' server.
	// This is the most usual LineType.
	LineTypeMessage LineType = "msg"
	// LineTypeSend is a one of the LineTypes.
	//
	// Message of line of this type contains message sent from one of our client when recording.
	LineTypeSend LineType = "send"
	// LineTypeStart is a one of the LineTypes.
	//
	// Line of this type indicates the first line in the continuous recording.
	LineTypeStart LineType = "start"
	// LineTypeEnd is a one of the LineTypes.
	//
	// Line of this type indicates the end line in the continuous recording.
	LineTypeEnd LineType = "end"
	// LineTypeError is a one of the LineTypes.
	//
	// Message of line of this type contains an error during recording.
	// Used in both server-side (exchanges' server) error and client-side (our clients which receive WebSocket data) error.
	LineTypeError LineType = "err"
)

// StructLine is the line which have a struct as a message.
// See `StringLine`.
type StructLine struct {
	Exchange  string
	Type      LineType
	Timestamp int
	Channel   *string
	Message   interface{}
}

// StringLine is the data structure of a single line from a response.
//
// `exchange`, `type` and `timestamp` is always present, **but `channel` or `message` is not.**
// This is because with certain `type`, a line might not contain `channel` or `message`, or both.
type StringLine struct {
	// Name of exchange from which this line is recorded.
	Exchange string
	// If `type === LineType.MESSAGE`, then a line is a normal message.
	// All of value are present.
	// You can get an assosiated channel by `channel`, and its message by `message`.
	//
	// If `type === LineType.SEND`, then a line is a request server sent when the dataset was
	// recorded.
	// All of value are present, though you can ignore this line.
	//
	// If `type === LineType.START`, then a line marks the start of new continuous recording of the
	// dataset.
	// Only `channel` is not present. `message` is the URL which used to record the dataset.
	// You might want to initialize your work since this essentially means new connection to
	// exchange's API.
	//
	// If `type === LineType.END`, then a line marks the end of continuous recording of the dataset.
	// Other than `type` and `timestamp` are not present.
	// You might want to perform some functionality when the connection to exchange's API was lost.
	//
	// If `type === LineType.ERROR`, then a line contains error message when recording the dataset.
	// Only `channel` is not present. `message` is the error message.
	// You want to ignore this line.
	Type LineType
	// Timestamp in nano seconds of this line was recorded.
	//
	// Timestamp is in unixtime-compatible format (unixtime * 10^9 + nanosec-part).
	// Timezone is UTC.
	Timestamp int64
	// Channel name which this line is associated with.
	// Could be `nil` according to `type`.
	Channel *string
	// Message.
	// Could be `nil` accoring to `type`.
	Message []byte
}

func convertTimeToMinute(tm time.Time) time.Time {
	return tm.Truncate(time.Minute)
}

package integration

type rawQueryResults struct {
	Raw []string `json:"raw"`
}

type bucket struct {
	StartAtNanos int64 `json:"startAtNanos"` // Start time of the bucket in nanoseconds
	Value        int   `json:"value"`        // Count
}

type timeBucketQueryResults struct {
	Granularity int64    `json:"granularity"`
	Buckets     []bucket `json:"buckets"`
}

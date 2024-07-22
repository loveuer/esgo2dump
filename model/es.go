package model

type ESSource struct {
	DocId   string         `json:"_id"`
	Index   string         `json:"_index"`
	Content map[string]any `json:"_source"`
	Sort    []any          `json:"sort"`
}

type ESResponseV6 struct {
	ScrollId string `json:"_scroll_id"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total    int         `json:"total"`
		MaxScore float64     `json:"max_score"`
		Hits     []*ESSource `json:"hits"`
	} `json:"hits"`
}

type ESResponseV7 struct {
	ScrollId string `json:"_scroll_id"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64     `json:"max_score"`
		Hits     []*ESSource `json:"hits"`
	} `json:"hits"`
}

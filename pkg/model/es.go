package model

type ESSource[T any] struct {
	DocId   string `json:"_id"`
	Index   string `json:"_index"`
	Content T      `json:"_source"`
	Sort    []any  `json:"sort"`
}

type ESResponseV6[T any] struct {
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
		Total    int            `json:"total"`
		MaxScore float64        `json:"max_score"`
		Hits     []*ESSource[T] `json:"hits"`
	} `json:"hits"`
}

type ESResponseV7[T any] struct {
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
		MaxScore float64        `json:"max_score"`
		Hits     []*ESSource[T] `json:"hits"`
	} `json:"hits"`
}

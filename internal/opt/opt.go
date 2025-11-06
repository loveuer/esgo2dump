package opt

type args struct {
	Version    bool
	Input      string
	Output     string
	Limit      int
	Max        int
	Type       string
	Timeout    int
	Field      string
	Sort       string
	Query      string
	QueryFile  string
	SplitLimit int
}

type config struct {
	Debug       bool `json:"-"`
	Dev         bool `json:"-"`
	DisablePing bool `json:"-"`
	Args        args `json:"-"`
}

var Cfg = &config{}

package opt

type args struct {
	Version   bool
	Input     string
	Output    string
	Limit     int
	Max       int
	Type      string
	Timeout   int
	Source    string
	Sort      string
	Query     string
	QueryFile string
}

type config struct {
	Debug bool `json:"-"`
	Dev   bool `json:"-"`
	Args  args `json:"-"`
}

var Cfg = &config{}

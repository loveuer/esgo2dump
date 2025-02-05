package cmd

import "time"

func init() {
	time.Local = time.FixedZone("CST", 8*3600)

	initRoot()
}

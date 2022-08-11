package main

import (
	"./wk"
	//_ "net/http/pprof"
)

func main() {
	wk.Init()
	if wk.L_PORT == 8002 {
		wk.Run_collector(wk.L_PORT)
	} else {
		wk.Run_processor(wk.L_PORT)
	}
	select {}
}

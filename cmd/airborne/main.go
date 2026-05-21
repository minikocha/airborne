package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/minikocha/airborne"
)

func main() {
	// TODO: 既存のファイルを上書きする・しないを選択できるようにする: flag.Bool("overwrite"...)
	versionFlag := flag.Bool("version", false, "show version")
	flag.Parse()
	if *versionFlag {
		fmt.Printf("versin: %s\n", airborne.Version())
		return
	}

	// TODO: 関係のない引数が渡された時にエラーにする

	a := airborne.NewAirborne()
	if err := a.LoadEnvVars(); err != nil {
		log.Fatal(err)
	}

	if err := a.WriteAll(); err != nil {
		log.Fatal(err)
	}
}

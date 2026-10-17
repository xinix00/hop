//go:build !tamago

// Host-stub: de echte main (main.go) is tamago-only — applib bestaat alleen
// onder GOOS=tamago. Deze stub houdt `go build ./...` en `go test ./...` op
// de host groen (zelfde patroon als hopprom's cmd/hopprom-hopos).
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "hopdns-hopos is een HopOS-slot-image; bouw met de tamago-toolchain (zie release.sh) of gebruik cmd/hopdns op een gewoon OS")
	os.Exit(1)
}

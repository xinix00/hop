//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cpuTempMilliC leest de CPU-temperatuur van de node uit sysfs, in
// milligraden Celsius. Eén getal per node, en dat is bewust de HEETSTE zone:
// wie meerdere sensoren heeft wil weten wanneer er érgens iets fout gaat, en
// dat is per definitie het maximum. 0 = geen sensor (VM's, containers).
func cpuTempMilliC() int {
	zones, _ := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	max := 0
	for _, z := range zones {
		b, err := os.ReadFile(z)
		if err != nil {
			continue
		}
		if v, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && v > max {
			max = v
		}
	}
	return max
}

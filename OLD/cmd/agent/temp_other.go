//go:build !linux

package main

// cpuTempMilliC: buiten Linux is er geen draagbare sensorweg die de moeite
// waard is (macOS eist SMC-entitlements) — 0 = onbekend, en de leader laat
// het veld dan gewoon weg.
func cpuTempMilliC() int { return 0 }

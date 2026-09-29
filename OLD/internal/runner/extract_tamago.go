//go:build tamago

// De tamago-kant van het uitpakken: er is niets uit te pakken. Een HopOS-node
// draait één ELF per slot en streamt die zelf naar zijn partitie; een tar of
// zip komt daar niet voor. De functies bestaan alléén zodat download.go één
// vorm heeft op elk platform — wie ze tóch aanroept krijgt een melding die
// zegt wat er aan de hand is, in plaats van een uitpakker die 90 KB
// tar/zip/gzip in het image legt om nooit te lopen.
//
// Zie extract_host.go voor de echte implementatie en waarom de scheiding er is.

package runner

import "errors"

// errNoExtract is het antwoord op elke uitpak-vraag op bare metal.
var errNoExtract = errors.New("artifact extraction is not available on this platform: a HopOS node runs a raw ELF image per slot, so drop the job's `extract` field")

func extractTarGz(string, string) error  { return errNoExtract }
func extractTarBz2(string, string) error { return errNoExtract }
func extractZip(string, string) error    { return errNoExtract }

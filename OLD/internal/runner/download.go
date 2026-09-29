package runner

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/xinix00/hop/internal/types"
)

// downloadArtifact downloads and extracts an artifact to destDir
func downloadArtifact(artifact *types.Artifact, destDir string) error {
	if artifact == nil || artifact.URL == "" {
		return nil // No artifact to download
	}

	// Parse URL scheme
	u, err := url.Parse(artifact.URL)
	if err != nil {
		return fmt.Errorf("invalid artifact URL: %w", err)
	}

	// Determine if extraction is needed
	shouldExtract := artifact.Extract != ""
	var tempFile string

	if shouldExtract {
		tempFile = filepath.Join(destDir, ".download."+artifact.Extract)
	} else {
		// Raw file: use filename override or basename from URL
		name := artifact.Filename
		if name == "" {
			name = filepath.Base(u.Path)
		}
		tempFile = filepath.Join(destDir, name)
	}

	if shouldExtract {
		defer os.Remove(tempFile)
	}

	// Route to appropriate downloader based on scheme
	switch u.Scheme {
	case "http", "https":
		if err := downloadHTTP(artifact, tempFile); err != nil {
			return fmt.Errorf("HTTP download failed: %w", err)
		}
	case "s3":
		if err := downloadS3(artifact, tempFile); err != nil {
			return fmt.Errorf("S3 download failed: %w", err)
		}
	default:
		return fmt.Errorf("unsupported URL scheme: %s (use http://, https://, or s3://)", u.Scheme)
	}

	// Extract if needed
	if shouldExtract {
		switch artifact.Extract {
		case "tar.gz", "tgz":
			if err := extractTarGz(tempFile, destDir); err != nil {
				return fmt.Errorf("extract tar.gz failed: %w", err)
			}
		case "tar.bz2", "tbz2":
			if err := extractTarBz2(tempFile, destDir); err != nil {
				return fmt.Errorf("extract tar.bz2 failed: %w", err)
			}
		case "zip":
			if err := extractZip(tempFile, destDir); err != nil {
				return fmt.Errorf("extract zip failed: %w", err)
			}
		default:
			return fmt.Errorf("unsupported extract type: %s (use tar.gz, tar.bz2, or zip)", artifact.Extract)
		}
	} else {
		// Raw file: make executable
		if err := os.Chmod(tempFile, 0755); err != nil {
			return fmt.Errorf("chmod failed: %w", err)
		}
	}

	return nil
}

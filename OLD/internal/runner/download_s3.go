package runner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// downloadS3 downloads from S3-compatible URL.
// URL format: s3://host/path (e.g., s3://haas-builds.fsn1.your-objectstorage.com/dir/file.tar.gz)
// Auth: access_key, secret_key, region (optional, defaults to us-east-1)
func downloadS3(artifact *types.Artifact, destPath string) error {
	u, err := url.Parse(artifact.URL)
	if err != nil {
		return fmt.Errorf("invalid S3 URL: %w", err)
	}

	host := u.Host
	key := strings.TrimPrefix(u.Path, "/")
	region := artifact.Auth["region"]
	accessKey := artifact.Auth["access_key"]
	secretKey := artifact.Auth["secret_key"]

	if region == "" {
		region = "us-east-1"
	}
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("S3 requires access_key and secret_key in auth")
	}

	// Build signed HTTPS URL
	httpsURL, headers := signS3GetRequest(host, key, region, accessKey, secretKey)

	signedArtifact := &types.Artifact{
		URL:     httpsURL,
		Headers: headers,
	}

	return downloadHTTP(signedArtifact, destPath)
}

// signS3GetRequest creates AWS Signature Version 4 for S3 GET request
func signS3GetRequest(host, key, region, accessKey, secretKey string) (string, map[string]string) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	httpsURL := fmt.Sprintf("https://%s/%s", host, key)

	// Canonical request
	canonicalURI := "/" + key
	canonicalQueryString := ""
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", host, amzDate)
	signedHeaders := "host;x-amz-date"
	payloadHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256 of empty string

	canonicalRequest := fmt.Sprintf("GET\n%s\n%s\n%s\n%s\n%s",
		canonicalURI, canonicalQueryString, canonicalHeaders, signedHeaders, payloadHash)

	// String to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, region)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm, amzDate, credentialScope, sha256hex(canonicalRequest))

	// Signing key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	signingKey := hmacSHA256(kService, []byte("aws4_request"))

	// Signature
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	authorization := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, accessKey, credentialScope, signedHeaders, signature)

	headers := map[string]string{
		"Authorization":        authorization,
		"x-amz-date":           amzDate,
		"x-amz-content-sha256": payloadHash,
	}

	return httpsURL, headers
}

func sha256hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

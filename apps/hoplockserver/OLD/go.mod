module github.com/xinix00/hoplockserver

go 1.26

require (
	github.com/xinix00/hoplock v0.4.1
	github.com/xinix00/lean v0.7.0
)

// Temporary: v0.3.0 does not yet carry leanhttp's BodyReader/BodyLen fields.
// Drop this once lean is tagged with them.

package engine

import "net/http"

// UserInfo represents an authenticated user extracted from a request.
type UserInfo struct {
	ID          string // unique user/worker ID (UUID or token label)
	DisplayName string
	IsAdmin     bool
}

// AuthExtractor extracts user identity from an HTTP request.
// Implementations can use reva context, JWT, basic auth, etc.
type AuthExtractor interface {
	ExtractUser(r *http.Request) (*UserInfo, bool)
}

// AuthExtractorFunc is a convenience adapter for simple extraction functions.
type AuthExtractorFunc func(r *http.Request) (*UserInfo, bool)

func (f AuthExtractorFunc) ExtractUser(r *http.Request) (*UserInfo, bool) {
	return f(r)
}

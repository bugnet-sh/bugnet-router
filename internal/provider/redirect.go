package provider

import "net/http"

// NoRedirect hands a 3xx answer back as it is, so a sandbox can never point a client elsewhere
func NoRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

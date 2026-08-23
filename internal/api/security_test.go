package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAuthMiddleware_ConstantTime verifies that a wrong token is rejected
// with 401 and that a correct token is accepted. The point is that the
// comparison uses constant-time logic (crypto/subtle), which we can't
// observe directly, but we can verify correct vs. incorrect behavior.
func TestAuthMiddleware_ConstantTime(t *testing.T) {
	token := "super-secret-token-12345"
	mw := AuthMiddleware(token)

	tests := []struct {
		name      string
		headerKey string
		headerVal string
		wantAuth  bool
	}{
		{"correct X-API-Key", "X-API-Key", token, true},
		{"correct Bearer", "Authorization", "Bearer " + token, true},
		{"wrong token", "X-API-Key", "wrong-token", false},
		{"empty token", "X-API-Key", "", false},
		{"partial prefix", "X-API-Key", "super-secret", false},
		{"Bearer prefix only", "Authorization", "Bearer ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			})

			srv := httptest.NewServer(mw(next))
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			if tt.headerVal != "" {
				req.Header.Set(tt.headerKey, tt.headerVal)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()

			if tt.wantAuth {
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d, want 200", resp.StatusCode)
				}
				if !called {
					t.Fatal("handler should have been called")
				}
			} else {
				if resp.StatusCode != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", resp.StatusCode)
				}
				if called {
					t.Fatal("handler should NOT have been called")
				}
			}
		})
	}
}

// TestBasicAuthMiddleware verifies HTTP Basic Auth for the dashboard:
// correct credentials pass, wrong/missing credentials get 401 with a
// WWW-Authenticate challenge header so the browser shows a login prompt.
func TestBasicAuthMiddleware(t *testing.T) {
	user := "admin"
	pass := "s3cret-dashboard"
	mw := BasicAuthMiddleware(user, pass)

	tests := []struct {
		name     string
		user     string
		pass     string
		setBasic bool
		wantAuth bool
	}{
		{"correct credentials", user, pass, true, true},
		{"wrong password", user, "wrong", true, false},
		{"wrong username", "other", pass, true, false},
		{"no auth header", "", "", false, false},
		{"empty password", user, "", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			})

			srv := httptest.NewServer(mw(next))
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
			if tt.setBasic {
				req.SetBasicAuth(tt.user, tt.pass)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()

			if tt.wantAuth {
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status = %d, want 200", resp.StatusCode)
				}
				if !called {
					t.Fatal("handler should have been called")
				}
			} else {
				if resp.StatusCode != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", resp.StatusCode)
				}
				if called {
					t.Fatal("handler should NOT have been called")
				}
				// Browser needs WWW-Authenticate to show the login prompt.
				if resp.Header.Get("WWW-Authenticate") == "" {
					t.Fatal("expected WWW-Authenticate header on 401")
				}
			}
		})
	}
}

// TestBasicAuthMiddleware_EmptyUserPassthrough verifies that when no
// dashboard_user is configured, the middleware is a pass-through (no auth).
func TestBasicAuthMiddleware_EmptyUserPassthrough(t *testing.T) {
	mw := BasicAuthMiddleware("", "")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mw(next))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (passthrough)", resp.StatusCode)
	}
	if !called {
		t.Fatal("handler should be called when auth is disabled")
	}
}

func TestEnqueue_BodySizeLimit(t *testing.T) {
	h, _ := newTestHandler(t)

	// Build a payload larger than 1 MB.
	big := strings.Repeat("x", 2*maxPayloadSize)
	body := `{"payload":"` + big + `"}`

	req := httptest.NewRequest(http.MethodPost, "/queues/test/messages", strings.NewReader(body))
	req.SetPathValue("name", "test")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (body too large)", w.Code, http.StatusBadRequest)
	}
}

// TestEnqueue_NormalPayloadWorks verifies that a normal-sized payload still
// works after the body limit is in place.
func TestEnqueue_NormalPayloadWorks(t *testing.T) {
	h, _ := newTestHandler(t)

	body := `{"payload":"normal message"}`
	req := httptest.NewRequest(http.MethodPost, "/queues/test/messages", strings.NewReader(body))
	req.SetPathValue("name", "test")
	w := httptest.NewRecorder()

	h.enqueue(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusCreated)
	}
}

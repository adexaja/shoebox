package shoebox

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWebhookHandler_HMACSignature verifies that when a secret is set,
// the X-Shoebox-Signature header contains the correct HMAC-SHA256 of the
// payload.
func TestWebhookHandler_HMACSignature(t *testing.T) {
	var gotSig string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // drain
		mu.Lock()
		gotSig = r.Header.Get("X-Shoebox-Signature")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	secret := "whsec-test-key"
	payload := `{"event":"created"}`

	h := WebhookHandler(srv.URL, nil, WithWebhookSecret(secret))
	err := h(context.Background(), Message{
		ID:      "msg-sign-test",
		Queue:   "orders",
		Payload: []byte(payload),
	})
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	// Verify the signature.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	expected := hex.EncodeToString(mac.Sum(nil))

	mu.Lock()
	defer mu.Unlock()
	if gotSig != expected {
		t.Fatalf("signature = %q, want %q", gotSig, expected)
	}
	if !hmac.Equal([]byte(gotSig), []byte(expected)) {
		t.Fatal("hmac.Equal failed")
	}
}

// TestWebhookHandler_NoSignatureWithoutSecret verifies that no
// X-Shoebox-Signature header is sent when no secret is configured.
func TestWebhookHandler_NoSignatureWithoutSecret(t *testing.T) {
	var sig string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sig = r.Header.Get("X-Shoebox-Signature")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil) // no secret
	_ = h(context.Background(), Message{
		ID:      "msg",
		Queue:   "q",
		Payload: []byte("test"),
	})

	mu.Lock()
	defer mu.Unlock()
	if sig != "" {
		t.Fatalf("expected empty signature, got %q", sig)
	}
}

// TestWebhookHandler_NoRedirect verifies that the default webhook client
// does NOT follow redirects, preventing SSRF.
func TestWebhookHandler_NoRedirect(t *testing.T) {
	var redirectHit atomic.Bool

	mux := http.NewServeMux()
	// /start redirects to /internal — an attacker's target.
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		redirectHit.Store(true)
		http.Redirect(w, r, "/internal", http.StatusFound)
	})
	// /internal should NEVER be reached.
	mux.HandleFunc("/internal", func(w http.ResponseWriter, r *http.Request) {
		t.Error("redirect target was hit — SSRF vulnerability!")
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	h := WebhookHandler(srv.URL+"/start", nil)
	err := h(context.Background(), Message{
		ID:      "msg",
		Queue:   "q",
		Payload: []byte("test"),
	})

	// Should return an error (302 is non-2xx).
	if err == nil {
		t.Fatal("expected error for 302 redirect response")
	}

	// The /start handler should have been hit.
	if !redirectHit.Load() {
		t.Fatal("/start was not called")
	}
	// The test fails inside /internal if it's hit, so just reaching here
	// without a test.Error means the redirect was not followed.
}

// TestWebhookHandler_SignatureVariesByPayload verifies that different
// payloads produce different signatures (i.e., the HMAC covers the body).
func TestWebhookHandler_SignatureVariesByPayload(t *testing.T) {
	var sigs []string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) // drain
		mu.Lock()
		sigs = append(sigs, r.Header.Get("X-Shoebox-Signature"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	secret := "whsec-key"
	h := WebhookHandler(srv.URL, nil, WithWebhookSecret(secret))

	_ = h(context.Background(), Message{ID: "1", Queue: "q", Payload: []byte("payload-a")})
	_ = h(context.Background(), Message{ID: "2", Queue: "q", Payload: []byte("payload-b")})

	mu.Lock()
	defer mu.Unlock()
	if len(sigs) != 2 {
		t.Fatalf("expected 2 signatures, got %d", len(sigs))
	}
	if sigs[0] == sigs[1] {
		t.Fatal("signatures for different payloads should differ")
	}
}

// TestNewWebhookClient_NoRedirects tests the client directly.
func TestNewWebhookClient_NoRedirects(t *testing.T) {
	client := newWebhookClient(5*time.Second, 16)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/target", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resp, err := client.Get(srv.URL + "/redirect")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Should get the 302, NOT follow to /target.
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302 (redirect should not be followed)", resp.StatusCode)
	}
}

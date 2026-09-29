package webhook

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
	"time"
)

// selfcert_test.go — KEDA-style self-signed certificate rotator.
//
// The webhook must be able to serve TLS without cert-manager: it
// generates a CA + serving certificate, writes them to the mounted
// Secret, and patches the ValidatingWebhookConfiguration caBundle.
// Rotation reloads via the existing certwatcher (file rewrite, no
// restart needed).

// --- pure certificate generation (no API objects) ---

func TestSelfSign_GeneratesLoadablePair(t *testing.T) {
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dnsNames := []string{
		"service-spread-webhook-service.service-spread-system.svc",
		"service-spread-webhook-service.service-spread-system.svc.cluster.local",
	}
	leaf, err := ca.GenerateLeaf(dnsNames, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// The pair must be directly loadable as a TLS certificate.
	pair, err := tls.X509KeyPair(leaf.CertPEM, leaf.KeyPEM)
	if err != nil {
		t.Fatalf("generated pair not loadable: %v", err)
	}
	if len(pair.Certificate) != 1 {
		t.Fatalf("want 1 certificate in chain, got %d", len(pair.Certificate))
	}

	// SANs must carry the DNS names we asked for.
	crt, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range crt.DNSNames {
		got[d] = true
	}
	for _, want := range dnsNames {
		if !got[want] {
			t.Fatalf("SAN missing %q; has %v", want, crt.DNSNames)
		}
	}

	// The leaf must chain to the CA.
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("CA PEM not parseable")
	}
	if _, err := crt.Verify(x509.VerifyOptions{Roots: roots, DNSName: dnsNames[0]}); err != nil {
		t.Fatalf("leaf does not verify against CA: %v", err)
	}
}

func TestSelfSign_NeedsRotation(t *testing.T) {
	ca, err := GenerateCA(90 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// A cert valid 90h needs rotation when the refresh threshold is 7d
	// (90h < 7d = 168h -> must rotate).
	leaf, err := ca.GenerateLeaf([]string{"webhook.svc"}, 90*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !LeafNeedsRotation(leaf.CertPEM, 7*24*time.Hour) {
		t.Fatal("cert expiring in 90h must need rotation under a 7d threshold")
	}
	// A cert valid 60 days does not.
	leaf2, err := ca.GenerateLeaf([]string{"webhook.svc"}, 60*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if LeafNeedsRotation(leaf2.CertPEM, 7*24*time.Hour) {
		t.Fatal("cert expiring in 60d must NOT need rotation under a 7d threshold")
	}
	// Garbage PEM: rotate (fail closed toward regeneration).
	if !LeafNeedsRotation([]byte("not a pem"), 7*24*time.Hour) {
		t.Fatal("unparseable cert must be treated as needing rotation")
	}
}

func TestSecretRoundTrip(t *testing.T) {
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.GenerateLeaf([]string{"webhook.svc"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	sec := SecretFromCerts(ca, leaf)
	if sec.Data["tls.crt"] == nil || sec.Data["tls.key"] == nil || sec.Data["ca.crt"] == nil {
		t.Fatal("secret missing tls.crt/tls.key/ca.crt")
	}
	// Round-trip back into a loadable pair (what the certwatcher consumes).
	back, err := CertsFromSecret(sec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tls.X509KeyPair(back.CertPEM, back.KeyPEM); err != nil {
		t.Fatalf("round-tripped pair not loadable: %v", err)
	}
	if block, _ := pem.Decode(sec.Data["ca.crt"]); block == nil {
		t.Fatal("ca.crt not valid PEM")
	}
}

// --- caBundle patch payload ---

func TestCaBundlePatchPayload(t *testing.T) {
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	body, err := CaBundlePatch("service-spread-policy-validator", ca.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	s := string(body)
	if !contains(s, "/webhooks/0/clientConfig/caBundle") {
		t.Fatalf("patch path missing: %s", s)
	}
	if !contains(s, base64.StdEncoding.EncodeToString(ca.CertPEM)[:40]) {
		t.Fatalf("base64 CA not embedded in patch: %.80s", s)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

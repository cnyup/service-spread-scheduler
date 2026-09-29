package webhook

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// selfcert.go — KEDA-style self-signed certificate lifecycle.
//
// The webhook serves TLS without cert-manager: on startup (leader replica
// only) it generates a CA plus a serving certificate, writes them to the
// Secret the deployment mounts, and patches the ValidatingWebhook-
// Configuration caBundle. Rotation writes a new Secret revision; the
// kubelet propagates it into the mounted volume and the existing
// certwatcher hot-reloads the pair without a restart. A periodic loop
// re-issues the leaf before expiry.
//
// Secret name/keys mirror config/manager/webhook.yaml and the optional
// cert-manager path (config/webhook/certificates.yaml): the two sources are
// interchangeable — whichever wrote the Secret, this rotator only replaces
// it when the leaf is missing or close to expiry.

const (
	// ServingCertSecretName is the Secret referenced by the webhook
	// Deployment volume and the optional cert-manager Certificate.
	ServingCertSecretName = "service-spread-webhook-serving-cert"

	caCertKey    = "ca.crt"
	tlsCertKey   = "tls.crt"
	tlsKeyKey    = "tls.key"
	caLifeTime   = 10 * 365 * 24 * time.Hour // CA outlives many leaves
	leafLifeTime = 90 * 24 * time.Hour
	// refreshWhenLeft rotates the leaf when less than this remains.
	refreshWhenLeft = 30 * 24 * time.Hour
)

// CA is a generated self-signed certificate authority.
type CA struct {
	CertPEM []byte
	KeyPEM  []byte
}

// Leaf is a serving certificate signed by a CA.
type Leaf struct {
	CertPEM []byte
	KeyPEM  []byte
}

func rsaKey() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
}

func serial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

// GenerateCA creates a fresh self-signed CA valid for ttl.
func GenerateCA(ttl time.Duration) (*CA, error) {
	key, err := rsaKey()
	if err != nil {
		return nil, err
	}
	ser, err := serial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          ser,
		Subject:               pkix.Name{CommonName: "service-spread-webhook-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(ttl),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &CA{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
	}, nil
}

// GenerateLeaf signs a serving certificate for dnsNames with the CA.
func (c *CA) GenerateLeaf(dnsNames []string, ttl time.Duration) (*Leaf, error) {
	if len(dnsNames) == 0 {
		return nil, fmt.Errorf("dnsNames required")
	}
	caBlock, _ := pem.Decode(c.CertPEM)
	if caBlock == nil {
		return nil, fmt.Errorf("CA PEM undecodable")
	}
	caParsed, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, err
	}
	caKeyBlock, _ := pem.Decode(c.KeyPEM)
	if caKeyBlock == nil {
		return nil, fmt.Errorf("CA key PEM undecodable")
	}
	caPriv, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	key, err := rsaKey()
	if err != nil {
		return nil, err
	}
	ser, err := serial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: ser,
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caParsed, &key.PublicKey, caPriv)
	if err != nil {
		return nil, err
	}
	return &Leaf{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
	}, nil
}

// LeafNeedsRotation reports whether certPEM expires within threshold or is
// unparseable (fail toward regeneration).
func LeafNeedsRotation(certPEM []byte, threshold time.Duration) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return true
	}
	crt, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return true
	}
	return time.Until(crt.NotAfter) < threshold
}

// leafSignedBy reports whether leafPEM chains to caPEM — guards against a
// Secret written by another source (e2e bootstrap, cert-manager) whose CA
// does not match our stored ca.crt / patched caBundle.
func leafSignedBy(leafPEM, caPEM []byte) bool {
	lb, _ := pem.Decode(leafPEM)
	cb, _ := pem.Decode(caPEM)
	if lb == nil || cb == nil {
		return false
	}
	leaf, err := x509.ParseCertificate(lb.Bytes)
	if err != nil {
		return false
	}
	ca, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	_, err = leaf.Verify(x509.VerifyOptions{Roots: pool})
	return err == nil
}

// SecretFromCerts bundles ca/leaf into the Secret shape the deployment
// mounts. Namespace comes from WebhookNamespace().
func SecretFromCerts(ca *CA, leaf *Leaf) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServingCertSecretName,
			Namespace: WebhookNamespace(),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			caCertKey:  ca.CertPEM,
			tlsCertKey: leaf.CertPEM,
			tlsKeyKey:  leaf.KeyPEM,
		},
	}
}

// CertsFromSecret extracts the leaf pair from a Secret (round trip of
// SecretFromCerts). The CA PEM is available via sec.Data["ca.crt"].
func CertsFromSecret(sec *corev1.Secret) (*Leaf, error) {
	cert, ok := sec.Data[tlsCertKey]
	key, ok2 := sec.Data[tlsKeyKey]
	if !ok || !ok2 {
		return nil, fmt.Errorf("secret %s/%s lacks %s/%s", sec.Namespace, sec.Name, tlsCertKey, tlsKeyKey)
	}
	return &Leaf{CertPEM: cert, KeyPEM: key}, nil
}

// CaBundlePatch builds the JSON patch replacing the first webhook's
// caBundle with the base64 CA certificate.
func CaBundlePatch(webhookConfigName string, caPEM []byte) ([]byte, error) {
	_ = webhookConfigName // patch targets the named config at apply time
	payload := []map[string]interface{}{{
		"op":    "replace",
		"path":  "/webhooks/0/clientConfig/caBundle",
		"value": base64.StdEncoding.EncodeToString(caPEM),
	}}
	return json.Marshal(payload)
}

// WebhookNamespace resolves the namespace the rotator operates in
// (WEBHOOK_NAMESPACE env, injected by the deployment downward API).
func WebhookNamespace() string {
	if ns := os.Getenv("WEBHOOK_NAMESPACE"); ns != "" {
		return ns
	}
	return "service-spread-system"
}

// ServingCertDNSNames returns the SAN list for the webhook Service
// (config/webhook/service.yaml: service-spread-webhook-service).
func ServingCertDNSNames(ns string) []string {
	return []string{
		"service-spread-webhook-service." + ns + ".svc",
		"service-spread-webhook-service." + ns + ".svc.cluster.local",
	}
}

// CertRotator maintains the serving certificate. Production relies on the
// kubelet to propagate Secret revisions into the mounted volume (the
// certwatcher reloads); the rotator itself only writes the Secret and
// patches the caBundle. Only the leader replica runs it (main.go gates
// this behind leader election).
type CertRotator struct {
	client          kubernetes.Interface
	secretNamespace string
	webhookCfgName  string
	dnsNames        []string
}

// NewCertRotator builds a rotator bound to a kubeconfig.
func NewCertRotator(cfg *rest.Config, webhookCfgName string, dnsNames []string) (*CertRotator, error) {
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &CertRotator{
		client:          client,
		secretNamespace: WebhookNamespace(),
		webhookCfgName:  webhookCfgName,
		dnsNames:        dnsNames,
	}, nil
}

// Ensure runs one reconcile pass: read the Secret, (re)issue when the leaf
// is missing/expiring/NOT SIGNED BY the stored CA (e.g. a Secret written
// by the e2e bootstrap or cert-manager before switching to self-sign),
// then patch the caBundle when a new CA was issued.
// Returns whether a new certificate was issued this pass.
func (r *CertRotator) Ensure(ctx context.Context) (bool, error) {
	sec, err := r.client.CoreV1().Secrets(r.secretNamespace).Get(ctx, ServingCertSecretName, metav1.GetOptions{})
	needIssue := err != nil ||
		len(sec.Data[tlsCertKey]) == 0 ||
		len(sec.Data[caCertKey]) == 0 ||
		LeafNeedsRotation(sec.Data[tlsCertKey], refreshWhenLeft) ||
		!leafSignedBy(sec.Data[tlsCertKey], sec.Data[caCertKey])

	if !needIssue {
		return false, nil
	}

	// (Re)issue CA + leaf in one pass. A fresh CA on every rotation is safe:
	// the caBundle is patched atomically with the new Secret, and the
	// kube-apiserver reloads webhook configs without restart.
	ca, err := GenerateCA(caLifeTime)
	if err != nil {
		return false, fmt.Errorf("generate CA: %w", err)
	}
	leaf, err := ca.GenerateLeaf(r.dnsNames, leafLifeTime)
	if err != nil {
		return false, fmt.Errorf("generate leaf: %w", err)
	}
	newSec := SecretFromCerts(ca, leaf)
	// Secret .type is immutable: reuse the existing object's type (e2e
	// bootstrap creates it as Opaque; cert-manager as kubernetes.io/tls).
	if sec != nil && err == nil {
		newSec.Type = sec.Type
		newSec.ResourceVersion = sec.ResourceVersion
	}
	if _, err := r.client.CoreV1().Secrets(r.secretNamespace).Update(ctx, newSec, metav1.UpdateOptions{}); err != nil {
		if _, cerr := r.client.CoreV1().Secrets(r.secretNamespace).Create(ctx, newSec, metav1.CreateOptions{}); cerr != nil {
			return false, fmt.Errorf("write serving-cert secret: update: %v, create: %w", err, cerr)
		}
	}

	patch, err := CaBundlePatch(r.webhookCfgName, ca.CertPEM)
	if err != nil {
		return true, err
	}
	if _, err := r.client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Patch(
		ctx, r.webhookCfgName, types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
		return true, fmt.Errorf("patch caBundle on %s: %w", r.webhookCfgName, err)
	}
	// The certwatcher may miss the kubelet's atomic symlink swap on Secret
	// revision (verified on kind 2026-09-29: files in sync, no reload until
	// restart). Rolling the webhook Deployment on each issuance closes the
	// gap — same approach as KEDA's cert-rotator.
	if err := r.rollWebhookDeployment(ctx); err != nil {
		return true, fmt.Errorf("roll webhook pods after issuance: %w", err)
	}
	return true, nil
}

// WebhookDeploymentName is the Deployment the rotator rolls on issuance.
const WebhookDeploymentName = "service-spread-webhook"

// rollWebhookDeployment bumps a pod-template annotation to trigger a
// rolling restart so every replica loads the fresh pair.
func (r *CertRotator) rollWebhookDeployment(ctx context.Context) error {
	depClient := r.client.AppsV1().Deployments(r.secretNamespace)
	dep, err := depClient.Get(ctx, WebhookDeploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations["service-spread.io/cert-issued-at"] = time.Now().UTC().Format(time.RFC3339)
	_, err = depClient.Update(ctx, dep, metav1.UpdateOptions{})
	return err
}

// Run loops Ensure until ctx ends (checkInterval between passes); errors
// are logged and retried on the next tick.
func (r *CertRotator) Run(ctx context.Context, checkInterval time.Duration) {
	l := log.FromContext(ctx)
	if _, err := r.Ensure(ctx); err != nil {
		l.Error(err, "selfcert: initial ensure failed")
	}
	t := time.NewTicker(checkInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.Ensure(ctx); err != nil {
				l.Error(err, "selfcert: ensure failed")
			}
		}
	}
}

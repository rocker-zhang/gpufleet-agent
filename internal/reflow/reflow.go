// Package reflow builds the OPT-IN, DESENSITIZED, SIGNED product-health digest
// the agent may send back to the control plane (D-0003 reflow). It is off by
// default; the agent only calls Build when the customer has opted in.
//
// The digest is DesensitizedDigest, which structurally cannot carry customer
// data — Build copies ONLY the fault class, gate signature, a coarse count
// bucket, and the agent's own version off a verdict. It never reads a device id,
// job id, timestamp, or telemetry value (the proto has no field for them). This
// is NOT corpus and must never feed the lab-only fault corpus (master-plan red
// line); it exists solely to aggregate which fault classes fire across the
// install base, by release.
package reflow

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/proto"
)

// Bucket maps an exact occurrence count to a coarse bucket. An exact count never
// crosses the wire (it could fingerprint a fleet size).
func Bucket(count int) gpufleetv1.CountBucket {
	switch {
	case count <= 0:
		return gpufleetv1.CountBucket_COUNT_BUCKET_UNSPECIFIED
	case count == 1:
		return gpufleetv1.CountBucket_COUNT_BUCKET_ONE
	case count <= 5:
		return gpufleetv1.CountBucket_COUNT_BUCKET_FEW
	default:
		return gpufleetv1.CountBucket_COUNT_BUCKET_MANY
	}
}

// Build constructs a desensitized digest from a verdict, or nil when the customer
// has not opted in (optedIn=false) or the verdict is nil. It copies ONLY the
// class, signature, coarse bucket, and agent version — by construction nothing
// that identifies a device, job, host, or window. The digest is unsigned; call
// Sign before sending.
func Build(verdict *gpufleetv1.Verdict, count int, agentVersion string, optedIn bool) *gpufleetv1.DesensitizedDigest {
	if !optedIn || verdict == nil {
		return nil
	}
	return &gpufleetv1.DesensitizedDigest{
		FaultClass:   verdict.GetFaultClass(),
		Signature:    verdict.GetSignature(),
		CountBucket:  Bucket(count),
		AgentVersion: agentVersion,
	}
}

// canonicalBytes is the deterministic encoding signed/verified over: the digest
// with signed_digest cleared, marshaled deterministically. Producer and consumer
// must agree on exactly this, so it lives here and the control plane mirrors it.
func canonicalBytes(d *gpufleetv1.DesensitizedDigest) ([]byte, error) {
	c := proto.Clone(d).(*gpufleetv1.DesensitizedDigest)
	c.SignedDigest = nil
	return proto.MarshalOptions{Deterministic: true}.Marshal(c)
}

// Signer holds the agent build's Ed25519 release private key.
type Signer struct {
	priv ed25519.PrivateKey
}

// NewSignerFromSeedB64 builds a Signer from a base64 Ed25519 seed (the agent
// build/release key, injected at runtime — never in the open repo).
func NewSignerFromSeedB64(seedB64 string) (*Signer, error) {
	seed, err := base64.StdEncoding.DecodeString(seedB64)
	if err != nil {
		return nil, fmt.Errorf("reflow: decode signing seed: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("reflow: seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return &Signer{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// PublicKeyB64 returns the base64 public key the control plane pins to verify
// digests from this agent build.
func (s *Signer) PublicKeyB64() string {
	return base64.StdEncoding.EncodeToString(s.priv.Public().(ed25519.PublicKey))
}

// Sign sets d.signed_digest to the Ed25519 signature over the canonical bytes of
// the desensitized fields. After Sign, the control plane can verify provenance.
func (s *Signer) Sign(d *gpufleetv1.DesensitizedDigest) error {
	if d == nil {
		return fmt.Errorf("reflow: nil digest")
	}
	msg, err := canonicalBytes(d)
	if err != nil {
		return fmt.Errorf("reflow: canonicalize: %w", err)
	}
	d.SignedDigest = ed25519.Sign(s.priv, msg)
	return nil
}

// Verify checks a digest's signature against a public key. Exposed so the control
// plane verifier (and tests) share the EXACT canonical encoding the signer used.
func Verify(pubKey ed25519.PublicKey, d *gpufleetv1.DesensitizedDigest) bool {
	if d == nil || len(d.GetSignedDigest()) == 0 {
		return false
	}
	msg, err := canonicalBytes(d)
	if err != nil {
		return false
	}
	return ed25519.Verify(pubKey, msg, d.GetSignedDigest())
}

package reflow

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

func testSigner(t *testing.T) (*Signer, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Signer{priv: priv}, pub
}

func firedVerdict() *gpufleetv1.Verdict {
	return &gpufleetv1.Verdict{
		FaultClass: gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS,
		Signature:  gpufleetv1.GateSignature_GATE_SIGNATURE_UNSPECIFIED,
	}
}

func TestBucket(t *testing.T) {
	cases := map[int]gpufleetv1.CountBucket{
		0: gpufleetv1.CountBucket_COUNT_BUCKET_UNSPECIFIED,
		1: gpufleetv1.CountBucket_COUNT_BUCKET_ONE,
		3: gpufleetv1.CountBucket_COUNT_BUCKET_FEW,
		5: gpufleetv1.CountBucket_COUNT_BUCKET_FEW,
		6: gpufleetv1.CountBucket_COUNT_BUCKET_MANY,
		99: gpufleetv1.CountBucket_COUNT_BUCKET_MANY,
	}
	for n, want := range cases {
		if got := Bucket(n); got != want {
			t.Errorf("Bucket(%d)=%v, want %v", n, got, want)
		}
	}
}

// Opt-out and nil yield no digest (off by default).
func TestBuild_OptOutYieldsNil(t *testing.T) {
	if Build(firedVerdict(), 1, "v1", false) != nil {
		t.Error("not-opted-in must yield nil")
	}
	if Build(nil, 1, "v1", true) != nil {
		t.Error("nil verdict must yield nil")
	}
}

// Build copies ONLY the desensitized fields — structurally there is no field for
// a device/job/timestamp to leak into.
func TestBuild_CopiesOnlyDesensitizedFields(t *testing.T) {
	d := Build(firedVerdict(), 4, "agent-1.2.3", true)
	if d == nil {
		t.Fatal("opted-in fired verdict should build a digest")
	}
	if d.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS {
		t.Errorf("class = %v", d.GetFaultClass())
	}
	if d.GetCountBucket() != gpufleetv1.CountBucket_COUNT_BUCKET_FEW {
		t.Errorf("bucket = %v, want FEW (count 4)", d.GetCountBucket())
	}
	if d.GetAgentVersion() != "agent-1.2.3" {
		t.Errorf("version = %q", d.GetAgentVersion())
	}
}

func TestSignAndVerify_RoundTrip(t *testing.T) {
	s, pub := testSigner(t)
	d := Build(firedVerdict(), 2, "v1", true)
	if err := s.Sign(d); err != nil {
		t.Fatal(err)
	}
	if len(d.GetSignedDigest()) == 0 {
		t.Fatal("Sign did not set signed_digest")
	}
	if !Verify(pub, d) {
		t.Fatal("valid digest must verify")
	}
}

func TestVerify_TamperFails(t *testing.T) {
	s, pub := testSigner(t)
	d := Build(firedVerdict(), 2, "v1", true)
	_ = s.Sign(d)

	d.CountBucket = gpufleetv1.CountBucket_COUNT_BUCKET_MANY // tamper after signing
	if Verify(pub, d) {
		t.Fatal("tampered digest must not verify")
	}
}

func TestVerify_ForeignKeyFails(t *testing.T) {
	s, _ := testSigner(t)
	_, otherPub := testSigner(t)
	d := Build(firedVerdict(), 1, "v1", true)
	_ = s.Sign(d)
	if Verify(otherPub, d) {
		t.Fatal("a different key must not verify")
	}
}

func TestVerify_UnsignedFails(t *testing.T) {
	_, pub := testSigner(t)
	if Verify(pub, Build(firedVerdict(), 1, "v1", true)) {
		t.Fatal("unsigned digest must not verify")
	}
}

func TestNewSignerFromSeedB64(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s, err := NewSignerFromSeedB64(base64.StdEncoding.EncodeToString(priv.Seed()))
	if err != nil {
		t.Fatal(err)
	}
	if s.PublicKeyB64() != base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey)) {
		t.Error("derived pubkey mismatch")
	}
	if _, err := NewSignerFromSeedB64("@@@"); err == nil {
		t.Error("bad base64 should error")
	}
}

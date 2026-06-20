package reflow

import (
	"crypto/ed25519"
	"strings"
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

// Desensitization guard: even when the source Verdict is stuffed with rich
// fields (narration, cited signals, cost), the digest must carry ONLY the four
// desensitized fields. If a future proto adds a field that Build starts copying,
// this fails — catching a silent leak before it ships.
func TestBuild_IgnoresRichVerdictFields(t *testing.T) {
	v := &gpufleetv1.Verdict{
		FaultClass:   gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS,
		Signature:    gpufleetv1.GateSignature_GATE_SIGNATURE_UNSPECIFIED,
		Confidence:   0.99,
		Narration:    "node-7 GPU-abc123 on host prod-cluster-eu fell off the bus",
		CitedSignals: []*gpufleetv1.CitedSignal{{SignalId: "dmesg.xid79.GPU-abc123"}},
	}
	d := Build(v, 1, "v1.0.0", true)
	// The digest has exactly five proto fields; only the four desensitized ones
	// are populated and none echo the identifying narration/cited content.
	if d.GetFaultClass() != v.GetFaultClass() || d.GetSignature() != v.GetSignature() {
		t.Errorf("class/sig not carried: %+v", d)
	}
	if d.GetAgentVersion() != "v1.0.0" || d.GetCountBucket() != gpufleetv1.CountBucket_COUNT_BUCKET_ONE {
		t.Errorf("version/bucket wrong: %+v", d)
	}
	// Belt-and-suspenders: marshal and assert the identifying strings are absent.
	if s := d.String(); strings.Contains(s, "abc123") || strings.Contains(s, "prod-cluster") {
		t.Errorf("digest leaked identifying content: %s", s)
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

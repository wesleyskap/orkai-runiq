package queue

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptPayload(t *testing.T) {
	key := []byte("12345678901234567890123456789012") // 32 bytes
	plaintext := []byte("hello world")

	encrypted, err := EncryptPayload(plaintext, key)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if !IsEncrypted(encrypted) {
		t.Fatalf("expected payload to be marked as encrypted")
	}

	decrypted, err := DecryptPayload(encrypted, key)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("expected %s, got %s", plaintext, decrypted)
	}
}

func TestDecryptInvalidKey(t *testing.T) {
	key1 := []byte("12345678901234567890123456789012")
	key2 := []byte("1234567890123456789012345678901x")
	plaintext := []byte("hello world")

	encrypted, _ := EncryptPayload(plaintext, key1)
	_, err := DecryptPayload(encrypted, key2)
	if err == nil {
		t.Errorf("expected decryption to fail with incorrect key")
	}
}

func TestDecryptUnencrypted(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	_, err := DecryptPayload([]byte("not encrypted"), key)
	if err != ErrNoHeader {
		t.Errorf("expected ErrNoHeader, got %v", err)
	}
}

func TestDecryptTooShort(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	payload := append([]byte(nil), EncryptionHeader...)
	_, err := DecryptPayload(payload, key)
	if err != ErrPayloadTooShort {
		t.Errorf("expected ErrPayloadTooShort, got %v", err)
	}
}

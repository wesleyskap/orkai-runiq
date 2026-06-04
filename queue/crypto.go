package queue

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

var (
	// EncryptionHeader is prefixed to encrypted payloads to distinguish them.
	EncryptionHeader = []byte("runiq:enc:")

	// ErrInvalidKey is returned when the key size is not 16, 24, or 32 bytes.
	ErrInvalidKey = errors.New("crypto: invalid key size, must be 16, 24, or 32 bytes")

	// ErrPayloadTooShort is returned when an encrypted payload is too short.
	ErrPayloadTooShort = errors.New("crypto: payload is too short")

	// ErrNoHeader is returned when trying to decrypt a payload without the encryption header.
	ErrNoHeader = errors.New("crypto: missing encryption header")
)

// EncryptPayload encrypts data using AES-GCM and prepends the magic header.
func EncryptPayload(plaintext []byte, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidKey
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return buildEncryptedBytes(nonce, ciphertext), nil
}

func buildEncryptedBytes(nonce, ciphertext []byte) []byte {
	res := make([]byte, len(EncryptionHeader)+len(nonce)+len(ciphertext))
	copy(res[0:], EncryptionHeader)
	copy(res[len(EncryptionHeader):], nonce)
	copy(res[len(EncryptionHeader)+len(nonce):], ciphertext)
	return res
}

// IsEncrypted checks if the payload has the encryption header prefix.
func IsEncrypted(payload []byte) bool {
	if len(payload) < len(EncryptionHeader) {
		return false
	}
	for i, b := range EncryptionHeader {
		if payload[i] != b {
			return false
		}
	}
	return true
}

// DecryptPayload decrypts data using AES-GCM and verifies the magic header.
func DecryptPayload(payload []byte, key []byte) ([]byte, error) {
	if !IsEncrypted(payload) {
		return nil, ErrNoHeader
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidKey
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return decryptCipher(payload, gcm, key)
}

func decryptCipher(payload []byte, gcm cipher.AEAD, key []byte) ([]byte, error) {
	headerLen := len(EncryptionHeader)
	nonceSize := gcm.NonceSize()
	if len(payload) < headerLen+nonceSize {
		return nil, ErrPayloadTooShort
	}
	nonce := payload[headerLen : headerLen+nonceSize]
	ciphertext := payload[headerLen+nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

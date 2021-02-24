package storerecord

import (
	"math/rand"
	"testing"
	"time"

	"github.com/khenidak/london/pkg/backend/consts"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestSplitter(t *testing.T) {
	testCases := []struct {
		name             string
		value            []byte
		expectedPartsLen int
	}{
		{
			name:             "single-row-exact",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize)),
			expectedPartsLen: 1,
		},
		{
			name:             "single-row-less",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize)),
			expectedPartsLen: 1,
		},
		{
			name:             "larger-than-single-1",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize + 79)),
			expectedPartsLen: 2,
		},
		{
			name:             "larger-than-single-2",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize * 2)),
			expectedPartsLen: 2,
		},
		{
			name:             "larger-than-single-3",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize*2 + 79)),
			expectedPartsLen: 3,
		},
		{
			name:             "larger-than-single-4",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize*3 + 79)),
			expectedPartsLen: 4,
		},
		{
			name:             "larger-than-single-4-exact",
			value:            []byte(randStringRunes(consts.DataFieldMaxSize * 4)),
			expectedPartsLen: 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			parts := split(testCase.value, consts.DataFieldMaxSize)

			combined := []byte{}
			for _, part := range parts {
				combined = append(combined, part...)
			}

			if len(parts) != testCase.expectedPartsLen {
				t.Fatalf("expected part count %v got %v", testCase.expectedPartsLen, len(parts))
			}
			if string(combined) != string(testCase.value) {
				t.Fatalf("parts after combined != expected value")
			}

		})
	}
}

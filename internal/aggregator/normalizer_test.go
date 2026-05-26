package aggregator

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		maxRunes int
		want     string
		wantErr  error
	}{
		{
			name:     "trims lowercases and collapses spaces",
			input:    "  ПрИвЕт    МиР  ",
			maxRunes: 64,
			want:     "привет мир",
		},
		{
			name:     "collapses unicode whitespace",
			input:    "iphone\t15\ncase\u00a0black",
			maxRunes: 64,
			want:     "iphone 15 case black",
		},
		{
			name:     "removes control characters",
			input:    "iph\x00one\u0007 case",
			maxRunes: 64,
			want:     "iphone case",
		},
		{
			name:     "empty after normalization",
			input:    " \x00\u0007 ",
			maxRunes: 64,
			wantErr:  ErrEmptyQuery,
		},
		{
			name:     "too long",
			input:    strings.Repeat("я", 4),
			maxRunes: 3,
			wantErr:  ErrQueryTooLong,
		},
		{
			name:     "unicode length is counted in runes",
			input:    "чай",
			maxRunes: 3,
			want:     "чай",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NormalizeQuery(tt.input, tt.maxRunes)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NormalizeQuery() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("NormalizeQuery() = %q, want %q", got, tt.want)
			}
		})
	}
}

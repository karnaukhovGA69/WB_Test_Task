package aggregator

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

const defaultMaxQueryRunes = 256

var (
	ErrEmptyQuery   = errors.New("query is empty after normalization")
	ErrQueryTooLong = errors.New("query is too long")
)

func NormalizeQuery(query string, maxRunes int) (string, error) {
	if maxRunes <= 0 {
		maxRunes = defaultMaxQueryRunes
	}

	query = strings.TrimSpace(query)
	if query == "" {
		return "", ErrEmptyQuery
	}

	var builder strings.Builder
	builder.Grow(len(query))

	previousWasSpace := false
	writtenRunes := 0

	for _, r := range query {
		if unicode.IsSpace(r) {
			if builder.Len() > 0 && !previousWasSpace {
				builder.WriteRune(' ')
				previousWasSpace = true
				writtenRunes++
			}
			continue
		}

		if unicode.IsControl(r) {
			continue
		}

		builder.WriteRune(unicode.ToLower(r))
		previousWasSpace = false
		writtenRunes++

		if writtenRunes > maxRunes {
			return "", ErrQueryTooLong
		}
	}

	normalized := strings.TrimSpace(builder.String())
	if normalized == "" {
		return "", ErrEmptyQuery
	}

	if utf8.RuneCountInString(normalized) > maxRunes {
		return "", ErrQueryTooLong
	}

	return normalized, nil
}

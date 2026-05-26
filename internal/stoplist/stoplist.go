package stoplist

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
)

type Manager struct {
	mu    sync.Mutex
	words atomic.Value
}

func NewManager(initial []string) *Manager {
	manager := &Manager{}
	manager.words.Store(buildSet(initial))
	return manager
}

func (m *Manager) Contains(word string) bool {
	set := m.load()
	_, ok := set[normalize(word)]
	return ok
}

func (m *Manager) Add(word string) bool {
	word = normalize(word)
	if word == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.load()
	if _, ok := current[word]; ok {
		return false
	}

	next := cloneSet(current)
	next[word] = struct{}{}
	m.words.Store(next)

	return true
}

func (m *Manager) Remove(word string) bool {
	word = normalize(word)
	if word == "" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.load()
	if _, ok := current[word]; !ok {
		return false
	}

	next := cloneSet(current)
	delete(next, word)
	m.words.Store(next)

	return true
}

func (m *Manager) List() []string {
	current := m.load()
	words := make([]string, 0, len(current))
	for word := range current {
		words = append(words, word)
	}

	sort.Strings(words)
	return words
}

func (m *Manager) Size() int {
	return len(m.load())
}

func (m *Manager) load() map[string]struct{} {
	value := m.words.Load()
	if value == nil {
		return map[string]struct{}{}
	}

	return value.(map[string]struct{})
}

func buildSet(words []string) map[string]struct{} {
	set := make(map[string]struct{}, len(words))
	for _, word := range words {
		if normalized := normalize(word); normalized != "" {
			set[normalized] = struct{}{}
		}
	}

	return set
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	clone := make(map[string]struct{}, len(source)+1)
	for word := range source {
		clone[word] = struct{}{}
	}

	return clone
}

func normalize(word string) string {
	word = strings.TrimSpace(word)
	if word == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(word))

	previousWasSpace := false
	for _, r := range word {
		if unicode.IsControl(r) {
			continue
		}
		if unicode.IsSpace(r) {
			if builder.Len() > 0 && !previousWasSpace {
				builder.WriteRune(' ')
				previousWasSpace = true
			}
			continue
		}

		builder.WriteRune(unicode.ToLower(r))
		previousWasSpace = false
	}

	return strings.TrimSpace(builder.String())
}

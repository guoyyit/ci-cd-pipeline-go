package config

import (
	"path"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Matcher decides whether an event matches a pipeline's trigger filters.
type Matcher struct {
	triggers model.Triggers
}

// NewMatcher builds a Matcher for the given trigger rules.
func NewMatcher(t model.Triggers) *Matcher { return &Matcher{triggers: t} }

// Match reports whether the event should trigger the pipeline.
// Branch events match "branches" glob patterns; tag events match "tags".
func (m *Matcher) Match(ev model.Event) bool {
	switch ev.Kind {
	case model.KindPush:
		if ev.Branch == "" {
			return false
		}
		for _, pat := range m.triggers.Branches {
			if ok, _ := path.Match(pat, ev.Branch); ok {
				return true
			}
		}
	case model.KindTag:
		if ev.Tag == "" {
			return false
		}
		for _, pat := range m.triggers.Tags {
			if ok, _ := path.Match(pat, ev.Tag); ok {
				return true
			}
		}
	}
	return false
}

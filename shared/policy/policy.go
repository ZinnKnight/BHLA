package policy

import (
	"fmt"
	"time"

	"BHLA/shared/authroles"
)

type Action string

const (
	ActionLogin       Action = "login"
	ActionCreateOrder Action = "create_order"
)

type Rule struct {
	Limit  int
	Window time.Duration
}

var Unlimited = Rule{Limit: 0}

type Provider interface {
	RuleFor(plan string, action Action) Rule
}

type StaticProvider struct {
	rules map[authroles.Plan]map[Action]Rule
}

func (p *StaticProvider) validate() error {
	for _, plan := range authroles.All() {
		if _, ok := p.rules[plan]; !ok {
			return fmt.Errorf("policy: no rules for plan %q", plan)
		}
	}
	return nil
}

func NewStaticProvider() (*StaticProvider, error) {
	p := &StaticProvider{
		rules: map[authroles.Plan]map[Action]Rule{
			authroles.Free: {
				ActionLogin:       {Limit: 100, Window: time.Hour},
				ActionCreateOrder: {Limit: 10, Window: 24 * time.Hour},
			},
			authroles.Pro:   {},
			authroles.Admin: {},
		},
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *StaticProvider) RuleFor(plan string, action Action) Rule {
	actions, ok := p.rules[authroles.Plan(plan)]
	if !ok {
		actions = p.rules[authroles.Free]
	}
	if rule, ok := actions[action]; ok {
		return rule
	}
	return Unlimited
}

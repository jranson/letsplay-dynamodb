package session

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type Session interface {
	Config() aws.Config
}

func NewSession() (Session, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	return NewWithConfig(cfg), nil
}

func NewSessionMust() Session {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("session configuration error: " + err.Error())
	}
	return NewWithConfig(cfg)
}

func NewWithConfig(c aws.Config) Session {
	s := &identitySession{
		config: c,
	}
	return s
}

type identitySession struct {
	config aws.Config
}

func (s *identitySession) Config() aws.Config {
	return s.config
}

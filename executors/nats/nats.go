package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	nt "github.com/nats-io/nats.go"
	"github.com/ovh/venom"
	"github.com/pkg/errors"
)

const Name = "nats"

func New() venom.Executor {
	return &Executor{}
}

type Executor struct {
	Addr       string    `json:"addr" yaml:"addr"`
	ClientType string    `json:"client_type" yaml:"clientType"`
	Messages   []Message `json:"messages" yaml:"messages"`
}

type Message struct {
	Subject string              `json:"subject" yaml:"subject"`
	Payload string              `json:"payload" yaml:"payload"`
	Headers map[string][]string `json:"headers" yaml:"headers"`
}

type Result struct {
	TimeSeconds  float64       `json:"timeseconds" yaml:"timeSeconds"`
	Subjects     []string      `json:"subjects" yaml:"subjects"`
	Messages     [][]byte      `json:"messages" yaml:"messages"`
	MessagesJSON []interface{} `json:"messagesjson" yaml:"messagesJSON"`
	Err          string        `json:"err" yaml:"error"`
}

func (Executor) Run(ctx context.Context, step venom.TestStep) (interface{}, error) {
	var e Executor
	if err := mapstructure.Decode(step, &e); err != nil {
		return nil, err
	}

	start := time.Now()
	result := Result{}

	if e.Addr == "" {
		return nil, errors.New("address is mandatory")
	}

	var err error
	switch e.ClientType {
	case "publisher":
		err = e.publishMessage(ctx)
		if err != nil {
			result.Err = err.Error()
		}
	default:
		return nil, fmt.Errorf("clientType %q must be publisher", e.ClientType)
	}

	elapsed := time.Since(start)
	result.TimeSeconds = elapsed.Seconds()

	return result, nil
}

func (e Executor) session(ctx context.Context) (*nt.Conn, error) {
	venom.Debug(ctx, "creating session to %v", e.Addr)

	client, err := nt.Connect(e.Addr)
	if err != nil {
		return nil, err
	}

	venom.Debug(ctx, "connection setup complete")

	return client, err
}

func (e Executor) publishMessage(ctx context.Context) error {
	client, err := e.session(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	for i, m := range e.Messages {
		if len(m.Subject) == 0 {
			return errors.Errorf("mandatory field Subject was empty in Messages[%v](%v)", i, m)
		}

		msg := nt.Msg{
			Subject: m.Subject,
			Data:    []byte(m.Payload),
			Header:  m.Headers,
		}

		err = client.PublishMsg(&msg)
		if err != nil {
			venom.Debug(ctx, "Message publish failed")
			return errors.Wrapf(err, "Message publish failed: Messages[%v](%v)", i, m)
		}
	}

	return nil
}

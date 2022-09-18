package fastmulticache

import (
	"time"

	"github.com/golang/glog"
)

type MutexTimeout struct {
	ch chan struct{}
}

func NewMutexTimeout() (mu *MutexTimeout) {
	mu = &MutexTimeout{
		ch: make(chan struct{}, 1),
	}
	mu.ch <- struct{}{}
	return mu
}

func (m *MutexTimeout) LockTimeout(d time.Duration) bool {
	t := time.NewTimer(d)
	select {
	case <-m.ch:
		t.Stop()
		return true
	case <-t.C:
	}
	return false
}

func (m *MutexTimeout) Unlock() {
	select {
	case m.ch <- struct{}{}:
	default:
		glog.Error("unlock fail")
	}
}

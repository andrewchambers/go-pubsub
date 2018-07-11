package pubsub

import (
	"testing"
	"time"
)

func TestSub(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t1")
	ch3 := ps.Sub("t2")

	ps.Pub("hi", "t1")
	ps.Pub("hello", "t2")

	if (<-ch1).(string) != "hi" {
		t.FailNow()
	}

	if (<-ch2).(string) != "hi" {
		t.FailNow()
	}

	if (<-ch3).(string) != "hello" {
		t.FailNow()
	}

}

func TestSubOnce(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch := ps.SubOnce("t1")

	ps.Pub("hi", "t1")
	ps.Pub("hi", "t1")

	if (<-ch).(string) != "hi" {
		t.FailNow()
	}

	select {
	case <-ch:
		t.FailNow()
	case <-time.After(10 * time.Millisecond):
	}
}

func TestAddSub(t *testing.T) {
	ps := New(3)
	defer ps.Shutdown()

	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	ps.Pub("hi1", "t1")
	ps.Pub("hi2", "t2")

	ps.AddSub(ch1, "t2", "t3")
	ps.Pub("hi3", "t2")
	ps.Pub("hi4", "t3")

	join := make(chan struct{})

	go func() {
		if (<-ch1).(string) != "hi1" {
			t.FailNow()
		}

		if (<-ch1).(string) != "hi3" {
			t.FailNow()
		}

		if (<-ch1).(string) != "hi4" {
			t.FailNow()
		}

		join <- struct{}{}
	}()

	if (<-ch2).(string) != "hi2" {
		t.FailNow()
	}

	if (<-ch2).(string) != "hi3" {
		t.FailNow()
	}

	<-join
}

func TestUnsub(t *testing.T) {
	ps := New(2)
	defer ps.Shutdown()

	ch := ps.Sub("t1")

	ps.Pub("hi", "t1")
	ps.Unsub(ch, "t1")
	ps.Pub("hi", "t1")

	if (<-ch).(string) != "hi" {
		t.FailNow()
	}

	select {
	case <-ch:
		t.FailNow()
	case <-time.After(10 * time.Millisecond):
	}
}

func TestUnsubAll(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch1 := ps.Sub("t1", "t2", "t3")
	ch2 := ps.Sub("t1", "t3")

	ps.Unsub(ch1)
	ps.Pub("hi", "t1")

	if (<-ch2).(string) != "hi" {
		t.FailNow()
	}

	select {
	case <-ch1:
		t.FailNow()
	case <-time.After(10 * time.Millisecond):
	}
}

func TestShutdown(t *testing.T) {
	ps := New(10)
	ps.Shutdown()
	<-ps.Done()
}

func TestMultiSub(t *testing.T) {
	ps := New(2)
	defer ps.Shutdown()

	ch := ps.Sub("t1", "t2")

	ps.Pub("hi", "t1")
	ps.Pub("hello", "t2")

	if (<-ch).(string) != "hi" {
		t.FailNow()
	}

	if (<-ch).(string) != "hello" {
		t.FailNow()
	}
}

func TestMultiPub(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch1 := ps.Sub("t1")
	ch2 := ps.Sub("t2")

	ps.Pub("hi", "t1", "t2")

	if (<-ch1).(string) != "hi" {
		t.FailNow()
	}

	if (<-ch2).(string) != "hi" {
		t.FailNow()
	}
}

func TestTryPub(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch := ps.Sub("t1")
	ps.TryPub("hi", "t1")
	ps.TryPub("there", "t1")

	<-ch
	select {
	case <-ch:
		t.FailNow()
	default:
	}
}

func TestMultiUnsub(t *testing.T) {
	ps := New(1)
	defer ps.Shutdown()

	ch := ps.Sub("t1", "t2", "t3")
	ps.Unsub(ch, "t1")
	ps.Pub("hi", "t1")
	ps.Pub("hello1", "t2")
	ps.Unsub(ch, "t2", "t3")
	ps.Pub("hello2", "t2")

	if (<-ch).(string) != "hello1" {
		t.FailNow()
	}

	select {
	case <-ch:
		t.FailNow()
	case <-time.After(10 * time.Millisecond):
	}
}

// Package pubsub implements a simple multi-topic pub-sub
// library.
//
// Topics must be strings and messages of any type can be
// published. A topic can have any number of subcribers and
// all of them receive messages published on the topic.
package pubsub

type operation int

const (
	sub operation = iota
	subOnce
	pub
	tryPub
	unsub
	unsubAll
	shutdown
)

// PubSub is a collection of topics.
type PubSub struct {
	cmdChan      chan cmd
	capacity     int
	shuttingDown chan struct{}
}

type cmd struct {
	op     operation
	topics []string
	ch     chan interface{}
	msg    interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
func New(capacity int) *PubSub {
	ps := &PubSub{
		cmdChan:      make(chan cmd),
		shuttingDown: make(chan struct{}),
		capacity:     capacity,
	}
	go ps.start()
	return ps
}

// Returns a channel that will be closed
// when the PubSub has been shutdown
func (ps *PubSub) Done() chan struct{} {
	return ps.shuttingDown
}

// Sub returns a channel on which messages published on any of
// the specified topics can be received.
func (ps *PubSub) Sub(topics ...string) chan interface{} {
	return ps.sub(sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.sub(subOnce, topics...)
}

func (ps *PubSub) sub(op operation, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	select {
	case ps.cmdChan <- cmd{op: op, topics: topics, ch: ch}:
	case <-ps.shuttingDown:
	}
	return ch
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	select {
	case ps.cmdChan <- cmd{op: sub, topics: topics, ch: ch}:
	case <-ps.shuttingDown:
	}
}

// Pub publishes the given message to all subscribers of
// the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	select {
	case ps.cmdChan <- cmd{op: pub, topics: topics, msg: msg}:
	case <-ps.shuttingDown:
	}
}

// TryPub publishes the given message to all subscribers of
// the specified topics if the topic has buffer space.
func (ps *PubSub) TryPub(msg interface{}, topics ...string) {
	select {
	case ps.cmdChan <- cmd{op: tryPub, topics: topics, msg: msg}:
	case <-ps.shuttingDown:
	}
}

// Unsub unsubscribes the given channel from the specified
// topics. If no topic is specified, it is unsubscribed
// from all topics.
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	if len(topics) == 0 {
		select {
		case ps.cmdChan <- cmd{op: unsubAll, ch: ch}:
		case <-ps.shuttingDown:
		}
		return
	}

	select {
	case ps.cmdChan <- cmd{op: unsub, topics: topics, ch: ch}:
	case <-ps.shuttingDown:
	}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *PubSub) Shutdown() {
	select {
	case ps.cmdChan <- cmd{op: shutdown}:
	case <-ps.shuttingDown:
	}

}

func (ps *PubSub) start() {
	defer close(ps.shuttingDown)

	reg := registry{
		topics:       make(map[string]map[chan interface{}]bool),
		revTopics:    make(map[chan interface{}]map[string]bool),
		shuttingDown: ps.shuttingDown,
	}

loop:
	for cmd := range ps.cmdChan {

		if cmd.topics == nil {
			switch cmd.op {
			case unsubAll:
				reg.removeChannel(cmd.ch)
			case shutdown:
				break loop
			}

			continue loop
		}

		for _, topic := range cmd.topics {
			switch cmd.op {
			case sub:
				reg.add(topic, cmd.ch, false)
			case subOnce:
				reg.add(topic, cmd.ch, true)
			case tryPub:
				reg.sendNoWait(topic, cmd.msg)
			case pub:
				reg.send(topic, cmd.msg)
			case unsub:
				reg.remove(topic, cmd.ch)
			}
		}
	}

	for topic, chans := range reg.topics {
		for ch := range chans {
			reg.remove(topic, ch)
		}
	}
}

// registry maintains the current subscription state. It's not
// safe to access a registry from multiple goroutines simultaneously.
type registry struct {
	topics       map[string]map[chan interface{}]bool
	revTopics    map[chan interface{}]map[string]bool
	shuttingDown chan struct{}
}

func (reg *registry) add(topic string, ch chan interface{}, once bool) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]bool)
	}
	reg.topics[topic][ch] = once

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = true
}

func (reg *registry) send(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		select {
		case ch <- msg:
		case <-reg.shuttingDown:
		}
		if once {
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
			}
		}
	}
}

func (reg *registry) sendNoWait(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		select {
		case ch <- msg:
			if once {
				for topic := range reg.revTopics[ch] {
					reg.remove(topic, ch)
				}
			}
		default:
		}
	}
}

func (reg *registry) removeTopic(topic string) {
	m, ok := reg.topics[topic]
	if !ok {
		return
	}
	for ch := range m {
		reg.remove(topic, ch)
	}
}

func (reg *registry) removeChannel(ch chan interface{}) {
	m, ok := reg.revTopics[ch]
	if !ok {
		return
	}
	for topic := range m {
		reg.remove(topic, ch)
	}
}

func (reg *registry) remove(topic string, ch chan interface{}) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][ch]; !ok {
		return
	}

	delete(reg.topics[topic], ch)
	delete(reg.revTopics[ch], topic)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		delete(reg.revTopics, ch)
	}
}

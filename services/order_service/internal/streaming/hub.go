package streaming

import "sync"

type Update struct {
	OrderID string
	Status  string
}

type Hub struct {
	mu     sync.RWMutex
	subs   map[string]map[int]chan Update
	nextID int
}

func NewHub() *Hub {
	return &Hub{subs: make(map[string]map[int]chan Update)}
}

func (h *Hub) Subscribe(orderID string) (int, <-chan Update) {
	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++

	ch := make(chan Update, 1)
	if h.subs[orderID] == nil {
		h.subs[orderID] = make(map[int]chan Update)
	}
	h.subs[orderID][id] = ch
	return id, ch
}

func (h *Hub) Unsubscribe(orderID string, id int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if m, ok := h.subs[orderID]; ok {
		delete(m, id)
		if len(m) == 0 {
			delete(h.subs, orderID)
		}
	}
}

func (h *Hub) Publish(u Update) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, ch := range h.subs[u.OrderID] {
		select {
		case ch <- u:
		default:
		}
	}
}

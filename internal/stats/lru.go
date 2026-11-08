package stats

import "container/list"

// lru keeps at most limit entries, dropping the least recently used, and is not safe to share
type lru[K comparable, V any] struct {
	limit int
	order *list.List
	items map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key K
	val V
}

func newLRU[K comparable, V any](limit int) *lru[K, V] {
	return &lru[K, V]{limit: limit, order: list.New(), items: map[K]*list.Element{}}
}

func (l *lru[K, V]) get(k K) (V, bool) {
	e, ok := l.items[k]
	if !ok {
		var zero V
		return zero, false
	}
	l.order.MoveToFront(e)
	return e.Value.(*lruEntry[K, V]).val, true
}

func (l *lru[K, V]) put(k K, v V) {
	if e, ok := l.items[k]; ok {
		e.Value.(*lruEntry[K, V]).val = v
		l.order.MoveToFront(e)
		return
	}
	l.items[k] = l.order.PushFront(&lruEntry[K, V]{key: k, val: v})
	for l.order.Len() > l.limit {
		last := l.order.Back()
		l.order.Remove(last)
		delete(l.items, last.Value.(*lruEntry[K, V]).key)
	}
}

func (l *lru[K, V]) size() int { return l.order.Len() }

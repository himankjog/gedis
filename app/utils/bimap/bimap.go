package bimap

type BiMap[K comparable, V comparable] struct {
	lookupMap        map[K]V
	reverseLookupMap map[V]K
}

func New[K comparable, V comparable]() *BiMap[K, V] {
	return &BiMap[K, V]{
		lookupMap:        make(map[K]V),
		reverseLookupMap: make(map[V]K),
	}
}

func (this *BiMap[K, V]) Insert(key K, value V) {
	this.lookupMap[key] = value
	this.reverseLookupMap[value] = key
}

func (this *BiMap[K, V]) Lookup(key K) (V, bool) {
	value, ok := this.lookupMap[key]
	return value, ok
}

func (this *BiMap[K, V]) ReverseLookup(value V) (K, bool) {
	key, ok := this.reverseLookupMap[value]
	return key, ok
}

func (this *BiMap[K, V]) Delete(key K) {
	if value, ok := this.lookupMap[key]; ok {
		delete(this.lookupMap, key)
		delete(this.reverseLookupMap, value)
	}
}

func (this *BiMap[K, V]) DeleteUsingReverseLookup(value V) {
	if key, ok := this.reverseLookupMap[value]; ok {
		delete(this.lookupMap, key)
		delete(this.reverseLookupMap, value)
	}
}

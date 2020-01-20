package skiplist

func put(list *SkipList, key string, value string) {
	list.Put([]byte(key), []byte(value))
}

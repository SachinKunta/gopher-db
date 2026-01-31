package main

import (
	"fmt"

	"github.com/SachinKunta/gopher-db/internal/storage"
)

func main() {
	store, err := storage.New("data.wal")
	if err != nil {
		panic(err)
	}

	store.Set("name", []byte("sachin"))

	val, _ := store.Get("name")
	fmt.Printf("Got: %s\n", string(val))
}

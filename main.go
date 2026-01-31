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

	val, err := store.Get("name")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Got: %s\n", string(val))
}

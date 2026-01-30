package main

import (
	"fmt"

	"github.com/SachinKunta/gopher-db/internal/storage"
)

func main() {
	store := storage.New()
	store.Set("name", "sachin")

	val, ok := store.Get("name")
	if ok {
		fmt.Printf("Got: %s\n", val)
	}

	fmt.Printf("Keys: %v\n", store.Keys())
}

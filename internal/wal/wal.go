package wal

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type WAL struct {
	file *os.File
	path string
}

type Entry struct {
	Op    string
	Key   string
	Value string
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) Append(op string, key string, value any) error {
	_, err := fmt.Fprintf(w.file, "%s|%s|%v\n", op, key, value)
	if err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Replay() []Entry {
	f, err := os.OpenFile(w.path, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("Not able to read the file", err)
	}
	defer f.Close()

	var entries []Entry
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), "|")
		if len(line) >= 3 {
			entries = append(entries, Entry{
				Op:    line[0],
				Key:   line[1],
				Value: line[2],
			})
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return entries
}

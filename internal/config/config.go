package config

import (
	"bufio"
	"os"
	"strings"
)

const DefaultLogSegment = "00000000000000000000.log"

var LogDirs string = "/tmp/kraft-combined-logs"

func Load(filepath string) error {
	f, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "log.dirs=") {
			LogDirs = strings.TrimPrefix(line, "log.dirs=")
		}
	}
	return scanner.Err()
}

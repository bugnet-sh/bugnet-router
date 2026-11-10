// Command inputs rewrites deploy/image/inputs.sha256 by the rule of CP 19.6.3
package main

import (
	"fmt"
	"os"

	"github.com/bugnet-sh/bugnet-router/internal/image"
)

func main() {
	changed, err := image.SyncInputs("deploy", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if changed {
		fmt.Println("deploy/image/inputs.sha256 rewritten")
	}
}

package main

import "fmt"

func main() {
	seedDB := make(map[string]string)

	seedDB["name"] = "ABCD"

	fmt.Println(seedDB["name"])
}

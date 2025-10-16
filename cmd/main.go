package main

import (
	"dz3/internal/task"
	"fmt"
)

func main() {
	clients := []task.Client{
		{ID: 1, Type: task.Add, Balance: 4242},
		{ID: 2, Type: task.Remove, Balance: 6767},
		{ID: 3, Type: task.Add, Balance: 1337},
		{ID: 4, Type: task.Remove, Balance: 420},
		{ID: 5, Type: task.Remove, Balance: 228},
	}

	task.Run(&clients)
	fmt.Println(clients)
}

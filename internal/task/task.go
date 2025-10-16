package task

import (
	"sync"
)

type TaskType int

const (
	Add TaskType = iota
	Remove
	Done
)

type Client struct {
	ID      int
	Type    TaskType
	Balance float64
}

func dispatcher(input <-chan *Client, remove chan<- *Client, add chan<- *Client, wg *sync.WaitGroup) {
}

func removeTaskTypeServer(remove <-chan *Client, processedRemove chan<- *Client, wg *sync.WaitGroup) {
}

func addTaskTypeServer(add <-chan *Client, processedAdd chan<- *Client, wg *sync.WaitGroup) {
}

func finalServer(processedRemove <-chan *Client, processedAdd <-chan *Client, wg *sync.WaitGroup) {
}

func Run(clients []Client) {
}

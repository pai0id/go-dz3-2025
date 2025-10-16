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
	defer wg.Done()
	for client := range input {
		if client.Type == Remove {
			remove <- client
		} else if client.Type == Add {
			add <- client
		}
	}
	close(remove)
	close(add)
}

func removeTaskTypeServer(remove <-chan *Client, processedRemove chan<- *Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for client := range remove {
		client.Balance -= 100
		processedRemove <- client
	}
}

func addTaskTypeServer(add <-chan *Client, processedAdd chan<- *Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for client := range add {
		client.Balance += 100
		processedAdd <- client
	}
}

func finalServer(processedRemove <-chan *Client, processedAdd <-chan *Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for processedRemove != nil || processedAdd != nil {
		select {
		case client, ok := <-processedRemove:
			if !ok {
				processedRemove = nil
				continue
			}
			client.Balance *= 1.5
			client.Type = Done
		case client, ok := <-processedAdd:
			if !ok {
				processedAdd = nil
				continue
			}
			client.Balance /= 2
			client.Type = Done
		default:
			if processedRemove == nil && processedAdd == nil {
				return
			}
		}
	}
}

func Run(clients *[]Client) {
	input := make(chan *Client, 10)
	remove := make(chan *Client, 10)
	add := make(chan *Client, 10)
	processedRemove := make(chan *Client, 10)
	processedAdd := make(chan *Client, 10)

	var wg sync.WaitGroup
	var removeWg sync.WaitGroup
	var addWg sync.WaitGroup

	numRemoveServers := 2
	numAddServers := 2

	removeWg.Add(numRemoveServers)
	addWg.Add(numAddServers)

	wg.Add(1)
	go dispatcher(input, remove, add, &wg)

	for i := 0; i < numRemoveServers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			removeTaskTypeServer(remove, processedRemove, &removeWg)
		}()
	}

	for i := 0; i < numAddServers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addTaskTypeServer(add, processedAdd, &addWg)
		}()
	}

	go func() {
		removeWg.Wait()
		close(processedRemove)
	}()

	go func() {
		addWg.Wait()
		close(processedAdd)
	}()

	wg.Add(1)
	go finalServer(processedRemove, processedAdd, &wg)

	for i := range clients {
		input <- &clients[i]
	}
	close(input)

	wg.Wait()
}

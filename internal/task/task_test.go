package task

import (
	"sync"
	"testing"
)

func TestDispatcher(t *testing.T) {
	input := make(chan *Client, 10)
	remove := make(chan *Client, 10)
	add := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go dispatcher(input, remove, add, &wg)

	// Test Remove type
	clientRemove := &Client{ID: 1, Type: Remove, Balance: 200}
	input <- clientRemove

	// Test Add type
	clientAdd := &Client{ID: 2, Type: Add, Balance: 100}
	input <- clientAdd

	close(input)
	wg.Wait()

	// Check remove channel
	select {
	case received := <-remove:
		if received.ID != 1 || received.Type != Remove {
			t.Errorf("Expected client ID 1, Type Remove, got ID %d, Type %v", received.ID, received.Type)
		}
	default:
		t.Error("Expected client in remove channel")
	}

	// Check add channel
	select {
	case received := <-add:
		if received.ID != 2 || received.Type != Add {
			t.Errorf("Expected client ID 2, Type Add, got ID %d, Type %v", received.ID, received.Type)
		}
	default:
		t.Error("Expected client in add channel")
	}
}

func TestRemoveTaskTypeServer(t *testing.T) {
	remove := make(chan *Client, 10)
	processedRemove := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go removeTaskTypeServer(remove, processedRemove, &wg)

	client := &Client{ID: 1, Type: Remove, Balance: 200}
	remove <- client
	close(remove)

	wg.Wait()
	close(processedRemove)

	received := <-processedRemove
	if received.Balance != 100 {
		t.Errorf("Expected balance 100, got %f", received.Balance)
	}
	if received.ID != 1 || received.Type != Remove {
		t.Errorf("Expected client ID 1, Type Remove, got ID %d, Type %v", received.ID, received.Type)
	}
}

func TestAddTaskTypeServer(t *testing.T) {
	add := make(chan *Client, 10)
	processedAdd := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go addTaskTypeServer(add, processedAdd, &wg)

	client := &Client{ID: 2, Type: Add, Balance: 100}
	add <- client
	close(add)

	wg.Wait()
	close(processedAdd)

	received := <-processedAdd
	if received.Balance != 200 {
		t.Errorf("Expected balance 200, got %f", received.Balance)
	}
	if received.ID != 2 || received.Type != Add {
		t.Errorf("Expected client ID 2, Type Add, got ID %d, Type %v", received.ID, received.Type)
	}
}

func TestFinalServer(t *testing.T) {
	processedRemove := make(chan *Client, 10)
	processedAdd := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go finalServer(processedRemove, processedAdd, &wg)

	// Send a remove client
	clientRemove := &Client{ID: 1, Type: Remove, Balance: 100}
	processedRemove <- clientRemove
	close(processedRemove)

	// Send an add client
	clientAdd := &Client{ID: 2, Type: Add, Balance: 200}
	processedAdd <- clientAdd
	close(processedAdd)

	wg.Wait()

	// Note: Since channels are closed, we need to check the clients were modified
	// But since they are pointers, the original clients should be modified
	if clientRemove.Balance != 150 {
		t.Errorf("Expected remove client balance 150, got %f", clientRemove.Balance)
	}
	if clientRemove.Type != Done {
		t.Errorf("Expected remove client type Done, got %v", clientRemove.Type)
	}

	if clientAdd.Balance != 100 {
		t.Errorf("Expected add client balance 100, got %f", clientAdd.Balance)
	}
	if clientAdd.Type != Done {
		t.Errorf("Expected add client type Done, got %v", clientAdd.Type)
	}
}

func TestRun(t *testing.T) {
	clients := []Client{
		{ID: 1, Type: Remove, Balance: 200},
		{ID: 2, Type: Add, Balance: 100},
		{ID: 3, Type: Remove, Balance: 300},
	}

	Run(clients)

	// Check results
	if clients[0].Balance != 150 { // 200 - 100 = 100, then * 1.5 = 150
		t.Errorf("Expected client 0 balance 150, got %f", clients[0].Balance)
	}
	if clients[0].Type != Done {
		t.Errorf("Expected client 0 type Done, got %v", clients[0].Type)
	}

	if clients[1].Balance != 100 { // 100 + 100 = 200, then / 2 = 100
		t.Errorf("Expected client 1 balance 100, got %f", clients[1].Balance)
	}
	if clients[1].Type != Done {
		t.Errorf("Expected client 1 type Done, got %v", clients[1].Type)
	}

	if clients[2].Balance != 300 { // 300 - 100 = 200, then * 1.5 = 300
		t.Errorf("Expected client 2 balance 300, got %f", clients[2].Balance)
	}
	if clients[2].Type != Done {
		t.Errorf("Expected client 2 type Done, got %v", clients[2].Type)
	}
}

func TestDispatcherMultipleClients(t *testing.T) {
	input := make(chan *Client, 10)
	remove := make(chan *Client, 10)
	add := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go dispatcher(input, remove, add, &wg)

	// Send multiple clients
	clients := []*Client{
		{ID: 1, Type: Remove, Balance: 200},
		{ID: 2, Type: Add, Balance: 100},
		{ID: 3, Type: Remove, Balance: 300},
		{ID: 4, Type: Add, Balance: 50},
	}

	for _, client := range clients {
		input <- client
	}
	close(input)
	wg.Wait()

	// Collect from remove channel
	var removeClients []*Client
	for i := 0; i < 2; i++ {
		select {
		case client := <-remove:
			removeClients = append(removeClients, client)
		default:
			t.Error("Expected more clients in remove channel")
		}
	}

	// Collect from add channel
	var addClients []*Client
	for i := 0; i < 2; i++ {
		select {
		case client := <-add:
			addClients = append(addClients, client)
		default:
			t.Error("Expected more clients in add channel")
		}
	}

	if len(removeClients) != 2 || len(addClients) != 2 {
		t.Errorf("Expected 2 remove and 2 add clients, got %d remove, %d add", len(removeClients), len(addClients))
	}
}

func TestRemoveTaskTypeServerMultiple(t *testing.T) {
	remove := make(chan *Client, 10)
	processedRemove := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go removeTaskTypeServer(remove, processedRemove, &wg)

	clients := []*Client{
		{ID: 1, Type: Remove, Balance: 200},
		{ID: 2, Type: Remove, Balance: 150},
	}

	for _, client := range clients {
		remove <- client
	}
	close(remove)

	wg.Wait()
	close(processedRemove)

	var receivedClients []*Client
	for client := range processedRemove {
		receivedClients = append(receivedClients, client)
	}

	if len(receivedClients) != 2 {
		t.Errorf("Expected 2 clients, got %d", len(receivedClients))
	}

	expectedBalances := []float64{100, 50}
	for i, client := range receivedClients {
		if client.Balance != expectedBalances[i] {
			t.Errorf("Client %d: expected balance %f, got %f", i, expectedBalances[i], client.Balance)
		}
	}
}

func TestAddTaskTypeServerMultiple(t *testing.T) {
	add := make(chan *Client, 10)
	processedAdd := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go addTaskTypeServer(add, processedAdd, &wg)

	clients := []*Client{
		{ID: 1, Type: Add, Balance: 100},
		{ID: 2, Type: Add, Balance: 50},
	}

	for _, client := range clients {
		add <- client
	}
	close(add)

	wg.Wait()
	close(processedAdd)

	var receivedClients []*Client
	for client := range processedAdd {
		receivedClients = append(receivedClients, client)
	}

	if len(receivedClients) != 2 {
		t.Errorf("Expected 2 clients, got %d", len(receivedClients))
	}

	expectedBalances := []float64{200, 150}
	for i, client := range receivedClients {
		if client.Balance != expectedBalances[i] {
			t.Errorf("Client %d: expected balance %f, got %f", i, expectedBalances[i], client.Balance)
		}
	}
}

func TestFinalServerMultiple(t *testing.T) {
	processedRemove := make(chan *Client, 10)
	processedAdd := make(chan *Client, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go finalServer(processedRemove, processedAdd, &wg)

	removeClients := []*Client{
		{ID: 1, Type: Remove, Balance: 100},
		{ID: 3, Type: Remove, Balance: 200},
	}
	addClients := []*Client{
		{ID: 2, Type: Add, Balance: 200},
		{ID: 4, Type: Add, Balance: 400},
	}

	for _, client := range removeClients {
		processedRemove <- client
	}
	close(processedRemove)

	for _, client := range addClients {
		processedAdd <- client
	}
	close(processedAdd)

	wg.Wait()

	// Check remove clients: balance * 1.5, type = Done
	if removeClients[0].Balance != 150 || removeClients[0].Type != Done {
		t.Errorf("Remove client 0: expected balance 150, type Done, got %f, %v", removeClients[0].Balance, removeClients[0].Type)
	}
	if removeClients[1].Balance != 300 || removeClients[1].Type != Done {
		t.Errorf("Remove client 1: expected balance 300, type Done, got %f, %v", removeClients[1].Balance, removeClients[1].Type)
	}

	// Check add clients: balance / 2, type = Done
	if addClients[0].Balance != 100 || addClients[0].Type != Done {
		t.Errorf("Add client 0: expected balance 100, type Done, got %f, %v", addClients[0].Balance, addClients[0].Type)
	}
	if addClients[1].Balance != 200 || addClients[1].Type != Done {
		t.Errorf("Add client 1: expected balance 200, type Done, got %f, %v", addClients[1].Balance, addClients[1].Type)
	}
}

func TestRunEmptySlice(t *testing.T) {
	clients := []Client{}
	Run(clients)
	// Should not panic or error
}

func TestRunOnlyRemove(t *testing.T) {
	clients := []Client{
		{ID: 1, Type: Remove, Balance: 200},
		{ID: 2, Type: Remove, Balance: 300},
	}

	Run(clients)

	if clients[0].Balance != 150 || clients[0].Type != Done {
		t.Errorf("Client 0: expected balance 150, type Done, got %f, %v", clients[0].Balance, clients[0].Type)
	}
	if clients[1].Balance != 300 || clients[1].Type != Done {
		t.Errorf("Client 1: expected balance 300, type Done, got %f, %v", clients[1].Balance, clients[1].Type)
	}
}

func TestRunOnlyAdd(t *testing.T) {
	clients := []Client{
		{ID: 1, Type: Add, Balance: 100},
		{ID: 2, Type: Add, Balance: 50},
	}

	Run(clients)

	if clients[0].Balance != 100 || clients[0].Type != Done {
		t.Errorf("Client 0: expected balance 100, type Done, got %f, %v", clients[0].Balance, clients[0].Type)
	}
	if clients[1].Balance != 75 || clients[1].Type != Done {
		t.Errorf("Client 1: expected balance 75, type Done, got %f, %v", clients[1].Balance, clients[1].Type)
	}
}

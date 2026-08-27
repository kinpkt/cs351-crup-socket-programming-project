package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type LocalState struct {
	Name    string
	Attempt int
}

var (
	currentState = &LocalState{Name: "Waiting...", Attempt: 1}
	subscribers  = make(map[string]*net.UDPAddr) // Client IP:Port -> UDPAddr
	mu           sync.RWMutex
)

func main() {
	port := flag.String("port", "2007", "Station's port (for simulation)")
	stationID := flag.String("station", "1", "The competing station number for the server")
	competitor := flag.String("name", "N/A", "Initial competitor name")
	flag.Parse()

	currentState.Name = *competitor

	addrStr := fmt.Sprintf("localhost:%s", *port)
	addr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		log.Fatal("Couldn't resolve address:", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Listen failed:", err)
	}
	defer conn.Close()

	fmt.Printf("[STATION %s] Server running on UDP %s\n", *stationID, *port)

	controlChan := make(chan string)

	go simulateTimer(conn, *stationID, controlChan)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			controlChan <- strings.TrimSpace(scanner.Text())
		}
	}()

	buffer := make([]byte, 1024)
	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		message := strings.TrimSpace(string(buffer[:n]))
		clientKey := clientAddr.String()

		if strings.HasPrefix(message, "100 FOCUS_STATION") {
			parts := strings.Split(message, " ")
			if len(parts) >= 3 {
				requestedStation := parts[2]

				if requestedStation != *stationID {
					errMsg := fmt.Sprintf("503 SERVICE_UNAVAILABLE This is Station %s, not %s", *stationID, requestedStation)
					conn.WriteToUDP([]byte(errMsg), clientAddr)
					continue
				}

				mu.Lock()
				subscribers[clientKey] = clientAddr
				mu.Unlock()

				resp := fmt.Sprintf("200 ACCEPTED Station: %s Name: %s Attempt: %d", *stationID, currentState.Name, currentState.Attempt)
				conn.WriteToUDP([]byte(resp), clientAddr)
				fmt.Printf("[CLIENT CONNECTED] %s\n", clientKey)
			}
		}
	}
}

func simulateTimer(conn *net.UDPConn, stationID string, controlChan <-chan string) {
	var lastTime float64 = 0.0

	for {
		mu.RLock()
		fmt.Printf("\n[STATION %s] Press ENTER to START Attempt %d for %s...\n", stationID, currentState.Attempt, currentState.Name)
		fmt.Println("(Or type +, /, * and press ENTER to apply a penalty to the PREVIOUS solve)")
		mu.RUnlock()

		for {
			input := <-controlChan
			if input == "" {
				break
			} else if input == "+" {
				lastTime += 2.0
				finishMsg := fmt.Sprintf("201 FINISHED Time: %.2f Status: +2", lastTime)
				broadcast(conn, finishMsg)
				fmt.Printf(">> PENALTY APPLIED: +2 | New Time: %.2f <<\n", lastTime)
			} else if input == "/" {
				finishMsg := "201 FINISHED Time: DNF Status: DNF"
				broadcast(conn, finishMsg)
				fmt.Printf(">> PENALTY APPLIED: DNF <<\n")
			} else if input == "*" {
				finishMsg := "201 FINISHED Time: DNS Status: DNS"
				broadcast(conn, finishMsg)
				fmt.Printf(">> PENALTY APPLIED: DNS <<\n")
			} else if input == "b" {
				mu.Lock()
				if currentState.Attempt > 1 {
					currentState.Attempt--
				}
				mu.Unlock()

				mu.RLock()
				setupMsg := fmt.Sprintf("101 SET_COMPETITOR Name: %s Attempt: %d", currentState.Name, currentState.Attempt)
				mu.RUnlock()
				broadcast(conn, setupMsg)
				fmt.Printf(">> UNDO: Moved back to Attempt %d <<\n", currentState.Attempt)
			} else if input == "r" {
				mu.Lock()
				currentState.Attempt = 1
				mu.Unlock()

				mu.RLock()
				setupMsg := fmt.Sprintf("101 SET_COMPETITOR Name: %s Attempt: %d", currentState.Name, currentState.Attempt)
				mu.RUnlock()
				broadcast(conn, setupMsg)
				fmt.Printf(">> RESET: Cleared session for %s <<\n", currentState.Name)
			} else {
				fmt.Println("Unknown input. Press ENTER to start, or +, /, *, b, r")
			}
		}

		mu.RLock()
		setupMsg := fmt.Sprintf("101 SET_COMPETITOR Name: %s Attempt: %d", currentState.Name, currentState.Attempt)
		mu.RUnlock()
		broadcast(conn, setupMsg)

		fmt.Println(">> TIMER RUNNING! (Press ENTER again to STOP) <<")

		currentTime := 0.0
		ticker := time.NewTicker(10 * time.Millisecond)

	SolveLoop:
		for {
			select {
			case <-ticker.C:
				currentTime += 0.01
				updateMsg := fmt.Sprintf("102 UPDATE Time: %.2f Status: SOLVING", currentTime)
				broadcast(conn, updateMsg)

			case <-controlChan:
				ticker.Stop()
				lastTime = currentTime

				finishMsg := fmt.Sprintf("201 FINISHED Time: %.2f Status: FINISHED", currentTime)
				broadcast(conn, finishMsg)
				fmt.Printf(">> SOLVE FINISHED! Final Time: %.2f <<\n", currentTime)

				break SolveLoop
			}
		}

		mu.Lock()
		if currentState.Attempt < 5 {
			currentState.Attempt++
		} else {
			currentState.Attempt = 1
		}
		mu.Unlock()
	}
}

func broadcast(conn *net.UDPConn, message string) {
	mu.RLock()
	defer mu.RUnlock()

	msgBytes := []byte(message)
	for _, clientAddr := range subscribers {
		conn.WriteToUDP(msgBytes, clientAddr)
	}
}

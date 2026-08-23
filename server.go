package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
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

	addr, err := net.ResolveUDPAddr("udp", "localhost:2007")
	if err != nil {
		log.Fatal("Couldn't resolve address:", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Listen failed:", err)
	}
	defer conn.Close()

	fmt.Printf("[STATION %s] Server running on UDP %s\n", *stationID, *port)

	go simulateTimer(conn, *stationID)

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

				// Reject if the client asked for the wrong station
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

func simulateTimer(conn *net.UDPConn, stationID string) {
	for {
		time.Sleep(4 * time.Second)

		currentTime := 0.0
		targetTime := 4.0 + rand.Float64()*3.0

		mu.RLock()
		setupMsg := fmt.Sprintf("101 SET_COMPETITOR Name: %s Attempt %d", currentState.Name, currentState.Attempt)
		mu.RUnlock()
		broadcast(conn, setupMsg)

		ticker := time.NewTicker(100 * time.Millisecond)
		for range ticker.C {
			currentTime += 0.1
			if currentTime >= targetTime {
				ticker.Stop()
				finishMsg := fmt.Sprintf("201 FINISHED Time: %.3f Status: FINISHED", currentTime)
				broadcast(conn, finishMsg)
				break
			} else {
				updateMsg := fmt.Sprintf("102 UPDATE Time: %.3f Status: SOLVING", currentTime)
				broadcast(conn, updateMsg)
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

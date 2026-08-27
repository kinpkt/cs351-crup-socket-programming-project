package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type StationData struct {
	StationID string
	Message   string
}

type OverlayState struct {
	CompetitorName string
	CurrentAttempt int
	Solves         [5]string
	RunningTime    string
	IsSolving      bool
}

const STATION_COUNT = 8
const BASE_PORT = 2007

func main() {
	dataChannel := make(chan StationData)
	keyboardChannel := make(chan string)

	for i := 0; i <= STATION_COUNT; i++ {
		stationName := fmt.Sprintf("Station %d", i+1)
		serverAddr := fmt.Sprintf("localhost:%d", BASE_PORT+i)

		go listenToServer(stationName, serverAddr, dataChannel)
	}

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			keyboardChannel <- strings.TrimSpace(scanner.Text())
		}
	}()

	currentFocusedStation := "Station 1"
	fmt.Printf("Master Scoreboard started. Focusing on %s\n", currentFocusedStation)

	// Init scoreboard data
	boardState := OverlayState{
		CompetitorName: "Waiting...",
		CurrentAttempt: 1,
		Solves:         [5]string{"-", "-", "-", "-", "-"},
		RunningTime:    "0.00",
		IsSolving:      false,
	}

	obsTicker := time.NewTicker(16 * time.Millisecond)
	stateChanged := true

	for {
		select {
		case data := <-dataChannel:
			if data.StationID == currentFocusedStation {
				if len(data.Message) >= 3 {
					statusCode := data.Message[0:3]

					if statusCode == "101" || statusCode == "200" {
						name, attempt := parseSetupMessage(data.Message)

						// Clear on new competitor or new attempt 1
						if boardState.CompetitorName != name || attempt == 1 {
							boardState.CompetitorName = name
							boardState.Solves = [5]string{"-", "-", "-", "-", "-"}
						}

						// Reverse attempt back
						for i := attempt - 1; i < 5; i++ {
							boardState.Solves[i] = "-"
						}

						boardState.CurrentAttempt = attempt
						boardState.RunningTime = "0.00"
						boardState.IsSolving = false
						stateChanged = true
					} else if statusCode == "102" {
						boardState.RunningTime = extractValue(data.Message, "Time:")
						boardState.IsSolving = true
						stateChanged = true
					} else if statusCode == "201" {
						finalTimeStr := extractValue(data.Message, "Time:")
						status := extractValue(data.Message, "Status:")

						formattedTime := formatTime(finalTimeStr)

						if status == "+2" {
							formattedTime += "+"
						}

						boardState.RunningTime = formattedTime

						if boardState.CurrentAttempt >= 1 && boardState.CurrentAttempt <= 5 {
							boardState.Solves[boardState.CurrentAttempt-1] = formattedTime
						}

						boardState.IsSolving = false
						stateChanged = true
					}
				}
			}

		case <-obsTicker.C:
			if stateChanged {
				writeOverlayFile(boardState)
				stateChanged = false
			}

		case input := <-keyboardChannel:
			if input >= "1" && input <= "9" {
				currentFocusedStation = "Station " + input
				fmt.Printf("\n>>> SWITCHED OBS FEED TO %s <<<\n\n", currentFocusedStation)
			}
		}
	}
}

func writeOverlayFile(state OverlayState) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("COMPETITOR: %s\n", state.CompetitorName))
	sb.WriteString("------------------------\n")

	for i := 0; i < 5; i++ {
		displayTime := state.Solves[i]

		if i+1 == state.CurrentAttempt && state.Solves[i] == "-" {
			displayTime = formatTime(state.RunningTime)
		}

		sb.WriteString(fmt.Sprintf("Solve %d: %s\n", i+1, displayTime))
	}

	if !state.IsSolving {
		if state.CurrentAttempt == 4 {
			sb.WriteString(getBPAWPA(state.Solves))
		} else if state.CurrentAttempt == 5 {
			sb.WriteString(calculateAo5(state.Solves))
		}
	}

	err := os.WriteFile("overlay.txt", []byte(sb.String()), 0644)
	if err != nil {
		log.Println("Error writing to OBS file:", err)
	}
}

// TODO: Test BPA WPA and Ao5
func getBPAWPA(solves [5]string) string {
	sum := 0.0
	maxTime := math.Inf(-1)
	minTime := math.Inf(1)
	DNFDNSCount := 0
	BPAString := ""
	WPAString := ""
	for _, solve := range solves {
		if solve == "-" {
			continue
		}

		cleanSolve := strings.TrimRight(solve, "+")

		val, err := strconv.ParseFloat(cleanSolve, 64)
		if err == nil {
			if val < minTime {
				minTime = val
			}

			if val > maxTime {
				maxTime = val
			}

			sum += val
		} else if cleanSolve == "DNF" || cleanSolve == "DNS" {
			DNFDNSCount++
		}
	}

	if DNFDNSCount >= 2 {
		WPAString = "DNF"
		BPAString = "DNF"
	} else if DNFDNSCount == 1 {
		WPAString = "DNF"
		BPAString = fmt.Sprintf("%.2f", sum/3.0)
	} else {
		WPAString = fmt.Sprintf("%.2f", (sum-minTime)/3.0)
		BPAString = fmt.Sprintf("%.2f", (sum-maxTime)/3.0)
	}

	return fmt.Sprintf("BPA: %s, WPA: %s", formatTime(BPAString), formatTime(WPAString))
}

func calculateAo5(solves [5]string) string {
	sum := 0.0
	maxTime := math.Inf(-1)
	minTime := math.Inf(1)
	DNFDNSCount := 0
	Ao5String := ""
	for _, solve := range solves {
		if solve == "-" {
			continue
		}

		cleanSolve := strings.TrimRight(solve, "+")

		val, err := strconv.ParseFloat(cleanSolve, 64)
		if err == nil {
			if val < minTime {
				minTime = val
			}

			if val > maxTime {
				maxTime = val
			}

			sum += val
		} else if cleanSolve == "DNF" || cleanSolve == "DNS" {
			DNFDNSCount++
		}
	}

	if DNFDNSCount >= 2 {
		Ao5String = "DNF"
	} else if DNFDNSCount == 1 {
		sum -= minTime
		Ao5String = fmt.Sprintf("%.2f", sum/3.0)
	} else {
		sum -= (minTime + maxTime)
		Ao5String = fmt.Sprintf("%.2f", sum/3.0)
	}

	return fmt.Sprintf("Ao5: %s", formatTime(Ao5String))
}

func formatTime(timeStr string) string {
	if timeStr == "DNF" || timeStr == "DNS" {
		return timeStr
	}

	totalSeconds, err := strconv.ParseFloat(timeStr, 64)
	if err != nil {
		fmt.Println("Error during type conversion:", err)
		return "-"
	}

	if totalSeconds < 60 {
		return timeStr
	}

	minutes := int(totalSeconds / 60)
	seconds := totalSeconds - float64(minutes*60)

	return fmt.Sprintf("%02d:%05.2f", minutes, seconds)
}

func extractValue(msg string, key string) string {
	parts := strings.Split(msg, " ")
	for i, part := range parts {
		if part == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return "-"
}

func parseSetupMessage(msg string) (string, int) {
	nameStart := strings.Index(msg, "Name: ")
	attemptStart := strings.Index(msg, " Attempt:")

	name := "Unknown"
	attempt := 1

	if nameStart != -1 && attemptStart != -1 && attemptStart > nameStart {
		name = msg[nameStart+6 : attemptStart]
	}

	attemptStr := extractValue(msg, "Attempt:")
	if parsed, err := strconv.Atoi(attemptStr); err == nil {
		attempt = parsed
	}

	return name, attempt
}

func listenToServer(stationID string, serverAddr string, ch chan<- StationData) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		log.Fatalf("Error resolving %s: %v", stationID, err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Error connecting to %s: %v", stationID, err)
	}
	defer conn.Close()

	reqMsg := fmt.Sprintf("100 FOCUS_STATION %s", strings.Split(stationID, " ")[1])
	conn.Write([]byte(reqMsg))

	buffer := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		ch <- StationData{
			StationID: stationID,
			Message:   string(buffer[:n]),
		}
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//set maximum simulation delay
var Max_Delay = 1500

func main () {

	//Create Switch
	mainSwitchId := 10
	mainSwitch := spawnSwitch(mainSwitchId)

	//Create User connection to switch
	mainSwitch.SetupChanIn <- Frame {
								To: mainSwitchId,
								From: -1,
								Key: cmdConnect,
								Data: ConnectReqPayload(ConnectReq{
																	Type: servUser,
																	Id: -1})}
	connectReply := <- mainSwitch.SetupChanOut
	userChanOut := connectReply.Data.ConnectAns.ToPort
	userChanIn := connectReply.Data.ConnectAns.FromPort

	//ensure that Log_Files directory exists
	if err := os.MkdirAll("Log_Files", 0755); err != nil {
		fmt.Println("Error creating log directory:", err)
		return
	}

	filename := "Log_Files/User_Log.txt"
	logfile, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		fmt.Println("Error opening log file:", err)
		return
	}

	var logWg sync.WaitGroup
	logWg.Add(1)

	//create logger and close before exiting
	WriteToLogFile := make(chan string, 256)
	done := make(chan struct{})

	defer logWg.Wait()
	defer close(WriteToLogFile)

	go func () {
		defer logfile.Close()
		defer logWg.Done()

		for str := range WriteToLogFile {
			logentry := fmt.Sprintf("[%s]\t%s", time.Now().Format("15:04:05.000"),str)
			fmt.Fprintln(logfile, logentry)
		}
	}()
	
	//Listen to user channel in
	go func () {
		for { 
			select {
				case <- done:
					return

				case frame := <- userChanIn:
					cmd := frame.Key

					switch cmd {
						case msgPingReply:
							msg := frame.Data.S
							WriteToLogFile <- msg

						case msgReadReply:
							key := frame.Data.ReadReply.Key
							value := frame.Data.ReadReply.Value
							vrsn := frame.Data.ReadReply.Version
							if vrsn == -1 {
								WriteToLogFile <- fmt.Sprintf("Read Error:\n\tKey = %d not in database.\n", key)
							} else {
								WriteToLogFile <- fmt.Sprintf("Read Reply:\n\tKey = %d, Value = %d, Version = %d\n", key, value, vrsn)
							}

						case msgWriteReply:
							key := frame.Data.WriteReply.Key
							value := frame.Data.WriteReply.Value
							vrsn := frame.Data.WriteReply.Version
							WriteToLogFile <- fmt.Sprintf("Write Reply:\n\tKey = %d, Value = %d, Version = %d\n", key, value, vrsn)
						
						case msgError:
							WriteToLogFile <- frame.Data.S

						case msgCRAQError:
							err := frame.Data.CRAQError
							fmt.Printf("CRAQ Server Error: %s\n", err)

						default:
							kind := frame.Data.Kind
							WriteToLogFile <- fmt.Sprintf("Error: Received a payload of type %d.\n", kind)

					}
			}
		}
	}()
	
	//Specify number of servers in chain
	numServers := 4

	//Create static CRAQ Struct
	CRstruct := make([]int, numServers)
	for i := range CRstruct {
		CRstruct[i] = i
	}

	//create wait group for graceful shutdown
	var wg sync.WaitGroup

	CRAQServerList := make([]CRAQServer, numServers)
	//Create CRAQ servers in chain
	for i := range CRAQServerList {
		mainSwitch.SetupChanIn <- Frame {
									To: mainSwitchId,
									From: -1,
									Key: cmdConnect,
									Data: ConnectReqPayload(ConnectReq{
																		Type: servCRAQ,
																		Id: i})}
		connectReply = <- mainSwitch.SetupChanOut
	
		wg.Add(1)
		CRAQServerList[i] = NewCRAQServer(i, servCRAQ, CRstruct, i, mainSwitchId, connectReply.Data.ConnectAns.FromPort, connectReply.Data.ConnectAns.ToPort)
		spawnCRAQServer(&CRAQServerList[i], &wg)

	}


	WriteToLogFile <- "CRAQ Server successfully started."
	//REPL TO INTERACT
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Simulated CRAQ Server!")
	fmt.Println("Recognized Commands:")
	fmt.Println("\thelp")
	fmt.Println("\tping")
	fmt.Println("\tread")
	fmt.Println("\twrite")
	fmt.Println("\tquit")
	fmt.Println("")

	for {
		fmt.Print("Enter a command: ")
		input, _ := reader.ReadString('\n')
		cmd := strings.TrimSpace(input)

		switch cmd {
		case "quit":
			fmt.Println("Shutting Down Servers!\nGoodbye!\n")
			//Disconnect user channel
			userChanOut <- Frame{
								To:   mainSwitchId,
								From: -1,
								Key:  cmdDisconnect,
								Data: DisconnectReqPayload(DisconnectReq{Id: -1}),
							}
			
			close(done)
			//Shut down all servers and switches gracefully, then return
			for i := range CRAQServerList {
				CRAQServerList[i].Power <- struct{}{}
			}

			wg.Wait()
			mainSwitch.Power <- struct{}{}
			WriteToLogFile <- "CRAQ Server successfully shutdown."
			time.Sleep(100*time.Millisecond)
			return
		
		case "help":
			fmt.Println("Recognized Commands:")
			fmt.Println("\thelp")
			fmt.Println("\tping")
			fmt.Println("\tread")
			fmt.Println("\twrite")
			fmt.Println("\tquit")
			fmt.Println("")

		case "ping":
			fmt.Print("Enter a server: ")
			s_str, _ := reader.ReadString('\n')
			s, err := strconv.Atoi(strings.TrimSpace(s_str))
			if err != nil {fmt.Println("Server error:  Given server id is not an integer!\n"); continue}

			userChanOut <- Frame {
								To: s,
								From: -1,
								Key: cmdPing,
								Data: StringPayload("")}
			fmt.Println("")
		
		case "read":
			fmt.Print("Enter a server: ")
			s_str, _ := reader.ReadString('\n')
			s, err := strconv.Atoi(strings.TrimSpace(s_str))
			if err != nil {fmt.Println("Server error:  Given server id is not an integer!\n"); continue}

			fmt.Print("Key: ")
			k_input, _ := reader.ReadString('\n')
			k_str := strings.TrimSpace(k_input)
			k, err := strconv.Atoi(k_str)
			if err != nil {fmt.Println("Key error:  Given key is not an integer!\n"); continue}
			
			
			newReadReq := ReadRequest {
								From: -1,
								Key: k}
			userChanOut <- Frame {
								To: s,
								From: -1,
								Key: cmdRead,
								Data: ReadRequestPayload(newReadReq)}
			fmt.Println("")
			

		case "write":
			fmt.Print("Key: ")
			k_input, _ := reader.ReadString('\n')
			k_str := strings.TrimSpace(k_input)
			k, err := strconv.Atoi(k_str)
			if err != nil {fmt.Println("Key error:  Given key is not an integer!\n"); continue}
		
			fmt.Print("Value: ")
			v_input, _ := reader.ReadString('\n')
			v_str := strings.TrimSpace(v_input)
			v, err := strconv.Atoi(v_str)
				if err != nil {fmt.Println("Value error:  Given value is not an integer!\n"); continue}
			
			newWriteReq := WriteRequest{
									From: -1,
									Key: k,
									Value: v}
			userChanOut <- Frame {
								To: CRstruct[0],
								From: -1,
								Key: cmdWrite,
								Data: WriteRequestPayload(newWriteReq)}
			fmt.Println("")

		default:
			fmt.Println("Bad Command! Try again.\n")
			fmt.Println("Recognized Commands:")
			fmt.Println("\thelp")
			fmt.Println("\tping")
			fmt.Println("\tread")
			fmt.Println("\twrite")
			fmt.Println("\tquit")
			fmt.Println("")
		}
	}
}

package main

import (
	//"bufio"
	"fmt"
	"os"
	//"strconv"
	//"strings"
	"sync"
	"time"
)

//set maximum simulation delay
var Max_Delay = 100

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

				case frame, ok := <- userChanIn:
					if !ok {
						return
					}
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
								WriteToLogFile <- fmt.Sprintf("Read Reply from Server %d:\n\tKey = %d, Value = %d, Version = %d\n", frame.From, key, value, vrsn)
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

	//Populate servers with data (delays added to be consistent with delays in system)
	fmt.Println("Writing to CRAQ System.")
	WriteToLogFile <- "Starting Bulk Write"
	for i := 0; i < 5; i++ {
		fmt.Println("\tSending write request ", i)
		newWriteReq := WriteRequest{
									From: -1,
									Key: i,
									Value: 100*(i+1)}
		userChanOut <- Frame {
								To: CRstruct[0],
								From: -1,
								Key: cmdWrite,
								Data: WriteRequestPayload(newWriteReq)}
		time.Sleep(20*time.Millisecond)
	}
	
	time.Sleep(2*time.Second)

	fmt.Println("Testing Reads (unique key to each server).")
	WriteToLogFile <- "Starting First Read Test (unique key to each server)"
	for i := 0; i < 4; i++ {
		fmt.Println("\tSending read request ", i)
		newReadReq := ReadRequest {
								From: -1,
								Key: i}
		userChanOut <- Frame {
							To: i,
							From: -1,
							Key: cmdRead,
							Data: ReadRequestPayload(newReadReq)}
		time.Sleep(10*time.Millisecond)
	}
	time.Sleep(2*time.Second)

	fmt.Println("Testing Reads (same key to each server).")
	WriteToLogFile <- "Starting Second Read Test (same key to each server)"
	for i := 0; i < 4; i++ {
		fmt.Println("\tSending read request ", i)
		newReadReq := ReadRequest {
								From: -1,
								Key: 2}
		userChanOut <- Frame {
							To: i,
							From: -1,
							Key: cmdRead,
							Data: ReadRequestPayload(newReadReq)}
		time.Sleep(10*time.Millisecond)
	}

	time.Sleep(2*time.Second)

	fmt.Println("Testing simple write with overlapping reads.")
	WriteToLogFile <- "Starting Third Read Test (simple write with overlapping reads)"
	//write new value for key 0
	newWriteReq := WriteRequest{
							From: -1,
							Key: 0,
							Value: -100}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 0 -> -100"

	//immediately read key 0 from 
	newReadReq := ReadRequest {
							From: -1,
							Key: 0}
	userChanOut <- Frame {
						To: 3,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 0 from server 3"

	//wait and try to read key 0 from head and server 1
	time.Sleep(50*time.Millisecond)
	newReadReq = ReadRequest {
							From: -1,
							Key: 0}
	userChanOut <- Frame {
						To: 0,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 0 from server 0"
	
	time.Sleep(50*time.Millisecond)
	newReadReq = ReadRequest {
							From: -1,
							Key: 0}
	userChanOut <- Frame {
						To: 1,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 0 from server 1"

	time.Sleep(2*time.Second)

	fmt.Println("Testing read after multiple writes to the same key.")
	WriteToLogFile <- "Starting read test after multiple writes to the same key"

	newWriteReq = WriteRequest{
							From: -1,
							Key: 1,
							Value: -200}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 1 -> -200"

	time.Sleep(25*time.Millisecond)

	newWriteReq = WriteRequest{
							From: -1,
							Key: 1,
							Value: -300}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 1 -> -300"

	time.Sleep(25*time.Millisecond)
	
	newWriteReq = WriteRequest{
							From: -1,
							Key: 1,
							Value: -400}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 1 -> -400"

	time.Sleep(50*time.Millisecond)

	newReadReq = ReadRequest {
							From: -1,
							Key: 1}
	userChanOut <- Frame {
						To: 0,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 1 to server 0"
	
	time.Sleep(2*time.Second)

	fmt.Println("Testing interleaved reads and writes to the same key (round 1).")
	WriteToLogFile <- "Starting first interleaved read and write test to the same key"

	//write to key 2, wait for 20ms
	newWriteReq = WriteRequest{
							From: -1,
							Key: 2,
							Value: -1000}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 2 -> -1000"

	time.Sleep(20*time.Millisecond)

	//write to key 2, wait for 20 ms
	newWriteReq = WriteRequest{
							From: -1,
							Key: 2,
							Value: -2000}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 2 -> -2000"

	time.Sleep(50*time.Millisecond)

	//read key 2 from server 2, wait 20ms
	newReadReq = ReadRequest {
							From: -1,
							Key: 2}
	userChanOut <- Frame {
						To: 2,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 2 to server 2"

	time.Sleep(20*time.Millisecond)

	//read key 2 from server 1, wait 20ms
	newReadReq = ReadRequest {
							From: -1,
							Key: 2}
	userChanOut <- Frame {
						To: 1,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 2 to server 1"

	time.Sleep(50*time.Millisecond)

	//write to key 2, wait 20ms
	newWriteReq = WriteRequest{
							From: -1,
							Key: 2,
							Value: -3000}
	userChanOut <- Frame {
						To: CRstruct[0],
						From: -1,
						Key: cmdWrite,
						Data: WriteRequestPayload(newWriteReq)}
	WriteToLogFile <- "Issued write: key 2 -> -3000"

	time.Sleep(20*time.Millisecond)

	//read key 2 from server 0, wait 20ms
	newReadReq = ReadRequest {
							From: -1,
							Key: 2}
	userChanOut <- Frame {
						To: 0,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 2 to server 0"

	time.Sleep(50*time.Millisecond)

	//read key 2 from server 2
	newReadReq = ReadRequest {
							From: -1,
							Key: 2}
	userChanOut <- Frame {
						To: 3,
						From: -1,
						Key: cmdRead,
						Data: ReadRequestPayload(newReadReq)}
	WriteToLogFile <- "Issued read: key 2 to server 3"

	time.Sleep(2*time.Second)

	//read key 2 from all servers
	WriteToLogFile <- "Issuing reads for key 2 to all servers"
	for i := 0; i < 4; i++ {
		fmt.Println("\tSending read request ", i)
		newReadReq := ReadRequest {
								From: -1,
								Key: 2}
		userChanOut <- Frame {
							To: i,
							From: -1,
							Key: cmdRead,
							Data: ReadRequestPayload(newReadReq)}
		time.Sleep(10*time.Millisecond)
	}

	time.Sleep(2*time.Second)

	fmt.Println("Testing interleaved reads and writes to the same key (round 2).")
	WriteToLogFile <- "Starting second interleaved read and write test to the same key"

	// write key 3 -> 4000
	newWriteReq = WriteRequest{
		From:  -1,
		Key:   3,
		Value: 4000,
	}
	userChanOut <- Frame{
		To:   CRstruct[0],
		From: -1,
		Key:  cmdWrite,
		Data: WriteRequestPayload(newWriteReq),
	}
	WriteToLogFile <- "Issued write: key 3 -> 4000"

	time.Sleep(50 * time.Millisecond)

	// read from server 3
	newReadReq = ReadRequest{
		From: -1,
		Key:  3,
	}
	userChanOut <- Frame{
		To:   3,
		From: -1,
		Key:  cmdRead,
		Data: ReadRequestPayload(newReadReq),
	}
	WriteToLogFile <- "Issued read: key 3 to server 3"

	time.Sleep(20 * time.Millisecond)

	// read from server 1
	newReadReq = ReadRequest{
		From: -1,
		Key:  3,
	}
	userChanOut <- Frame{
		To:   1,
		From: -1,
		Key:  cmdRead,
		Data: ReadRequestPayload(newReadReq),
	}
	WriteToLogFile <- "Issued read: key 3 to server 1"

	time.Sleep(50 * time.Millisecond)

	// write key 3 -> 5000
	newWriteReq = WriteRequest{
		From:  -1,
		Key:   3,
		Value: 5000,
	}
	userChanOut <- Frame{
		To:   CRstruct[0],
		From: -1,
		Key:  cmdWrite,
		Data: WriteRequestPayload(newWriteReq),
	}
	WriteToLogFile <- "Issued write: key 3 -> 5000"

	time.Sleep(10 * time.Millisecond)

	// write key 3 -> 6000 (close succession)
	newWriteReq = WriteRequest{
		From:  -1,
		Key:   3,
		Value: 6000,
	}
	userChanOut <- Frame{
		To:   CRstruct[0],
		From: -1,
		Key:  cmdWrite,
		Data: WriteRequestPayload(newWriteReq),
	}
	WriteToLogFile <- "Issued write: key 3 -> 6000"

	time.Sleep(30 * time.Millisecond)

	// read from server 0 (should reflect stacked-write behavior)
	newReadReq = ReadRequest{
		From: -1,
		Key:  3,
	}
	userChanOut <- Frame{
		To:   0,
		From: -1,
		Key:  cmdRead,
		Data: ReadRequestPayload(newReadReq),
	}
	WriteToLogFile <- "Issued read: key 3 to server 0"

	time.Sleep(2 * time.Second)

	// final convergence check
	WriteToLogFile <- "Issuing reads for key 3 to all servers"
	for i := 0; i < 4; i++ {
		fmt.Println("\tSending read request ", i)
		newReadReq = ReadRequest{
			From: -1,
			Key:  3,
		}
		userChanOut <- Frame{
			To:   i,
			From: -1,
			Key:  cmdRead,
			Data: ReadRequestPayload(newReadReq),
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(5*time.Second)
	fmt.Println("Stress test exiting.")
	close(done)
}
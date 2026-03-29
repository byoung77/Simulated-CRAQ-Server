package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

//constants
const (
	cmdCRAQCommit = "commit_to_CRAQ_table"
	cmdCRAQWrite = "write_to_CRAQ_server"
	cmdPing = "ping" 
	cmdRead = "read"
	cmdWrite = "write"
	cmdUpdateCRAQStructure = "update_CRAQ_struct"
	msgPingReply = "ping_reply"
	msgReadReply = "read_reply_from CRAQ"
	msgWriteReply = "write_reply_from CRAQ"
	msgCRAQError = "craq_error"
)

//server structs
type CRAQServer struct {
	Id int
	Type string
	CRstruct []int
	CRstructIdx int
	SwitchId int
	NetworkChanIn chan Frame
	NetworkChanOut chan Frame
	Power chan struct{}
	isHead bool
	isTail bool
	Prev int
	Next int
}

//server constructor
func NewCRAQServer(Id int, Type string, CRstruct []int, CRstructIdx int, SwitchId int, NetworkChanIn chan Frame,
	NetworkChanOut chan Frame) CRAQServer {
	
	if len(CRstruct) < 2 {
		panic("invalid CRstruct")
	}

	if CRstructIdx < 0 || CRstructIdx >= len(CRstruct) {
		panic("invalid CRstructIdx")
	}
		
	
	isHead := false
	if CRstructIdx == 0 {
		isHead = true
	}

	isTail := false
	if CRstructIdx == len(CRstruct)-1 {
		isTail = true
	}

	prev := -1
	if !isHead {
		prev = CRstruct[CRstructIdx-1]
	}

	next := -1
	if !isTail {
		next = CRstruct[CRstructIdx+1]
	}

	power := make(chan struct{})

	return CRAQServer{
		Id: Id,
		Type: Type,
		CRstruct: CRstruct,
		CRstructIdx: CRstructIdx,
		SwitchId: SwitchId,
		NetworkChanIn: NetworkChanIn,
		NetworkChanOut: NetworkChanOut,
		Power: power,
		isHead: isHead,
		isTail: isTail,
		Prev: prev,
		Next: next,	
	}
}

type Frame struct {
	To int
	From int
	Key string
	Data Payload
}


//Payloads
type PayloadKind int
const (
	PayloadNone PayloadKind = iota
	PayloadCRAQLog
	PayloadInt
	PayloadString
	PayloadPair
	PayloadCRAQCommitReq
	PayloadWriteReply
	PayloadWriteRequest
	PayloadReadReply
	PayloadReadRequest
	PayloadPort
    PayloadRoute
	PayloadConnectReq
	PayloadConnectAns
	PayloadDisconnectReq
	PayloadCRAQError
	PayloadCRAQStruct
)

type Payload struct{
	Kind PayloadKind
	CRAQLog CRAQLogEntry
	I int
	S string
	Pair [2]int
	CRAQCommit CommitRequest
	WriteReply WriteReply
	WriteRequest WriteRequest
	ReadReply ReadReply
	ReadRequest ReadRequest
	Port Port
	Route map[int]int
	ConnectReq ConnectReq
	ConnectAns ConnectAns
	DisconnectReq DisconnectReq
	CRAQError string
	CRAQStruct CRAQStruct
}

func NoPayload() Payload {
	return Payload{Kind: PayloadNone}
}

func IntPayload(v int) Payload {
	return Payload{Kind: PayloadInt, I: v}
}

func StringPayload(s string) Payload {
	return Payload{Kind: PayloadString, S: s}
}

func CRAQLogPayload(log CRAQLogEntry) Payload {
	return Payload{Kind: PayloadCRAQLog, CRAQLog: log}
}

func PairPayload(k, v int) Payload {
	return Payload{Kind: PayloadPair, Pair: [2]int{k, v}}
}

func CRAQCommitPayload(comm CommitRequest) Payload {
	return Payload{Kind: PayloadCRAQCommitReq, CRAQCommit: comm}
}

func WriteReplyPayload(reply WriteReply) Payload {
	return Payload{Kind: PayloadWriteReply, WriteReply: reply}
}

func WriteRequestPayload(writeReq WriteRequest) Payload {
	return Payload{Kind: PayloadWriteRequest, WriteRequest: writeReq}
}

func ReadReplyPayload(reply ReadReply) Payload {
	return Payload{Kind: PayloadReadReply, ReadReply: reply}
}

func ReadRequestPayload(readReq ReadRequest) Payload {
	return Payload{Kind: PayloadReadRequest, ReadRequest: readReq}
}

func PortPayload(p Port) Payload {
	return Payload{Kind: PayloadPort, Port: p}
}

func RoutePayload(r map[int]int) Payload {
	return Payload{Kind: PayloadRoute, Route: r}
}

func ConnectReqPayload(r ConnectReq) Payload {
	return Payload{Kind: PayloadConnectReq, ConnectReq: r}
}

func ConnectAnsPayload(a ConnectAns) Payload {
	return Payload{Kind: PayloadConnectAns, ConnectAns: a}
}

func DisconnectReqPayload(r DisconnectReq) Payload {
	return Payload{Kind: PayloadDisconnectReq, DisconnectReq: r}
}

func CRAQErrorPayload(err string) Payload {
	return Payload{Kind: PayloadCRAQError, CRAQError: err}
}

func CRAQStructPayload(CRAQStruct CRAQStruct) Payload {
	return Payload{Kind: PayloadCRAQStruct, CRAQStruct: CRAQStruct}
}


//CRAQ Structs

type CommitRequest struct {
	Key int
	Version int
}

type CRAQLogEntry struct {
	From int
	Key int
	Value int
	Version int
}

type CRAQTableEntry struct {
	Key int
	Value int
	Dirty bool
	Version int
	WaitList []int
}

type ReadReply struct {
	Key int
	Value int
	Version int
}

type ReadRequest struct {
	From int
	Key int
}

type WriteReply struct {
	Key int
	Value int
	Version int
}

type WriteRequest struct {
	From int
	Key int
	Value int
}

type CRAQStruct struct {
	CRstruct []int
	CRstructIdx int
}

//func to simulate network delays
func RandDelay (min, max int) time.Duration {
	if min < 0 {min = 0}
	if max <= min {max = min + 100}

	diff := max - min
	if diff < 100 {diff = 100}
	return time.Duration(min + rand.Intn(diff)) * time.Millisecond
}

//function that creates a server instance

func spawnCRAQServer(srvr *CRAQServer, wg *sync.WaitGroup) {
	//ensure Log_Files directory exists
	os.MkdirAll("Log_Files", 0755)

	//create log file
	filename := fmt.Sprintf("Log_Files/CRAQLog_Server_%d.txt", srvr.Id)
	logfile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)

	timeOn := time.Now()

	//create data table and CRAQ log
	DataTable := make(map[int]CRAQTableEntry)
	CRAQLog := make(map[int][]CRAQLogEntry)

	//create server_id string
	UpdateServerIdString := func() string {
		new_server_id := fmt.Sprintf("CRAQLog Server %d", srvr.Id)
		if srvr.isHead {
			new_server_id = new_server_id + " (head)"
		}

		if srvr.isTail {
			new_server_id = new_server_id + " (tail)"
		}
		return new_server_id
	}

	server_id := UpdateServerIdString()
		

	//create logger
	WriteToLogFile := make(chan string, 500)
	go func () {
		defer logfile.Close()
		for str := range WriteToLogFile {
			logentry := fmt.Sprintf("[%s]\t%s", time.Now().Format("15:04:05.000"),str)
			fmt.Fprintln(logfile, logentry)
		}
	}()

	//common control protocols
	UpdateChainStructure := func (CRstruct []int, CRstructIdx int) bool {

		if len(CRstruct) < 2 {
			return false
		}

		if CRstructIdx < 0 || CRstructIdx >= len(CRstruct) {
			return false
		}

		if CRstruct[CRstructIdx] != srvr.Id {
			return false
		}

		srvr.CRstruct = CRstruct
		srvr.CRstructIdx = CRstructIdx
			
		
		isHead := false
		if CRstructIdx == 0 {
			isHead = true
		}

		isTail := false
		if CRstructIdx == len(CRstruct)-1 {
			isTail = true
		}

		prev := -1
		if !isHead {
			prev = CRstruct[CRstructIdx-1]
		}

		next := -1
		if !isTail {
			next = CRstruct[CRstructIdx+1]
		}

		srvr.isHead = isHead
		srvr.isTail = isTail
		srvr.Prev = prev
		srvr.Next = next

		server_id = UpdateServerIdString()

		return true
	}

	HandlePingReq := func (to int) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received ping request from server %d.\n", srvr.Id, to)
		
		timeUp := time.Since(timeOn)
		pingReply := fmt.Sprintf("%s: Time up: %2f seconds.\n",server_id, timeUp.Seconds()) 

		srvr.NetworkChanOut <- Frame {
									To: to,
									From: srvr.Id,
									Key: msgPingReply,
									Data: StringPayload(pingReply)}
		return
	}

	HandlePowerDown := func () {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received shutdown request.\n", srvr.Id)
		srvr.NetworkChanOut <- Frame {
									To: srvr.SwitchId,
									From: srvr.Id,
									Key: cmdDisconnect,
									Data: DisconnectReqPayload(DisconnectReq{Id: srvr.Id})}

		timeUp := time.Since(timeOn)
		WriteToLogFile <- fmt.Sprintf("%s: Total Time up: %2f seconds.\n",server_id, timeUp.Seconds())
		return
	}

	//Head Specific Protocols

	HandleHeadWriteReq := func(writeReq WriteRequest){
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received write request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		from := writeReq.From
		key := writeReq.Key
		value := writeReq.Value
		//Check if entry is in DataTable
			//If no - create initial entry with version 0 - set Dirty = true
			//If yes - set Dirty = true
		//Assign version number and create LogEntry
		entry, ok := DataTable[key]
		newLogEntry := CRAQLogEntry{}
		if !ok {
			DataTable[key] = CRAQTableEntry {
											Key: key,
											Value: value,
											Dirty: true,
											Version: 0}
			newLogEntry = CRAQLogEntry {
										From: from,
										Key: key,
										Value: value,
										Version: 1}
		} else {
			DataTable[key] = CRAQTableEntry {
											Key: entry.Key,
											Value: entry.Value,
											Dirty: true,
											Version: entry.Version,
											WaitList: entry.WaitList}
			version := -1
			if len(CRAQLog[key]) == 0 {
				version = entry.Version + 1
			} else {
				version = CRAQLog[key][len(CRAQLog[key])-1].Version + 1
			}

			newLogEntry = CRAQLogEntry {
										From: from,
										Key: key,
										Value: value,
										Version: version}
		}		
		//Add LogEntry to Log
		CRAQLog[key] = append(CRAQLog[key], newLogEntry)
		//Pass LogEntry to next server in chain
		newFrame := Frame {
						To: srvr.Next,
						From: srvr.Id,
						Key: cmdCRAQWrite,
						Data: CRAQLogPayload(newLogEntry)}
		srvr.NetworkChanOut <- newFrame
		return
	}

	HandleHeadCommitReq := func(commitReq CommitRequest, from int){
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received commit request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		key := commitReq.Key
		version := commitReq.Version
		//Look up log entry in log (indexed by key)
		//Save relevant Log entry.  Discard all log entries with lower version number
		logs := CRAQLog[key]
		log := CRAQLogEntry{ Version: -1 }
		for i:=0; i<len(logs); i++ {
			if logs[i].Version == version {
				log = logs[i]
				CRAQLog[key] = CRAQLog[key][i+1:]
				break
			}
		}
		if log.Version == -1 { 
			WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received bad commit request. Version %d not found for Key %d.\n", srvr.Id, version, key)

			newFrame := Frame {
							To: from,
							From: srvr.Id,
							Key: msgCRAQError,
							Data: CRAQErrorPayload(fmt.Sprintf("Bad commit request. Version %d not found for Key %d.", version, key))}
			srvr.NetworkChanOut <- newFrame
			return 
		}
		//Update Value in Table to one given in log entry and update version
		replyTo := log.From
		newTableEntry := CRAQTableEntry{
									Key: log.Key,
									Value: log.Value,
									Dirty: true,
									Version: log.Version,
									WaitList: DataTable[key].WaitList}
		//If log is empty for key, set Dirty = false in Table and reply to anyone waiting for read (this should be improved to something non-blocking)
		if len(CRAQLog[key]) == 0 {
			newTableEntry.Dirty = false
			value := newTableEntry.Value
			vrsn := newTableEntry.Version
			L := len(newTableEntry.WaitList)

			for i:=0; i < L; i++ {
				to := newTableEntry.WaitList[i]
				go func(to int) {
					time.Sleep(10 * time.Millisecond)
				
					newReadReply := ReadReply {
											Key: key,
											Value: value,
											Version: vrsn}
					newFrame := Frame {
									To: to,
									From: srvr.Id,
									Key: msgReadReply,
									Data: ReadReplyPayload(newReadReply)}
					srvr.NetworkChanOut <- newFrame
				}(to)
			}
			newTableEntry.WaitList = newTableEntry.WaitList[:0]
			delete(CRAQLog, key)
		}
		DataTable[key] = newTableEntry
		//Reply on WriteReplyChannel
		newWriteReply := WriteReply {
									Key: DataTable[key].Key,
									Value: DataTable[key].Value,
									Version: DataTable[key].Version}
		srvr.NetworkChanOut <- Frame {
									To: replyTo,
									From: srvr.Id,
									Key: msgWriteReply,
									Data: WriteReplyPayload(newWriteReply)}
		return
	}

	//Tail Specific Protocols

	HandleTailWriteReq := func(writeReq CRAQLogEntry) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received write request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		key := writeReq.Key
		value := writeReq.Value
		vrsn := writeReq.Version

		CRAQLog[key] = append(CRAQLog[key], writeReq)
		DataTable[key] = CRAQTableEntry{
									Key: key,
									Value: value,
									Dirty: false,
									Version: vrsn}
		newCommitReq := CommitRequest {
									Key: key,
									Version: vrsn}
		newFrame := Frame {
						To: srvr.Prev,
						From: srvr.Id,
						Key: cmdCRAQCommit,
						Data: CRAQCommitPayload(newCommitReq)}
		srvr.NetworkChanOut <- newFrame
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d (tail) sending commit back up chain.\n", srvr.Id)
		return
	}

	HandleTailReadReq := func(readReq ReadRequest) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received read request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		key := readReq.Key
		//Look up key in Table.  If not present, report version as -1
		entry, ok := DataTable[key]
		if !ok {
			newReadReply := ReadReply {
									Key: key,
									Value: 0,
									Version: -1}

			srvr.NetworkChanOut <- Frame {
										To: readReq.From,
										From: srvr.Id,
										Key: msgReadReply,
										Data: ReadReplyPayload(newReadReply)}
			
			return
		}
		//Tail is never dirty			
		newReadReply :=  ReadReply {
								Key: key,
								Value: entry.Value,
								Version: entry.Version}

		srvr.NetworkChanOut <- Frame {
									To: readReq.From,
									From: srvr.Id,
									Key: msgReadReply,
									Data: ReadReplyPayload(newReadReply)}
		return			
	}

	//Generic Node Protocols

	HandleCRAQWriteReq := func(writeReq CRAQLogEntry) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received write request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		key := writeReq.Key
		entry, ok := DataTable[key]
		if !ok {
			DataTable[key] = CRAQTableEntry {
										Key: key,
										Value: writeReq.Value,
										Dirty: true,
										Version: 0}
		} else {
			DataTable[key] = CRAQTableEntry {
										Key: key,
										Value: entry.Value,
										Dirty: true,
										Version: entry.Version,
										WaitList: entry.WaitList}
		}
		CRAQLog[key] = append(CRAQLog[key], writeReq)
		newFrame := Frame {
						To: srvr.Next,
						From: srvr.Id,
						Key: cmdCRAQWrite,
						Data: CRAQLogPayload(writeReq)}
		srvr.NetworkChanOut <- newFrame
		return
	}

	HandleCRAQCommitReq := func(commitReq CommitRequest, from int) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received commit request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))

		key := commitReq.Key
		version := commitReq.Version
		//Look up log entry in log (indexed by key)
		//Save relevant Log enrty.  Discard all log entries with lower version number
		logs := CRAQLog[key]
		log := CRAQLogEntry{ Version: -1 }
		for i:=0; i<len(logs); i++ {
			if logs[i].Version == version {
				log = logs[i]
				CRAQLog[key] = CRAQLog[key][i+1:]
				break
			}
		}
		if log.Version == -1 { 
			WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received bad commit request. Version %d not found for Key %d.\n", srvr.Id, version, key)

			newFrame := Frame {
							To: from,
							From: srvr.Id,
							Key: msgCRAQError,
							Data: CRAQErrorPayload(fmt.Sprintf("Bad commit request. Version %d not found for Key %d.", version, key))}
			srvr.NetworkChanOut <- newFrame
			return 
		}
		//Update Value in Table to one given in log entry and update version
		newTableEntry := CRAQTableEntry{
									Key: log.Key,
									Value: log.Value,
									Dirty: true,
									Version: log.Version,
									WaitList: DataTable[key].WaitList}
		//If log is empty for key, set Dirty = false in Table and reply to anyone waiting for read (this should be improved to something non-blocking)
		if len(CRAQLog[key]) == 0 {
			newTableEntry.Dirty = false
			value := newTableEntry.Value
			vrsn := newTableEntry.Version
			L := len(newTableEntry.WaitList)

			for i:=0; i < L; i++ {
				to := newTableEntry.WaitList[i]
				go func(to int) {
					time.Sleep(10 * time.Millisecond)
				
					newReadReply := ReadReply {
											Key: key,
											Value: value,
											Version: vrsn}
					newFrame := Frame {
									To: to,
									From: srvr.Id,
									Key: msgReadReply,
									Data: ReadReplyPayload(newReadReply)}
					srvr.NetworkChanOut <- newFrame
				}(to)
			}
			newTableEntry.WaitList = newTableEntry.WaitList[:0]
			delete(CRAQLog, key)
		}
		DataTable[key] = newTableEntry
		srvr.NetworkChanOut <- Frame {
									To: srvr.Prev,
									From: srvr.Id,
									Key: cmdCRAQCommit,
									Data: CRAQCommitPayload(commitReq)}
		return
	}

	HandleCRAQReadReq := func(readReq ReadRequest) {
		WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received read request.\n", srvr.Id)
		time.Sleep(RandDelay(0,Max_Delay))
		key := readReq.Key
		//Look up key in Table.  If not present, report version as -1
		entry, ok := DataTable[key]
		if !ok {
			newReadReply := ReadReply {
									Key: key,
									Value: 0,
									Version: -1}

			srvr.NetworkChanOut <- Frame {
										To: readReq.From,
										From: srvr.Id,
										Key: msgReadReply,
										Data: ReadReplyPayload(newReadReply)}
			
			return
		}
		//If dirty = false - reply
		//If dirty = true - add to wait list
		if entry.Dirty {
			entry.WaitList = append(entry.WaitList, readReq.From)
			DataTable[key] = entry
			return
		} else {
			newReadReply :=  ReadReply {
									Key: key,
									Value: entry.Value,
									Version: entry.Version}

			srvr.NetworkChanOut <- Frame {
										To: readReq.From,
										From: srvr.Id,
										Key: msgReadReply,
										Data: ReadReplyPayload(newReadReply)}
			return
		}
	}
	
	go func(){
		defer close(WriteToLogFile)
		defer wg.Done()
		//Log that server was started successfully
		WriteToLogFile <- fmt.Sprintf("%s started at time %s.\n", server_id, timeOn.Format("15:04:05"))

		for {
			select {
				case frame := <- srvr.NetworkChanIn:
					if frame.To != srvr.Id { 
						continue 
					}

					cmd := frame.Key
					switch cmd {
						case cmdPing:
							HandlePingReq(frame.From)

						case cmdUpdateCRAQStructure:
							update := frame.Data.CRAQStruct
							//ignore bad requests
							ok := UpdateChainStructure(update.CRstruct, update.CRstructIdx)
							if ok {
								WriteToLogFile <- fmt.Sprintf("CRAQ Server %d updated chain structure: idx=%d, struct=%v\n",
									srvr.Id, srvr.CRstructIdx, srvr.CRstruct)
							} else {
								newFrame := Frame {
									To: frame.From,
									From: srvr.Id,
									Key: msgCRAQError,
									Data: CRAQErrorPayload("Invalid CRAQ Structure.")}
								srvr.NetworkChanOut <- newFrame
							}

						case cmdWrite:
							if srvr.isHead {
								newWriteReq := frame.Data.WriteRequest
								HandleHeadWriteReq(newWriteReq)
							} else {
								newFrame := Frame {
									To: frame.From,
									From: srvr.Id,
									Key: msgCRAQError,
									Data: CRAQErrorPayload("Writes must go to head node.")}
								srvr.NetworkChanOut <- newFrame
							}
						
						case cmdCRAQWrite:
							// NOTE: Defensive protocol checks.
							// These cases should not occur in the current static chain configuration.
							// They guard against invalid message routing and become important when
							// dynamic reconfiguration (UpdateChainStructure) is fully exercised.
							if srvr.isHead {
								//Head shouldn't get these requests
								continue
							} else if srvr.isTail {
								if frame.From != srvr.Prev{
									continue
								}
								newCRAQLogEntry := frame.Data.CRAQLog
								HandleTailWriteReq(newCRAQLogEntry)
							} else {
								if frame.From != srvr.Prev{
									continue
								}
								newCRAQLogEntry := frame.Data.CRAQLog
								HandleCRAQWriteReq(newCRAQLogEntry)
							}
							
						case cmdCRAQCommit:
							// NOTE: Defensive protocol checks.
							// These cases should not occur in the current static chain configuration.
							// They guard against invalid message routing and become important when
							// dynamic reconfiguration (UpdateChainStructure) is fully exercised.
							if srvr.isHead {
								if frame.From != srvr.Next{
									continue
								}
								newCommitReq := frame.Data.CRAQCommit
								HandleHeadCommitReq(newCommitReq, frame.From)
							} else if srvr.isTail {
								//tail shouldn't get these requests
								continue 
							} else {
								if frame.From != srvr.Next{
									continue
								}
								newCommitReq := frame.Data.CRAQCommit
								HandleCRAQCommitReq(newCommitReq, frame.From)
							}

						case cmdRead:
							newReadReq := frame.Data.ReadRequest
							if srvr.isTail {
								HandleTailReadReq(newReadReq)
							} else {
								HandleCRAQReadReq(newReadReq)
							}
						
						default:
							newFrame := Frame {
									To: frame.From,
									From: srvr.Id,
									Key: msgCRAQError,
									Data: CRAQErrorPayload(fmt.Sprintf("Unknown command: %s.", cmd))}
							srvr.NetworkChanOut <- newFrame

							WriteToLogFile <- fmt.Sprintf("CRAQ Server %d received unknown command: %s.\n", srvr.Id, cmd)
					}

				case <- srvr.Power:
					HandlePowerDown()
					return
			}
		}
	}()
}


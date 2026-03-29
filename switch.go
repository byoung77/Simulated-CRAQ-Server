package main

/*
 This file implements a virtual switch with a fixed number of ports (currently 8),
 sized for a small distributed systems simulation.

 The switch uses setup channels (in/out) to handle connection requests and to
 distribute routing information to connected devices.

 Each port corresponds to a connection point for a device (e.g., CRAQ server,
 client, or other component). Devices are assigned IDs, and the switch maintains
 a routing table mapping IDs to ports.

 Note: The number of ports is fixed in the current design. Although ports are
 created in a loop, the main event loop uses a static `select` over each port's
 input channel, which requires the number of ports to be known at compile time.

 To extend the number of ports, update:
   - the size of portList (e.g., make([]Port, N))
   - the corresponding `select` cases in the switch loop

 This design keeps the switch simple and explicit for simulation purposes.
*/

import (
	"fmt"
	"strconv"
	"time"
)

// Hardware Types
const (
	devEmpty = ""
	devSwitch = "switch"
	servCRAQ = "CRAQ"
	servUser = "User"
	emptyPortId = -9999
)

const (
	cmdConnect = "connect"
	cmdDisconnect = "disconnect"
	cmdSendRoute = "send_route"
	msgError = "error"
	msgPortAssgn = "use_port"
	msgRecvRoute = "route_reply"
)

type Port struct {
	Type string
	Id int
	ChIn chan Frame
	ChOut chan Frame
}

type ConnectReq struct {
	Type string
	Id int
}

type DisconnectReq struct {
	Id int
}

type ConnectAns struct {
	Success bool
	ToPort chan Frame
	FromPort chan Frame	
} 

func CreatePort() Port {
	return Port{
		Type: devEmpty,
		Id: emptyPortId,
		ChIn: make(chan Frame, 256),
		ChOut: make(chan Frame, 256)}
}

type Switch struct {
	Id int
	SetupChanIn  chan<- Frame
	SetupChanOut <-chan Frame
	Power chan struct{} 
}

func spawnSwitch(id int) Switch {
	setupChanIn := make(chan Frame)
	setupChanOut := make(chan Frame)
	Power := make(chan struct{})
	
	go func() {
		timeOn := time.Now()
		switchId := id
		portList := make([]Port, 8)
		routeTable := make(map[int]int)
		
		//Set Up Ports
		for i:=0; i<8; i++ {
			portList[i] = CreatePort()
		}
		
		handleSetupMsg := func(frm Frame) {
			msg := frm.Key
			data := frm.Data
			
			switch msg {
				
				//Connect Device to Port
				case cmdConnect:
					req := data.ConnectReq
					for i := range portList {
						if portList[i].Id == req.Id {
							setupChanOut <- Frame{
										To: frm.From,
										From: switchId, 
										Key: msgError,
										Data: StringPayload("General Error:  Connect request failed (device Id already present).\n")}
							return
						}
					}
					
					for i := range portList {
						if portList[i].Type == devEmpty {
							portList[i].Type = req.Type
							portList[i].Id = req.Id
							setupChanOut <- Frame{
												To: frm.From, 
												From: switchId, 
												Key: msgPortAssgn, 
												Data: ConnectAnsPayload(ConnectAns{true, portList[i].ChIn, portList[i].ChOut})}
							routeTable[req.Id] = i
							return
						}
					}
					
					setupChanOut <- Frame{
										To: frm.From,
										From: switchId, 
										Key: msgError,
										Data: StringPayload("General Error:  Connect request failed (switch full).\n")}
					
			
				//Send RouteTable
				case cmdSendRoute:
					for i := range portList {
						if portList[i].Type == devSwitch { 
							portList[i].ChOut <- Frame{
														To: portList[i].Id,
														From: switchId,
														Key: msgRecvRoute,
														Data: RoutePayload(routeTable)}
						}
					}
					setupChanOut <- Frame{
										To: frm.From,
										From: switchId,
										Key: msgRecvRoute,
										Data: RoutePayload(routeTable)}	
			}		
		}
		
		handleNetworkMsg := func(frm Frame, port int) {
			rcpnt := frm.To
			if rcpnt == switchId {
				switch frm.Key {
					case msgRecvRoute:
						newTable := frm.Data.Route
						
						for key := range newTable {
							routeTable[key] = newTable[key]
						}
						
					case cmdPing:
						timeUp := time.Since(timeOn)

						pingReply := fmt.Sprintf("Port %d: Time up: %2f seconds.\n",switchId, timeUp.Seconds()) 

						portList[port].ChOut <- Frame {
											To: frm.From,
											From: switchId,
											Key: msgPingReply,
											Data: StringPayload(pingReply)}
											
					case cmdDisconnect:
						prt, ok := routeTable[frm.From]
						if ok {
							portList[prt] = CreatePort()
							delete(routeTable,frm.From)
						}
					
					default:
						portList[port].ChOut <- Frame{
												To: frm.From,
												From: switchId,
												Key: msgError,
												Data: StringPayload("General Error: " + frm.Key + " not a valid switch command.\n")}
				}
			} else {
				prt, ok := routeTable[rcpnt]
				if ok {
					portList[prt].ChOut <- frm
				} else {
					portList[port].ChOut <- Frame{
												To: frm.From,
												From: switchId,
												Key: msgError,
												Data: StringPayload("Network Error: Server with Id " + strconv.Itoa(rcpnt) + " not found on network.\n")}
				}
			}
		}
		
		for {
			select {
				case msg := <- setupChanIn:
					handleSetupMsg(msg)
				case msg := <- portList[0].ChIn:
					handleNetworkMsg(msg, 0)
				case msg := <- portList[1].ChIn:
					handleNetworkMsg(msg, 1)
				case msg := <- portList[2].ChIn:
					handleNetworkMsg(msg, 2)
				case msg := <- portList[3].ChIn:
					handleNetworkMsg(msg, 3)
				case msg := <- portList[4].ChIn:
					handleNetworkMsg(msg, 4)
				case msg := <- portList[5].ChIn:
					handleNetworkMsg(msg, 5)
				case msg := <- portList[6].ChIn:
					handleNetworkMsg(msg, 6)
				case msg := <- portList[7].ChIn:
					handleNetworkMsg(msg, 7)

				case <- Power:
					return
			}
		}
	}()
	
	return Switch{id, setupChanIn, setupChanOut, Power}
}

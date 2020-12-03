package main

import (
	"log"
	"net"
	"context"
	"math/rand"
	"google.golang.org/grpc"
	pb "../proto"
	"fmt"
	"time"
	"io"
	"os"
	"bufio"
	"strings"
	"io/ioutil"
	"unsafe"
)

var port = ":"+strings.Split(readAddress()[0],":")[1] //Quiza debamos usar distintos puertos segun en que trabajamos
var addressNameNode= "10.10.28.10:50051"

type server struct {
	pb.UnimplementedLibroServiceServer
}

var dataNodes = []string{}
var status = "Ok"
var libroChunks= make(map[string][]aChunk) //Nose si este diccionario funca bien
var funcionamiento = "EsperaInput"
var estadoCritico = estado{status:"LIBERADA"}

type aChunk struct{
	offset int
	data []byte
}

type estado struct{
	status string
	timestamp int64 
}

func readAddress() []string{
    f, err := os.Open("Tarea2DataNode/datanode/address.txt")
    listaAddress:= []string{}

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

    scanner := bufio.NewScanner(f)

    for scanner.Scan() {
        listaAddress=append(listaAddress,scanner.Text())
        
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
    return listaAddress
}

func (s* server) VerStatus2(ctx context.Context,prop *pb.Propuesta) (*pb.Status, error) {
	distribucion := prop.GetChunk()
	status := statusRevisar(distribucion)
	statusMensaje:= pb.Status{Status:status}
	return &statusMensaje, nil
}

func statusRevisar(distribucion []*pb.PropuestaChunk) string{
	a := "ok"
	nChunks:= 0
	for _,libro:=range libroChunks{
		nChunks=nChunks+len(libro)
	}
	for _,p := range distribucion {
		if p.GetIpMaquina() == dataNodes[0]{
			nChunks = nChunks + 1
		}
	}

	if nChunks>=200000{
		a ="not ok"
	}

	return a
}

func (s* server) UploadBook(stream pb.LibroService_UploadBookServer) error {
	fmt.Println("Subiendo libro...")
	ChunksPorEnviar := []pb.SendChunk{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF { // no hay mas chunks
			//enviar la propuesta
			s1 := rand.NewSource(time.Now().UnixNano())
    		r1 := rand.New(s1)

    		prop := []int{}

    		for len(prop) != len(ChunksPorEnviar) { //aleatoriamente se elige donde se alamcena cada chunk
    			prop = append(prop,r1.Intn(len(dataNodes)))
    		}
             
            distribucion := []*pb.PropuestaChunk{}
            ChunksPorDistribuir := []*pb.PropuestaChunk{}

    		for i, add := range prop {
    			distribucion = append(distribucion,&pb.PropuestaChunk{Offset : ChunksPorEnviar[i].GetOffset(),
    				IpMaquina : dataNodes[add],NombreLibro : ChunksPorEnviar[i].GetName()})
    		}

    		if funcionamiento == "C"{//Version centralizada

    			conn, err := grpc.Dial(addressNameNode, grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
    			if err != nil {
    				log.Fatalf("did not connect: %v", err)
    			}
    			defer conn.Close()
    			c := pb.NewLibroServiceClient(conn)
    			//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				//defer cancel()
				distribucionRevisada , err := c.SendPropuesta(context.Background(),&pb.Propuesta{Chunk : distribucion})
				if err != nil{
					fmt.Println("No se pudo aprobar la distribucion fallo critico")
					fmt.Println(err)
				}

				ChunksPorDistribuir = distribucionRevisada.GetChunk()
				
		    } else { //Version distribuidad

		    	for { //
		    		respuestas := []string{}
		    		respuestas = append(respuestas,statusRevisar(distribucion))

		    		for _,dir := range dataNodes[1:] { //Enviamos la distribucion a cada nodo y guardamos sus respuestas
		    			conn, err := grpc.Dial(dir, grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
    					if err != nil {
    						respuestas = append(respuestas,"notOk")
    						fmt.Println("Nodo caido")
    					} else{
    						defer conn.Close()
    						c := pb.NewLibroServiceClient(conn)
    						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
							defer cancel()
							stat , err := c.VerStatus2(ctx,&pb.Propuesta{Chunk : distribucion})
							if err != nil{
								respuestas = append(respuestas,"notOk")
							} else {
								respuestas = append(respuestas,stat.GetStatus())
							}
						}

		    		}

		    		acepto := true

		    		for i,res := range respuestas { //modificamos la propuesta
		    			if res != "ok"{
		    				for j,p := range distribucion{
		    					if p.GetIpMaquina() == dataNodes[i]{
		    						newadd := r1.Intn(len(dataNodes))
		    						for p.GetIpMaquina() == dataNodes[newadd]{
		    							newadd = r1.Intn(len(dataNodes))
		    						}
                                    distribucion[j] = &pb.PropuestaChunk{Offset :p.GetOffset(),
                                    	IpMaquina : dataNodes[newadd],NombreLibro : p.GetNombreLibro()}
                                    	acepto = false
		    						break
		    					}
		    				}
		    			}

		    		}

		    		if acepto { //Algoritmo de ricart y arawala
						estadoCritico.status="BUSCADA"
						estadoCritico.timestamp=time.Now().UnixNano()
						for i,nodo:=range dataNodes{
							if i==0{
							}else{
								connNode,err := grpc.Dial(nodo,grpc.WithInsecure(),grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
								if err !=nil{
									fmt.Println("nodo : ",nodo," caido")
								} else {
								defer connNode.Close()
								pregunta:= pb.NewLibroServiceClient(connNode)
								ctx, cancel:=context.WithTimeout(context.Background(),time.Second)
								defer cancel()
								respuesta,err:= pregunta.EstadoLog(ctx,&pb.EstadoNode{Status:estadoCritico.status,Timestamp:estadoCritico.timestamp})
								if err !=nil{
									fmt.Println("nodo : ",nodo," caido")
								}else if respuesta.Status != "LIBERADA"{
									fmt.Println("nodo : ",nodo," no liberado")
								} 
							}
							}
							}
							
						estadoCritico.status="TOMADA"	
						ChunksPorDistribuir = distribucion
		    			conn, err := grpc.Dial(addressNameNode, grpc.WithInsecure(), grpc.WithBlock())
    					if err != nil {
    						fmt.Println(err)
    					}
    					defer conn.Close()
    					c := pb.NewLibroServiceClient(conn)
    					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						_, err = c.VerStatus2(ctx,&pb.Propuesta{Chunk : distribucion})
						if err != nil{
							fmt.Println("Fallo la escritura en el log")
						}
						estadoCritico.status="LIBERADA"
		    			break
		    		}

		    	}


		    }

			for _,ch := range ChunksPorDistribuir{
				destiny := ch.GetIpMaquina()
				ind := 0
				for j,un := range ChunksPorEnviar{
					if ch.GetOffset() == un.GetOffset(){
						ind = j
						break
					}
				}
				if destiny == dataNodes[0]{
					chunkEscribir,chunkOffset,chunkLibro:=ChunksPorEnviar[ind].Chunk,ChunksPorEnviar[ind].Offset,ChunksPorEnviar[ind].Name
					newChunk:=aChunk{offset:int(chunkOffset) , data:chunkEscribir}
					libroChunks[chunkLibro]=append(libroChunks[chunkLibro],newChunk)
					
					archivoChunk, err := os.Create(strings.Split(chunkLibro,".")[0]+"--"+fmt.Sprint(chunkOffset))
				    if err != nil {
				        panic(err)
				    }
					defer archivoChunk.Close()
					if _, err = archivoChunk.Write(chunkEscribir); err != nil { 
						panic(err)
					}
			

				} else {
					conn2, err := grpc.Dial(destiny, grpc.WithInsecure(), grpc.WithBlock(),grpc.WithTimeout(30 * time.Second))
    				if err != nil {
    					fmt.Println("No se pudo comunicar con el nodo , fallo critico")
    				}
    				defer conn2.Close()
    				c := pb.NewLibroServiceClient(conn2)
    				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_ , err = c.OrdenarChunk(ctx,&ChunksPorEnviar[ind])
					if err == nil {
					}
				}
			}
			err = stream.SendAndClose(&pb.ReplyEmpty{Ok : 1})
			if err != nil {
				fmt.Println(err)
				return err
			}
			fmt.Println("Se subio el Libro")
			return nil

		} else if err != nil { // hubo un problema
			return err

		}

		ChunksPorEnviar = append(ChunksPorEnviar,pb.SendChunk{Name : chunk.GetName(),Offset : chunk.GetOffset(), 
			Chunk : chunk.GetChunk()})


	}
}
func (s* server) VerStatus(ctx context.Context, status *pb.Status) (*pb.Status, error){
	envioStatus:= revisarStatus()
	statusEnviar:=pb.Status{Status:envioStatus}
	return &statusEnviar,nil
}

func (s* server) OrdenarChunk(ctx context.Context, chunkRecibido *pb.SendChunk ) (*pb.ReplyEmpty, error){
	chunkEscribir,chunkOffset,chunkLibro:=chunkRecibido.Chunk,chunkRecibido.Offset,chunkRecibido.Name
	newChunk:=aChunk{offset:int(chunkOffset) , data:chunkEscribir}
	libroChunks[chunkLibro]=append(libroChunks[chunkLibro],newChunk)

	archivoChunk, err := os.Create(strings.Split(chunkLibro,".")[0]+"--"+fmt.Sprint(chunkOffset))
    if err != nil {
        panic(err)
    }
	defer archivoChunk.Close()
	if _, err = archivoChunk.Write(chunkEscribir); err != nil { 
		panic(err)
	}
	reply:=pb.ReplyEmpty{Ok:int64(1)}
	return &reply,nil
}

func (s* server ) DownloadChunk(ctx context.Context, chunkID *pb.ChunkId) (*pb.SendChunk, error){
	IDChunk:=chunkID.Id
	chunkPedido, err := ioutil.ReadFile(IDChunk) 
	if err != nil {
		fmt.Print(err)
	}
	newSendChunk:= pb.SendChunk{Chunk:chunkPedido}
	chunkInfo := unsafe.Sizeof(newSendChunk.GetChunk())
	fmt.Println("Chunkinfo: "+fmt.Sprint(chunkInfo))
	return &newSendChunk,nil
}


func (s* server) EstadoLog(ctx context.Context, peticion *pb.EstadoNode ) (*pb.EstadoNode, error){
	horaPeticion:= peticion.Timestamp
	estadoCriticoNode:=pb.EstadoNode{}
	for{
		if (estadoCritico.status=="LIBERADA"){
			estadoCriticoNode=pb.EstadoNode{Status:estadoCritico.status}
			break
		}else if (estadoCritico.status=="BUSCADA"){
			if (horaPeticion<=estadoCritico.timestamp){
				estadoCriticoNode=pb.EstadoNode{Status:"LIBERADA"}
				break
			}else{
				time.Sleep(time.Second)
			}
		}else{
			time.Sleep(time.Second)
		}
	}
	return &estadoCriticoNode,nil


}
func revisarStatus()string{
	fmt.Println("Viendo status..")
	nChunks:= 0
	for _,libro:=range libroChunks{
		nChunks=nChunks+len(libro)
	}
	if nChunks>=200000{
		status="error"
	}else {
		status="ok"
	}
	return status 
}


func main() { 
	reader := bufio.NewReader(os.Stdin)
    fmt.Println("DataNode")
    fmt.Println("---------------------")

    
    fmt.Print("Indique si desea el funcionamiento centralizado o distribuido (C o D) : ")//se pide si es Downloader y Uploader
    input1, _ := reader.ReadString('\n')
    input1 = strings.Replace(input1, "\n", "", -1)
    input1 = strings.Replace(input1, "\r", "", -1)
    funcionamiento = input1
    if funcionamiento != "C" && funcionamiento != "D"{ // Se pide si el funcionamiento es centralizado
    	log.Fatalf("Ingreso mal el tipo de funcionamiento ,abortando")
    }

	dataNodes=readAddress()
	fmt.Println(dataNodes,"namenode: ",addressNameNode)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterLibroServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

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
)

var port = ":"+strings.Split(ReadAddress()[0],":")[1] //Quiza debamos usar distintos puertos segun en que trabajamos
var addressNameNode= "localhost:50055"

type server struct {
	pb.UnimplementedLibroServiceServer
}

var dataNodes = []string{}
var status = "Ok"
var LibroChunks= make(map[string][]Chunk) //Nose si este diccionario funca bien
var funcionamiento = "EsperaInput"


type Chunk struct{
	offset int
	data []byte
}

func ReadAddress() []string{
    f, err := os.Open("address.txt")
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

    		for i, add := range prop {
    			distribucion = append(distribucion,&pb.PropuestaChunk{Offset : ChunksPorEnviar[i].GetOffset(),
    				IpMaquina : dataNodes[add],NombreLibro : ChunksPorEnviar[i].GetName()})
    		}

    		if funcionamiento == "C"{//Version centralizada

    		conn, err := grpc.Dial(addressNameNode, grpc.WithInsecure(), grpc.WithBlock())
    		if err != nil {
    			log.Fatalf("did not connect: %v", err)
    		}
    		defer conn.Close()
    		c := pb.NewLibroServiceClient(conn)
    		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			distribucionRevisada , err := c.SendPropuesta(ctx,&pb.Propuesta{Chunk : distribucion})
			if err != nil{
				fmt.Println(err)
			}

			ChunksPorDistribuir := distribucionRevisada.GetChunk()
		    } else { //Version distribuidad

		    	for { //
		    		respuestas := []bool{}

		    		for _,dir := range dataNodes[1:] { //Enviamos la distribucion a cada nodo y guardamos sus respuestas
		    			
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
					chunkRecibido:=ChunksPorEnviar[ind]
					chunkEscribir,chunkOffset,chunkLibro:=chunkRecibido.Chunk,chunkRecibido.Offset,chunkRecibido.Name
					newChunk:=Chunk{offset:int(chunkOffset) , data:chunkEscribir}
					LibroChunks[chunkLibro]=append(LibroChunks[chunkLibro],newChunk)
					fmt.Println("Cargado el libro")
					fmt.Println(LibroChunks)

				} else {
					conn2, err := grpc.Dial(destiny, grpc.WithInsecure(), grpc.WithBlock())
    				if err != nil {
    					log.Fatalf("did not connect: %v", err)
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
	envioStatus:= Status()
	statusEnviar:=pb.Status{Status:envioStatus}
	return &statusEnviar,nil
}

func (s* server) OrdenarChunk(ctx context.Context, chunkRecibido *pb.SendChunk ) (*pb.ReplyEmpty, error){
	chunkEscribir,chunkOffset,chunkLibro:=chunkRecibido.Chunk,chunkRecibido.Offset,chunkRecibido.Name
	newChunk:=Chunk{offset:int(chunkOffset) , data:chunkEscribir}
	LibroChunks[chunkLibro]=append(LibroChunks[chunkLibro],newChunk)
	fmt.Println("Cargado el libro")
	fmt.Println(LibroChunks)
	reply:=pb.ReplyEmpty{Ok:int64(1)}
	return &reply,nil
}




func Status()string{
	fmt.Println("Viendo status..")
	nChunks:= 0
	for _,libro:=range LibroChunks{
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
    if funcionamiento != "C" && funcionamiento != "D"{
    	log.Fatalf("Ingreso mal el tipo de funcionamiento ,abortando")
    }

	dataNodes=ReadAddress()
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

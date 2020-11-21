package main

import (
	"log"
	"net"
	"context"
	"math/rand"
	"google.golang.org/grpc"
	pb "../proto"
	"fmt"
)

const (
	port = ":50051" //Quiza debamos usar distintos puertos segun en que trabajamos
	addressNameNode  = "10.10.28.10:50051"
	addressDataNodeSelf  = "10.10.28.11:50051"
	addressDataNode1  = "10.10.28.12:50051"
	addressDataNode2  = "10.10.28.13:50051"
)

var dataNodes = [3]string{addressDataNodeSelf,addressDataNode1,addressDataNode2}

type server struct {
	pb.UnimplementedOrdenServiceServer
}
var LibroAux= []Chunk{}
var LibroChunks= make(map[string][]Chunk) //Nose si este diccionario funca bien

type Chunk struct{
	offset int
	data []byte
}

type libro struct{
	nombre string
	cantidadChunks int
}
//Primero cuando lleguen los chunks se acumularan en LibroAUX , luego al llegar al offset final pasara la siguiente funcion
func Newlibro(nombre string,cantidadChunks int) libro{
	nuevoLibro:=libro{nombre:nombre,cantidadChunks:cantidadChunks}
	LibroChunks[nuevoLibro.nombre]=LibroAux
	return nuevoLibro
}

func (s* server) UploadBook(stream pb.OrdenService_UploadBookServer) error {
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

    		for _, add := range prop {
    			//enviar DataNode[add]
    		}

		} else if err != nil { // hubo un problema
			fmt.Println(err)
			return err

		}

		ChunksPorEnviar = append(ChunksPorEnviar,chunk)


	}
	return nil
}
func main() { 
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterOrdenServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

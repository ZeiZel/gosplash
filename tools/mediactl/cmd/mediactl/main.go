// mediactl — крошечный gRPC-клиент к media-сервису.
//
//	go run ./tools/mediactl/cmd -id <uuid> -user 1
//
// Нужен, чтобы позвать сервис руками и увидеть, как выглядит клиентская
// сторона gRPC: канал, сгенерированный стаб, контекст с таймаутом, разбор
// ошибки по коду. Ровно то же самое делает catalog в своём консьюмере —
// только там это спрятано внутри бизнес-логики.
//
// Почему не grpcurl: он универсальнее, но требует reflection и на некоторых
// машинах капризничает. Своя утилита в двадцать строк надёжнее и заодно
// показывает код, который ты и так будешь писать.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
)

func main() {
	target := flag.String("target", "localhost:9101", "адрес gRPC media")
	id := flag.String("id", "", "id фото")
	userID := flag.Int64("user", 0, "id пользователя — он же ключ шардирования")
	flag.Parse()

	if *id == "" || *userID == 0 {
		log.Fatal("нужны -id и -user")
	}

	// Соединение ленивое: реальный коннект произойдёт при первом вызове,
	// и клиент сам переподключится, если сервис перезапустят.
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("подключение: %v", err)
	}
	defer conn.Close()

	client := mediav1.NewMediaServiceClient(conn)

	// Таймаут на вызов обязателен. Без него зависший сервер подвесит и клиента:
	// в gRPC нет разумного значения по умолчанию, ждать будут вечно.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetPhoto(ctx, &mediav1.GetPhotoRequest{Id: *id, UserId: *userID})
	if err != nil {
		// Ошибки gRPC несут КОД, а не текст. По коду клиент решает, что делать:
		// NotFound и InvalidArgument повторять бессмысленно,
		// Unavailable и DeadlineExceeded — наоборот, стоит.
		st, _ := status.FromError(err)
		log.Fatalf("GetPhoto: %s: %s", st.Code(), st.Message())
	}

	// protojson печатает protobuf в JSON по правилам самого протокола
	// (camelCase, enum'ы строками), а не через encoding/json.
	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(resp)
	if err != nil {
		log.Fatalf("вывод: %v", err)
	}
	fmt.Println(string(out))
}

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
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	mediav1 "gosplash/gen/go/gosplash/media/v1"
)

// run делает всю работу и ВОЗВРАЩАЕТ ошибку, а не завершает процесс сам.
//
// Разница не косметическая: при os.Exit/log.Fatal прямо из main отложенные
// вызовы (defer) НЕ выполняются — соединения остаются незакрытыми, буферы
// несброшенными. Именно это и ловит линтер (gocritic exitAfterDefer).
// Когда выход происходит обычным return, все defer успевают отработать,
// а os.Exit вызывается уже в main, когда возвращаться некуда.
func run() error {
	target := flag.String("target", "localhost:9101", "адрес gRPC media")
	id := flag.String("id", "", "id фото")
	userID := flag.Int64("user", 0, "id пользователя — он же ключ шардирования")
	flag.Parse()

	if *id == "" || *userID == 0 {
		return errors.New("нужны -id и -user")
	}

	// Соединение ленивое: реальный коннект произойдёт при первом вызове,
	// и клиент сам переподключится, если сервис перезапустят.
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("подключение: %w", err)
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
		return fmt.Errorf("GetPhoto: %s: %s", st.Code(), st.Message())
	}

	// protojson печатает protobuf в JSON по правилам самого протокола
	// (camelCase, enum'ы строками), а не через encoding/json.
	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(resp)
	if err != nil {
		return fmt.Errorf("вывод: %w", err)
	}
	fmt.Println(string(out))
	return nil
}

// main намеренно состоит из трёх строк: это единственное место программы,
// которому позволено завершать процесс. К этому моменту run уже вернулась,
// то есть все её defer отработали.
func main() {
	if err := run(); err != nil {
		slog.Error("остановлен с ошибкой", "error", err)
		os.Exit(1)
	}
}

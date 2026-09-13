// Фикстура: app, нарушающий ports & adapters дважды — google.golang.org/grpc
// и net/http напрямую. gen/go здесь НЕ нарушение (запрещён только для
// domain), поэтому его в этой фикстуре нет.
package app

import (
	"net/http"

	"google.golang.org/grpc"
)

type Service struct {
	conn   *grpc.ClientConn
	client *http.Client
}

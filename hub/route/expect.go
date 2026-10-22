package route

import (
	"github.com/metacubex/mihomo/component/expect"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// expectRouter 暴露影子模式学到的分布和判定统计
func expectRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		rep := expect.Snapshot()
		if rep == nil {
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, newError("expect shadow mode is off"))
			return
		}
		render.JSON(w, r, rep)
	})
	return r
}

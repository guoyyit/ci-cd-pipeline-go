// Package server exposes a tiny local HTTP control API used by pipelinectl:
//
//	GET  /runs                 list runs
//	GET  /runs/{id}            get one run
//	POST /runs/{id}/cancel     cancel a run            {"reason":"..."}
//	POST /runs/{id}/rerun      rerun a run             {"force":false}
//	POST /runs/{id}/approve    approve a stage         {"stage":"...","approver":"..."}
//	POST /runs/{id}/reject     reject a stage          {"stage":"...","approver":"...","comment":"..."}
//
// It binds to localhost only; this is a local single-user service.
package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/engine"
)

// Server is the HTTP control plane.
type Server struct {
	eng *engine.Engine
	mux *http.ServeMux
}

// New constructs a Server around the given engine.
func New(eng *engine.Engine) *Server {
	s := &Server{eng: eng, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/runs", s.handleRuns)
	s.mux.HandleFunc("/runs/", s.handleRun)
}

// Handler returns the http.Handler to serve.
func (s *Server) Handler() http.Handler { return s.mux }

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	runs, err := s.eng.GetRuns(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(rest, "/")
	if parts[0] == "" {
		writeErr(w, http.StatusBadRequest, "run id required")
		return
	}
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	ctx := r.Context()
	if action == "" {
		run, err := s.eng.GetRun(ctx, id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var body map[string]interface{}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	str := func(k string) string {
		if body == nil {
			return ""
		}
		if v, ok := body[k].(string); ok {
			return v
		}
		return ""
	}

	switch action {
	case "cancel":
		if err := s.eng.CancelRun(ctx, id, str("reason")); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		run, _ := s.eng.GetRun(ctx, id)
		writeJSON(w, http.StatusOK, run)
	case "rerun":
		force, _ := body["force"].(bool)
		run, err := s.eng.RerunRun(ctx, id, force)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
	case "approve":
		run, err := s.eng.Approve(ctx, id, str("stage"), str("approver"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
	case "reject":
		run, err := s.eng.Reject(ctx, id, str("stage"), str("approver"), str("comment"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, run)
	default:
		writeErr(w, http.StatusNotFound, "unknown action: "+action)
	}
}

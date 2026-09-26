package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Server 提供流水线 HTTP API：
//
//	GET  /api/runs                     列出全部运行
//	GET  /api/runs/{id}                查询运行详情
//	POST /api/runs/{id}/cancel         取消运行
//	POST /api/runs/{id}/retry          重试失败及下游阶段（?force=1 全量重跑）
//	POST /api/runs/{id}/stages/{s}/approve   审批通过
//	POST /api/runs/{id}/stages/{s}/reject    审批拒绝（body 需含 reason）
//	POST /api/events                   手动投递事件
type Server struct {
	runner *Runner
	mux    *http.ServeMux
}

// NewServer 创建 HTTP 服务。
func NewServer(r *Runner) *Server {
	s := &Server{runner: r, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/runs", s.handleRuns)
	s.mux.HandleFunc("/api/runs/", s.handleRunSub)
	s.mux.HandleFunc("/api/events", s.handleEvents)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

// Handler 返回 HTTP 处理器。
func (s *Server) Handler() http.Handler { return s.mux }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.runner.ListRuns())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var e Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid event: "+err.Error())
		return
	}
	run, created, err := s.runner.SubmitEvent(e)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if run == nil {
		writeJSON(w, http.StatusOK, map[string]any{"created": false, "reason": "filtered or duplicate"})
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"created": created, "run": run})
}

func (s *Server) handleRunSub(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/runs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	id := parts[0]

	// /api/runs/{id}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		run, ok := s.runner.GetRun(id)
		if !ok {
			writeErr(w, http.StatusNotFound, "run not found")
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}

	// /api/runs/{id}/cancel | /retry
	if len(parts) == 2 {
		switch parts[1] {
		case "cancel":
			if r.Method != http.MethodPost {
				writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			if err := s.runner.CancelRun(id); err != nil {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
			return
		case "retry":
			if r.Method != http.MethodPost {
				writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			force := r.URL.Query().Get("force") == "1"
			if err := s.runner.RetryRun(id, force); err != nil {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"status": "retrying", "force": force})
			return
		}
	}

	// /api/runs/{id}/stages/{stage}/approve|reject
	if len(parts) == 4 && parts[1] == "stages" {
		stage := parts[2]
		action := parts[3]
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			Approver string `json:"approver"`
			Reason   string `json:"reason"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Approver == "" {
			body.Approver = "anonymous"
		}
		switch action {
		case "approve":
			if err := s.runner.Approve(id, stage, body.Approver); err != nil {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "approved"})
			return
		case "reject":
			if err := s.runner.Reject(id, stage, body.Approver, body.Reason); err != nil {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
			return
		}
	}
	writeErr(w, http.StatusNotFound, "not found")
}

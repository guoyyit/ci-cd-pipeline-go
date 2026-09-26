// Command pipelinectl controls a running pipelined daemon over its localhost
// HTTP API.
//
//	pipelinectl [-addr 127.0.0.1:8080] list
//	pipelinectl get <run-id>
//	pipelinectl cancel <run-id> [-reason "..."]
//	pipelinectl rerun <run-id> [-force]
//	pipelinectl approve <run-id> -stage <name> -as <user>
//	pipelinectl reject  <run-id> -stage <name> -as <user> -comment "why"
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "daemon control API address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	c := &client{base: "http://" + *addr, http: &http.Client{Timeout: 10 * time.Second}}

	var err error
	switch args[0] {
	case "list":
		err = listRuns(c)
	case "get":
		requireNArgs(args, 2)
		err = getRun(c, args[1])
	case "cancel":
		fs := flag.NewFlagSet("cancel", flag.ExitOnError)
		reason := fs.String("reason", "canceled by user", "cancel reason")
		pos, fl := splitArgs(args[1:], nil)
		_ = fs.Parse(fl)
		requireNArgs(pos, 1)
		err = post(c, pos[0], "cancel", map[string]string{"reason": *reason})
	case "rerun":
		fs := flag.NewFlagSet("rerun", flag.ExitOnError)
		force := fs.Bool("force", false, "re-run all stages, including successful ones")
		pos, fl := splitArgs(args[1:], map[string]bool{"force": true})
		_ = fs.Parse(fl)
		requireNArgs(pos, 1)
		err = post(c, pos[0], "rerun", map[string]interface{}{"force": *force})
	case "approve":
		fs := flag.NewFlagSet("approve", flag.ExitOnError)
		stage := fs.String("stage", "", "stage name")
		as := fs.String("as", "", "approving user")
		pos, fl := splitArgs(args[1:], nil)
		_ = fs.Parse(fl)
		requireNArgs(pos, 1)
		err = post(c, pos[0], "approve", map[string]string{"stage": *stage, "approver": *as})
	case "reject":
		fs := flag.NewFlagSet("reject", flag.ExitOnError)
		stage := fs.String("stage", "", "stage name")
		as := fs.String("as", "", "rejecting user")
		comment := fs.String("comment", "", "mandatory rejection comment")
		pos, fl := splitArgs(args[1:], nil)
		_ = fs.Parse(fl)
		requireNArgs(pos, 1)
		err = post(c, pos[0], "reject",
			map[string]string{"stage": *stage, "approver": *as, "comment": *comment})
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pipelinectl [list|get|cancel|rerun|approve|reject] ...")
}

func requireNArgs(args []string, n int) {
	if len(args) != n {
		usage()
		os.Exit(2)
	}
}

type client struct {
	base string
	http *http.Client
}

func listRuns(c *client) error {
	body, err := c.get("/runs")
	if err != nil {
		return err
	}
	var runs []*model.Run
	if err := json.Unmarshal(body, &runs); err != nil {
		return err
	}
	fmt.Printf("%-22s %-10s %-10s %-12s %s\n", "RUN ID", "STATUS", "PIPELINE", "REF", "FINISHED")
	for _, r := range runs {
		ref := r.Branch
		if r.Tag != "" {
			ref = "tag:" + r.Tag
		}
		fin := "-"
		if r.FinishedAt != nil {
			fin = r.FinishedAt.Format(time.RFC3339)
		}
		fmt.Printf("%-22s %-10s %-10s %-12s %s\n", r.ID, r.Status, r.Pipeline, ref, fin)
	}
	return nil
}

func getRun(c *client, id string) error {
	body, err := c.get("/runs/" + id)
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		return err
	}
	fmt.Println(pretty.String())
	return nil
}

func post(c *client, id, action string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+"/runs/"+id+"/"+action, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, string(respBody))
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, respBody, "", "  "); err == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Println(string(respBody))
	}
	return nil
}

func (c *client) get(path string) ([]byte, error) {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(body))
	}
	return body, nil
}

// splitArgs separates positional arguments from flags so users may write the
// run id either before or after options (the stdlib flag package stops at the
// first non-flag token). boolFlags lists flags that take no value.
func splitArgs(args []string, boolFlags map[string]bool) (positionals, rest []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if len(a) > 0 && a[0] == '-' {
			name := a
			if idx := indexEq(name); idx >= 0 {
				name = name[:idx]
			}
			rest = append(rest, a)
			if idx := indexEq(a); idx < 0 && !boolFlags[trimDash(name)] && i+1 < len(args) {
				rest = append(rest, args[i+1])
				i++
			}
			continue
		}
		positionals = append(positionals, a)
	}
	return positionals, rest
}

func indexEq(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return i
		}
	}
	return -1
}

func trimDash(s string) string {
	for len(s) > 0 && s[0] == '-' {
		s = s[1:]
	}
	return s
}

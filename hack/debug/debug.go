// Copyright 2023 The glassdb Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/mbrt/glassdb/internal/data"
	"github.com/mbrt/glassdb/internal/data/paths"
)

const graphTemplate = `
digraph D {
	splines=false;
	node[shape=ellipse];

	// Time.
	{{range $i, $e := .Calls}}
		t{{$i}} [shape=record];
	{{- end}}

	{{range .Times}}
		{{.V1}} -> {{.V2}};
	{{- end}}

	// Calls.
	{{range $i, $e := .Calls}}
		c{{$i}} [label="{{$e.Label}}"];
	{{- end}}

	{{range .TimeCalls}}
		{rank=same {{.V1}} {{.V2}}};
	{{- end}}

	// Clusters.
	{{range $i, $e := .Clusters}}
		g{{$i}} [shape=record,label="{{$e.Name}}"];
		{{- range .Edges}}
			{{.V1}} -> {{.V2}};
		{{- end}}
	{{- end}}

	{rank=same {{range $i, $e := .Clusters}}g{{$i}} {{end}}};
}
`

var commands = newCommander()

func errorf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
}

func newCommander() *commander {
	return &commander{
		cmds: map[string]command{},
	}
}

type commander struct {
	cmds map[string]command
}

func (c *commander) Add(fs *flag.FlagSet, fn func(args []string) error) {
	c.cmds[fs.Name()] = command{
		fn: fn,
		fs: fs,
	}
}

func (c *commander) Parse() error {
	c.Add(flag.NewFlagSet("help", flag.ExitOnError), func([]string) error {
		errorf("usage: debug <command>\n\navailable commands:\n")
		for k := range c.cmds {
			errorf("  %s\n", k)
		}
		return nil
	})
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: debug <command>")
	}
	cmd, ok := c.cmds[os.Args[1]]
	if !ok {
		return fmt.Errorf("command %q not found", os.Args[1])
	}
	if err := cmd.fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	return cmd.fn(cmd.fs.Args()[1:])
}

type command struct {
	fn func(args []string) error
	fs *flag.FlagSet
}

func decodePath(args []string) error {
	for _, p := range args {
		pr, err := paths.Parse(p)
		if err != nil {
			return fmt.Errorf("parsing path %q: %v", p, err)
		}
		fmt.Printf("Parts: %+v", pr)

		suffix := strings.Join([]string{string(pr.Type), pr.Suffix}, "/")
		switch pr.Type {

		case paths.KeyType:
			k, err := paths.ToKey(suffix)
			if err != nil {
				return fmt.Errorf("path to key %q: %v", suffix, err)
			}
			fmt.Printf(", Key: %s\n", string(k))

		case paths.CollectionType:
			c, err := paths.ToCollection(suffix)
			if err != nil {
				return fmt.Errorf("path to collection %q: %v", suffix, err)
			}
			fmt.Printf(", Collection: %s\n", string(c))

		case paths.TransactionType:
			tx, err := paths.ToTransaction(suffix)
			if err != nil {
				return fmt.Errorf("path to transaction %q: %v", tx, err)
			}
			fmt.Printf(", Transaction: %s\n", tx.String())

		default:
			fmt.Println()
		}
	}

	return nil
}

func decodeTag(args []string) error {
	for _, a := range args {
		buf, err := base64.URLEncoding.DecodeString(a)
		if err != nil {
			return fmt.Errorf("decoding %q: %v", a, err)
		}
		fmt.Println(data.TxID(buf).String())
	}
	return nil
}

func encodePath(args []string) error {
	for _, a := range args {
		fmt.Println(paths.FromKey("prefix", []byte(a)))
	}
	return nil
}

type call struct {
	Time      int
	Goroutine string
	Label     string
	Path      string
}

type graphData struct {
	Times     []graphEdge
	Calls     []call
	TimeCalls []graphEdge
	Clusters  []graphCluster
}

type graphEdge struct {
	V1 string
	V2 string
}

type graphCluster struct {
	Name  string
	Edges []graphEdge
}

func callGraph(args []string) error {
	var (
		records [][]string
		err     error
	)
	// Format: goroutine, label, path.
	if len(args) == 0 {
		records, err = readCSVStdin()
	} else {
		records, err = readCSV(args[0])
	}
	if err != nil {
		return err
	}

	calls := make([]call, len(records))
	for i, record := range records {
		if len(record) < 3 {
			return fmt.Errorf("line %d: expected record length >= 3, got %d", i+1, len(record))
		}
		calls[i] = call{
			Time:      i,
			Goroutine: record[0],
			Label:     record[1],
			Path:      record[2],
		}
	}
	removePathPrefix(calls)

	t := template.Must(template.New("").Parse(graphTemplate))
	return t.Execute(os.Stdout, buildGraphData(calls))
}

func readCSVStdin() ([][]string, error) {
	return csv.NewReader(os.Stdin).ReadAll()
}

func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	return r.ReadAll()
}

func removePathPrefix(cs []call) {
	if len(cs) == 0 {
		return
	}
	paths := make([]string, len(cs))
	for i, c := range cs {
		paths[i] = c.Path
	}
	sort.Strings(paths)
	longest := paths[0]

	for _, s := range paths[1:] {
		i := 0
		for i := 0; i < len(longest); i++ {
			if longest[i] != s[i] {
				break
			}
		}
		longest = longest[:i]
	}
	if len(longest) == 0 {
		return
	}

	// Drop the prefix.
	for i := range cs {
		cs[i].Path = "<p>" + cs[i].Path[:len(longest)]
	}
}

func buildGraphData(calls []call) graphData {
	var ts []graphEdge
	if len(calls) > 1 {
		ts = make([]graphEdge, len(calls)-1)
		for i := 0; i < len(calls)-1; i++ {
			ts[i] = graphEdge{
				fmt.Sprintf("t%d", i),
				fmt.Sprintf("t%d", i+1),
			}
		}
	}

	tc := make([]graphEdge, len(calls))
	for i := 0; i < len(tc); i++ {
		tc[i] = graphEdge{
			fmt.Sprintf("t%d", i),
			fmt.Sprintf("c%d", i),
		}
	}

	// Divide in clusters (per goroutine and path).
	clusters := make(map[string][]call)
	for _, c := range calls {
		cname := fmt.Sprintf("%s|%s", c.Goroutine, c.Path)
		clusters[cname] = append(clusters[cname], c)
	}
	var cs []graphCluster
	for cname, calls := range clusters {
		edges := make([]graphEdge, len(calls))
		edges[0] = graphEdge{
			fmt.Sprintf("g%d", len(cs)),
			fmt.Sprintf("c%d", calls[0].Time),
		}
		for i := 0; i < len(calls)-1; i++ {
			edges[i+1] = graphEdge{
				fmt.Sprintf("c%d", calls[i].Time),
				fmt.Sprintf("c%d", calls[i+1].Time),
			}
		}
		// Add an edge
		cs = append(cs, graphCluster{
			Name:  cname,
			Edges: edges,
		})
	}

	return graphData{
		Times:     ts,
		Calls:     calls,
		TimeCalls: tc,
		Clusters:  cs,
	}
}

func parseLogEvents(args []string) error {
	var in io.Reader

	if len(args) == 0 {
		in = os.Stdin
	} else {
		f, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	}
	scanner := bufio.NewScanner(in)
	re := regexp.MustCompile(`(tx|event|path|err|extra)=([^,]+)`)
	w := csv.NewWriter(os.Stdout)
	header := []string{"transaction", "event", "path", "error", "extra"}
	if err := w.Write(header); err != nil {
		return err
	}

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(string(line), "event=") {
			continue
		}
		match := re.FindAllStringSubmatch(line, -1)
		if len(match) == 0 {
			continue
		}
		record := make([]string, len(header))
		for _, m := range match {
			key, val := m[1], m[2]
			switch key {
			case "tx":
				record[0] = val
			case "event":
				record[1] = val
			case "path":
				record[2] = val
			case "err":
				record[3] = val
			case "extra":
				record[4] = val
			}
		}

		if err := w.Write(record); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	w.Flush()
	return w.Error()
}

func main() {
	commands.Add(flag.NewFlagSet("decode-path", flag.ExitOnError), decodePath)
	commands.Add(flag.NewFlagSet("decode-tag", flag.ExitOnError), decodeTag)
	commands.Add(flag.NewFlagSet("encode-path", flag.ExitOnError), encodePath)
	commands.Add(flag.NewFlagSet("call-graph", flag.ExitOnError), callGraph)
	commands.Add(flag.NewFlagSet("parse-log-events", flag.ExitOnError), parseLogEvents)

	if err := commands.Parse(); err != nil {
		errorf("error: %v\n", err)
		os.Exit(1)
	}
}

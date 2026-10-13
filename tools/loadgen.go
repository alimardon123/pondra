// A small HTTP load generator for the serving benchmark (Python's client tops out near 2k req/s).
//
//	go run tools/loadgen.go -url 'http://127.0.0.1:18300/lookup/kv/{k}' -keys 2000000 -c 32 -secs 5
//	go run tools/loadgen.go -url http://127.0.0.1:18300/sql -body 'SELECT count(*) FROM kv' -c 32
//	go run tools/loadgen.go -url http://127.0.0.1:18300/sql -bodies mix.sql -c 200
//	go run tools/loadgen.go -urls http://a:18300/sql,http://b:18301/sql -bodies mix.sql -c 400
//
// `{k}` in the URL (or body) becomes a random key in [0, keys), `{n}` a number in [0, 1000).
// `-bodies`: a file of statements, one a line, each request one of them at random; the JSON line
// then has each one's p50, p99 and errors too (`by`, in the file's order). A line may start with a
// label and a tab (`q6<TAB>SELECT …`): the lines that share a label are one entry of `by`, so a file
// of thousands of statements reports once per query. `-urls` sends the requests round-robin over
// several nodes (`per_url` counts them). Prints one JSON line.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "", "URL; {k} is replaced by a random key")
	urls := flag.String("urls", "", "several URLs, comma-separated: requests go round-robin over them (instead of -url)")
	body := flag.String("body", "", "POST this body instead of a GET ({k} replaced too)")
	bodies := flag.String("bodies", "", "a file of bodies, one a line: each request POSTs one at random")
	keys := flag.Int("keys", 1, "keys are drawn from [0, keys)")
	conc := flag.Int("c", 8, "concurrent clients")
	secs := flag.Float64("secs", 5, "duration")
	flag.Parse()
	var mix []string    // each line's statement ({k} still in it)
	var group []int     // the entry of `by` each line counts in
	var labels []string // one per entry of `by`: the label, or "" for a line with none
	index := map[string]int{}
	if *bodies != "" {
		text, err := os.ReadFile(*bodies)
		if err != nil {
			panic(err)
		}
		for _, l := range strings.Split(string(text), "\n") {
			if strings.TrimSpace(l) == "" {
				continue
			}
			label, statement := "", l
			if i := strings.Index(l, "\t"); i >= 0 {
				label, statement = l[:i], l[i+1:]
			}
			key := label
			if key == "" {
				key = fmt.Sprintf("#%d", len(mix)) // (a line with no label is an entry of its own)
			}
			g, seen := index[key]
			if !seen {
				g = len(labels)
				index[key] = g
				labels = append(labels, label)
			}
			mix = append(mix, statement)
			group = append(group, g)
		}
	} else if *body != "" {
		mix, group, labels = []string{*body}, []int{0}, []string{""}
	}
	targets := []string{*url}
	if *urls != "" {
		targets = strings.Split(*urls, ",")
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc}}
	begin := time.Now()
	deadline := begin.Add(time.Duration(*secs * float64(time.Second)))
	var mu sync.Mutex
	var lat []float64
	by := make([][]float64, len(labels))
	byErr := make([]int, len(labels))
	hits := make([]int64, len(targets)) // requests sent to each target
	var turn uint64                     // round-robin: request n goes to target n % len(targets)
	errs := 0
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []float64
			each := make([][]float64, len(labels))
			eachBad := make([]int, len(labels))
			bad := 0
			for time.Now().Before(deadline) {
				n := atomic.AddUint64(&turn, 1) - 1
				t := int(n % uint64(len(targets)))
				atomic.AddInt64(&hits[t], 1)
				k := fmt.Sprint(rand.Intn(*keys))
				fill := strings.NewReplacer("{k}", k, "{n}", fmt.Sprint(rand.Intn(1000)))
				start := time.Now()
				var r *http.Response
				var err error
				which := -1
				if len(mix) == 0 {
					r, err = client.Get(fill.Replace(targets[t]))
				} else {
					which = rand.Intn(len(mix))
					r, err = client.Post(targets[t], "text/plain", strings.NewReader(fill.Replace(mix[which])))
				}
				failed := true
				if err == nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
					failed = r.StatusCode != 200
				}
				if failed {
					bad++
				}
				ms := float64(time.Since(start).Microseconds()) / 1000
				mine = append(mine, ms)
				if which >= 0 {
					g := group[which]
					each[g] = append(each[g], ms)
					if failed {
						eachBad[g]++
					}
				}
			}
			mu.Lock()
			lat, errs = append(lat, mine...), errs+bad
			for g := range labels {
				by[g] = append(by[g], each[g]...)
				byErr[g] += eachBad[g]
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	// (over the time until the last request came back: a request sent before the deadline is counted,
	// and with answers that take seconds, dividing by -secs alone counted them as if they took none)
	elapsed := time.Since(begin).Seconds()
	sort.Float64s(lat)
	pct := func(xs []float64, p float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		return xs[int(p*float64(len(xs)-1))]
	}
	each := []map[string]any{}
	if len(mix) > 1 {
		for g, xs := range by {
			sort.Float64s(xs)
			entry := map[string]any{"requests": len(xs), "errors": byErr[g], "p50_ms": pct(xs, 0.5), "p99_ms": pct(xs, 0.99)}
			if labels[g] != "" {
				entry["label"] = labels[g]
			}
			each = append(each, entry)
		}
	}
	out := map[string]any{"clients": *conc, "requests": len(lat), "qps": int(float64(len(lat)) / elapsed), "secs": elapsed,
		"errors": errs, "p50_ms": pct(lat, 0.5), "p99_ms": pct(lat, 0.99), "max_ms": pct(lat, 1), "by": each}
	if len(targets) > 1 {
		out["per_url"] = hits
	}
	json.NewEncoder(os.Stdout).Encode(out)
}

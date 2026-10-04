// A small HTTP load generator for the serving benchmark (Python's client tops out near 2k req/s).
//
//	go run tools/loadgen.go -url 'http://127.0.0.1:18300/lookup/kv/{k}' -keys 2000000 -c 32 -secs 5
//	go run tools/loadgen.go -url http://127.0.0.1:18300/sql -body 'SELECT count(*) FROM kv' -c 32
//	go run tools/loadgen.go -url http://127.0.0.1:18300/sql -bodies mix.sql -c 200
//
// `{k}` in the URL (or body) becomes a random key in [0, keys), `{n}` a number in [0, 1000).
// `-bodies`: a file of statements, one a line, each request one of them at random; the JSON line
// then has each one's p50 and p99 too (`by`, in the file's order). Prints one JSON line.
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
	"time"
)

func main() {
	url := flag.String("url", "", "URL; {k} is replaced by a random key")
	body := flag.String("body", "", "POST this body instead of a GET ({k} replaced too)")
	bodies := flag.String("bodies", "", "a file of bodies, one a line: each request POSTs one at random")
	keys := flag.Int("keys", 1, "keys are drawn from [0, keys)")
	conc := flag.Int("c", 8, "concurrent clients")
	secs := flag.Float64("secs", 5, "duration")
	flag.Parse()
	var mix []string
	if *bodies != "" {
		text, err := os.ReadFile(*bodies)
		if err != nil {
			panic(err)
		}
		for _, l := range strings.Split(string(text), "\n") {
			if strings.TrimSpace(l) != "" {
				mix = append(mix, l)
			}
		}
	} else if *body != "" {
		mix = []string{*body}
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc}}
	deadline := time.Now().Add(time.Duration(*secs * float64(time.Second)))
	var mu sync.Mutex
	var lat []float64
	by := make([][]float64, len(mix))
	errs := 0
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []float64
			each := make([][]float64, len(mix))
			bad := 0
			for time.Now().Before(deadline) {
				k := fmt.Sprint(rand.Intn(*keys))
				fill := strings.NewReplacer("{k}", k, "{n}", fmt.Sprint(rand.Intn(1000)))
				t := time.Now()
				var r *http.Response
				var err error
				which := -1
				if len(mix) == 0 {
					r, err = client.Get(fill.Replace(*url))
				} else {
					which = rand.Intn(len(mix))
					r, err = client.Post(*url, "text/plain", strings.NewReader(fill.Replace(mix[which])))
				}
				if err == nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
					if r.StatusCode != 200 {
						bad++
					}
				} else {
					bad++
				}
				ms := float64(time.Since(t).Microseconds()) / 1000
				mine = append(mine, ms)
				if which >= 0 {
					each[which] = append(each[which], ms)
				}
			}
			mu.Lock()
			lat, errs = append(lat, mine...), errs+bad
			for i := range by {
				by[i] = append(by[i], each[i]...)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Float64s(lat)
	pct := func(xs []float64, p float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		return xs[int(p*float64(len(xs)-1))]
	}
	each := []map[string]any{}
	if len(mix) > 1 {
		for _, xs := range by {
			sort.Float64s(xs)
			each = append(each, map[string]any{"requests": len(xs), "p50_ms": pct(xs, 0.5), "p99_ms": pct(xs, 0.99)})
		}
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{"clients": *conc, "requests": len(lat), "qps": int(float64(len(lat)) / *secs),
		"errors": errs, "p50_ms": pct(lat, 0.5), "p99_ms": pct(lat, 0.99), "max_ms": pct(lat, 1), "by": each})
}

// A small HTTP load generator for the serving benchmark (Python's client tops out near 2k req/s).
//
//	go run tools/loadgen.go -url 'http://127.0.0.1:18300/lookup/kv/{k}' -keys 2000000 -c 32 -secs 5
//	go run tools/loadgen.go -url http://127.0.0.1:18300/sql -body 'SELECT count(*) FROM kv' -c 32
//
// `{k}` in the URL (or body) becomes a random key in [0, keys). Prints one JSON line.
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
	keys := flag.Int("keys", 1, "keys are drawn from [0, keys)")
	conc := flag.Int("c", 8, "concurrent clients")
	secs := flag.Float64("secs", 5, "duration")
	flag.Parse()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc}}
	deadline := time.Now().Add(time.Duration(*secs * float64(time.Second)))
	var mu sync.Mutex
	var lat []float64
	errs := 0
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []float64
			bad := 0
			for time.Now().Before(deadline) {
				k := fmt.Sprint(rand.Intn(*keys))
				t := time.Now()
				var r *http.Response
				var err error
				if *body == "" {
					r, err = client.Get(strings.ReplaceAll(*url, "{k}", k))
				} else {
					r, err = client.Post(*url, "text/plain", strings.NewReader(strings.ReplaceAll(*body, "{k}", k)))
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
				mine = append(mine, float64(time.Since(t).Microseconds())/1000)
			}
			mu.Lock()
			lat, errs = append(lat, mine...), errs+bad
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Float64s(lat)
	pct := func(p float64) float64 { return lat[int(p*float64(len(lat)-1))] }
	json.NewEncoder(os.Stdout).Encode(map[string]any{"clients": *conc, "requests": len(lat), "qps": int(float64(len(lat)) / *secs),
		"errors": errs, "p50_ms": pct(0.5), "p99_ms": pct(0.99), "max_ms": lat[len(lat)-1]})
}

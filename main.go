package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
)

var N = 100000    // Number of requests
var C = 10        // Number of clients
var T = "SET,GET" // Tests to run
var S = 1000      // Number of item in the random set
var CSV = false   // Output in CSV format
var P = 1         // Number or requests per transaction
var Mem bool      // Use only memory, no disk persistence
var I0 bool       // for ever loop
var Keys, Vals []string
var Path = "data.db"

func main() {
	defer func() {
		if Path != ":memory:" {
			os.RemoveAll(Path)
		}
	}()
	flag.IntVar(&N, "n", N, "Total number of requests")
	flag.IntVar(&C, "c", C, "Number of parallel goroutines")
	flag.StringVar(&T, "t", T, "Only run the comma separated list of tests")
	flag.IntVar(&S, "s", S, "Number of items in the random set")
	flag.BoolVar(&CSV, "csv", CSV, "Output in CSV format")
	flag.IntVar(&P, "P", P, "Number requests per transaction")
	flag.BoolVar(&Mem, "mem", Mem, "Use only memory, no disk persistence")
	flag.BoolVar(&I0, "I0", I0, "Forever loop")
	flag.Parse()
	if N < 1 || C < 1 || S < 1 || P < 1 {
		fmt.Printf("invalid arguments")
		os.Exit(1)
	}
	if Mem {
		Path = ":memory:"
	}
	rand.Seed(time.Now().UnixNano())
	for _, i := range rand.Perm(S) {
		Keys = append(Keys, fmt.Sprintf("key:%d", i))
		Vals = append(Vals, fmt.Sprintf("val:%d", i))
	}

	for {
		for _, test := range strings.Split(T, ",") {
			test = strings.TrimSpace(test)
			switch strings.ToLower(test) {
			case "set":
				SET()
			case "get":
				GET()
			case "test":
				TEST()
			}
		}
		if !I0 {
			break
		}
	}
}

func fatal(what interface{}) {
	panic(fmt.Sprintf("%v", what))
}

func bench(name string, count int, clients int, fn func(n int) error) {
	defer fmt.Printf("\n")

	var stats1 runtime.MemStats
	var stats2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats1)

	fmt.Printf("%s: ", name)
	var sl, el int
	sl = len(name) + 2
	erase := func(n int) {
		fmt.Printf("%s%s%s", strings.Repeat("\b", n), strings.Repeat(" ", n), strings.Repeat("\b", n))
	}

	var start time.Time
	var mu sync.Mutex
	var procd int
	var wg sync.WaitGroup
	run := func(n int) {
		defer wg.Done()
		for i := 0; i < n; {
			var err error
			remain := n - i
			nn := P
			if remain < P {
				nn = remain
			}
			err = fn(nn)
			if err != nil {
				fatal(err)
			}
			mu.Lock()
			procd += nn
			if procd%10000 == 0 {
				dd := fmt.Sprintf("%.2f", float64(procd)/(float64(time.Now().Sub(start))/float64(time.Second)))
				erase(el)
				el = len(dd)
				fmt.Printf("%s", dd)
			}
			mu.Unlock()
			i += nn
		}
	}

	var uclients int // the actual number of clients used, this is not printed
	remain := count
	start = time.Now()
	for rpc := count / clients; remain > 0; {
		n := rpc
		if remain < rpc {
			n = remain
			remain = 0
		} else {
			remain -= rpc
		}
		uclients++
		wg.Add(1)
		go run(n)
	}
	wg.Wait()
	total := time.Now().Sub(start)
	runtime.GC()
	runtime.ReadMemStats(&stats2)

	var heap uint64
	if stats2.HeapAlloc > stats1.HeapAlloc {
		heap = stats2.HeapAlloc - stats1.HeapAlloc
	}
	erase(sl + el)

	fmt.Printf("====== %s ======\n", name)
	fmt.Printf("  %d request completed in %.2f seconds\n", count, float64(total)/float64(time.Second))
	fmt.Printf("  %d item random data set\n", S)
	plural := "s"
	if clients == 1 {
		plural = ""
	}
	fmt.Printf("  %d parallel client%s\n", clients, plural)
	fmt.Printf("  %d bytes in heap\n", heap)
	fmt.Printf("\n")
	fmt.Printf("%.2f requests per second\n", float64(procd)/(float64(total)/float64(time.Second)))
}

func SET() {
	os.RemoveAll(Path)
	db, err := buntdb.Open(Path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	bench("SET", N, C, func(n int) error {
		return db.Update(func(tx *buntdb.Tx) error {
			for i := 0; i < n; i++ {
				idx := rand.Int() % len(Keys)
				_, _, err := tx.Set(Keys[idx], Vals[idx], nil)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func GET() {
	os.RemoveAll(Path)
	db, err := buntdb.Open(Path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < len(Keys); i++ {
			_, _, err := tx.Set(Keys[i], Vals[i], nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		fatal(err)
	}
	bench("GET", N, C, func(n int) error {
		return db.View(func(tx *buntdb.Tx) error {
			for i := 0; i < n; i++ {
				idx := rand.Int() % len(Keys)
				val, err := tx.Get(Keys[idx])
				if err != nil {
					return err
				}
				if val != Vals[idx] {
					return fmt.Errorf("values mismatch '%v' != '%v'", val, Vals[idx])
				}
			}
			return nil
		})
	})
}
func TEST() {
	bench("TEST", N, C, func(n int) error {
		abcd := make([]byte, S)
		abcd = abcd
		return nil
	})
}

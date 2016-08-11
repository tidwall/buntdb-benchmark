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

var N = 100000  // Number of requests
var R = 10      // Number of routines
var T = ""      // Tests to run
var S = 1000    // Number of item in the random set
var CSV = false // Output in CSV format
var Q = false   // Quiet. Just show query/sec values
var P = 1       // Number or requests per transaction
var Mem bool    // Use only memory, no disk persistence
var Forever = 1 // number of times to re-run the tests
var Keys, Vals, ValsLL []string
var Path = "data.db"

func main() {
	defer func() {
		if Path != ":memory:" {
			os.RemoveAll(Path)
		}
	}()
	var cas string
	flag.StringVar(&cas, "case", "", "Specify a unique test case")
	flag.IntVar(&N, "n", N, "Number of operations per test")
	flag.IntVar(&R, "r", R, "Number of parallel goroutines")
	flag.StringVar(&T, "t", T, "Only run the comma separated list of tests")
	flag.IntVar(&S, "s", S, "Number of items in the random set")
	flag.BoolVar(&CSV, "csv", CSV, "Output in CSV format")
	flag.IntVar(&P, "P", P, "Number requests per transaction")
	flag.BoolVar(&Q, "q", Q, "Quiet. Just show query/sec values")
	flag.BoolVar(&Mem, "mem", Mem, "Use only memory, no disk persistence")
	flag.IntVar(&Forever, "N", Forever, "Number of times to re-run the tests. -1 = forever")
	flag.Parse()
	if N < 1 || R < 1 || S < 1 || P < 1 || S > 10000000 {
		fmt.Printf("invalid arguments")
		os.Exit(1)
	}
	if cas != "" {
		executeSpecialCase(cas)
		return
	}
	if Mem {
		Path = ":memory:"
	}
	rand.Seed(time.Now().UnixNano())
	for _, i := range rand.Perm(S) {
		Keys = append(Keys, fmt.Sprintf("key:%010d", i))
		Vals = append(Vals, fmt.Sprintf("%010d", i))
		ValsLL = append(ValsLL, fmt.Sprintf("[%f %f]", rand.Float64()*360-180, rand.Float64()*180-90))
	}

	if Forever < 0 {
		Forever = 0xFFFFFFF
	}
	if T == "" {
		T = "GET,SET,ASCEND,DESCEND,SPATIAL"
	}
	for i := 0; i < Forever; i++ {
		for _, test := range strings.Split(T, ",") {
			test = strings.TrimSpace(test)
			switch strings.ToLower(test) {
			case "set":
				SET()
			case "get":
				GET()
			case "ascend":
				for i := 100; i <= 800; i *= 2 {
					ASCEND(i)
				}
			case "ascend_100":
				ASCEND(100)
			case "ascend_200":
				ASCEND(200)
			case "ascend_400":
				ASCEND(400)
			case "ascend_800":
				ASCEND(800)
			case "descend":
				for i := 100; i <= 800; i *= 2 {
					DESCEND(i)
				}
			case "descend_100":
				DESCEND(100)
			case "descend_200":
				DESCEND(200)
			case "descend_400":
				DESCEND(400)
			case "descend_800":
				DESCEND(800)
			case "spatial":
				SPATIAL_SET() // GEO
				for i := 100; i <= 800; i *= 2 {
					SPATIAL_INTERSECTS(i)
				}
			case "spatial_set":
				SPATIAL_SET() // GEO
			case "spatial_intersects":
				for i := 100; i <= 800; i *= 2 {
					SPATIAL_INTERSECTS(i)
				}
			case "spatial_intersects_100":
				SPATIAL_INTERSECTS(100)
			case "spatial_intersects_200":
				SPATIAL_INTERSECTS(200)
			case "spatial_intersects_400":
				SPATIAL_INTERSECTS(400)
			case "spatial_intersects_800":
				SPATIAL_INTERSECTS(800)
			}
		}
	}
}

func fatal(what interface{}) {
	panic(fmt.Sprintf("%v", what))
}

func bench(name string, count int, routines int, fn func(n int) error) {
	if !CSV && !Q {
		defer fmt.Printf("\n")
	}
	var stats1 runtime.MemStats
	var stats2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats1)
	var sl, el int
	if !CSV && !Q {
		fmt.Printf("%s: ", name)
		sl = len(name) + 2
	}
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
			if !CSV && !Q {
				if procd%10000 == 0 {
					dd := fmt.Sprintf("%.2f", float64(procd)/(float64(time.Now().Sub(start))/float64(time.Second)))
					erase(el)
					el = len(dd)
					fmt.Printf("%s", dd)
				}
			}
			mu.Unlock()
			i += nn
		}
	}

	var uclients int // the actual number of routines used, this is not printed
	remain := count
	start = time.Now()
	for rpc := count / routines; remain > 0; {
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
	switch {
	default:
		erase(sl + el)
		fmt.Printf("====== %s ======\n", name)
		plural := "s"
		if count == 1 {
			plural = ""
		}
		fmt.Printf("  %d operation%s completed in %.2f seconds\n", count, plural, float64(total)/float64(time.Second))
		fmt.Printf("  %d item random data set\n", S)
		plural = "s"
		if routines == 1 {
			plural = ""
		}
		fmt.Printf("  %d parallel goroutine%s\n", routines, plural)
		fmt.Printf("  heap usage: %d bytes\n", heap)
		fmt.Printf("\n")
		fmt.Printf("%.2f operations per second\n", float64(procd)/(float64(total)/float64(time.Second)))
	case CSV:
		fmt.Printf("\"%s\",\"%.2f\"\n", strings.Split(name, " ")[0], float64(procd)/(float64(total)/float64(time.Second)))
	case Q:
		fmt.Printf("%s: %.2f operations per second\n", strings.Split(name, " ")[0], float64(procd)/(float64(total)/float64(time.Second)))
	}
}

func SET() {
	os.RemoveAll(Path)
	db, err := buntdb.Open(Path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	bench("SET", N, R, func(n int) error {
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

func dbFill(spatial bool) (*buntdb.DB, error) {
	os.RemoveAll(Path)
	db, err := buntdb.Open(Path)
	if err != nil {
		return nil, err
	}
	if spatial {
		err = db.CreateSpatialIndex("spatial", "*", buntdb.IndexRect)
		if err != nil {
			return nil, err
		}
	}
	if err := db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < len(Keys); i++ {
			var err error
			if !spatial {
				_, _, err = tx.Set(Keys[i], Vals[i], nil)
			} else {
				_, _, err = tx.Set(Keys[i], ValsLL[i], nil)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func GET() {
	db, err := dbFill(false)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	bench("GET", N, R, func(n int) error {
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

func ASCEND(n int) {
	db, err := dbFill(false)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	keys := make([]string, 0)
	for i := 0; i < n; i++ {
		keys = append(keys, fmt.Sprintf("key:%010d", i))
	}
	max := n
	bench(fmt.Sprintf("ASCEND_%d (first %d items)", n, n), N, R, func(n int) error {
		return db.View(func(tx *buntdb.Tx) error {
			for i := 0; i < n; i++ {
				idx := 0
				var ferr error
				err := tx.Ascend("", func(key, val string) bool {
					if idx == max {
						return false
					}
					if key != keys[idx] {
						ferr = fmt.Errorf("keys mismatch '%v' != '%v'", key, keys[idx])
						return false
					}
					idx++
					return true
				})
				if err != nil {
					return err
				}
				if ferr != nil {
					return ferr
				}
			}
			return nil
		})
	})
}

func DESCEND(n int) {
	db, err := dbFill(false)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	var l int
	err = db.View(func(tx *buntdb.Tx) error {
		var err error
		l, err = tx.Len()
		return err
	})
	if err != nil {
		fatal(err)
	}
	keys := make([]string, 0)
	for i := 0; i < n; i++ {
		keys = append(keys, fmt.Sprintf("key:%010d", l-i-1))
	}
	max := n
	bench(fmt.Sprintf("DESCEND_%d (last %d items)", n, n), N, R, func(n int) error {
		return db.View(func(tx *buntdb.Tx) error {
			for i := 0; i < n; i++ {
				idx := 0
				var ferr error
				err := tx.Descend("", func(key, val string) bool {
					if idx == max {
						return false
					}
					if key != keys[idx] {
						ferr = fmt.Errorf("keys mismatch '%v' != '%v'", key, keys[idx])
						return false
					}
					idx++
					return true
				})
				if err != nil {
					return err
				}
				if ferr != nil {
					return ferr
				}
			}
			return nil
		})
	})
}
func SPATIAL_INTERSECTS(n int) {
	db, err := dbFill(true)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	nn := n
	bench(fmt.Sprintf("SPATIAL_INTERSECTS_%d (first %d points)", n, n), N, R, func(n int) error {
		count := 0
		err := db.View(func(tx *buntdb.Tx) error {
			return tx.Intersects("spatial", "[-180 -90],[180 90]", func(key, val string) bool {
				if count == nn {
					return false
				}
				count++
				return true
			})
		})
		if err != nil {
			return err
		}
		return nil
	})
}
func SPATIAL_SET() {
	os.RemoveAll(Path)
	db, err := buntdb.Open(Path)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	err = db.CreateSpatialIndex("spatial", "*", buntdb.IndexRect)
	if err != nil {
		fatal(err)
	}
	bench(fmt.Sprintf("SPATIAL_SET"), N, R, func(n int) error {
		return db.Update(func(tx *buntdb.Tx) error {
			for i := 0; i < n; i++ {
				idx := rand.Int() % len(Keys)
				_, _, err := tx.Set(Keys[idx], ValsLL[idx], nil)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
}

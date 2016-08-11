package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/buntdb"
)

func executeSpecialCase(c string) {
	switch strings.ToLower(c) {
	default:
		fmt.Printf("case '%s' not found\n", c)
		os.Exit(1)
	case "10e6":
		case10e(int(10e6))
	case "10e5":
		case10e(int(10e5))
	case "10e4":
		case10e(int(10e4))
	case "10e3":
		case10e(int(10e3))
	}
}

const alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randString(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	for i, c := range b {
		b[i] = alnum[int(c)%len(alnum)]
	}
	return string(b)
}

// https://www.ssa.gov/oact/babynames/decades/century.html
var firsts = []string{
	"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Elizabeth", "William", "Linda", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Margaret", "Charles", "Sarah", "Christopher", "Karen", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Dorothy", "Donald", "Lisa", "Mark", "Sandra",
	"Paul", "Ashley", "Steven", "Kimberly", "George", "Donna", "Kenneth", "Carol", "Andrew", "Michelle", "Joshua", "Emily", "Edward", "Helen", "Brian", "Amanda", "Kevin", "Melissa", "Ronald", "Deborah", "Timothy", "Stephanie", "Jason", "Laura", "Jeffrey", "Rebecca", "Ryan", "Sharon", "Gary", "Cynthia", "Jacob", "Kathleen", "Nicholas",
	"Shirley", "Eric", "Amy", "Stephen", "Anna", "Jonathan", "Angela", "Larry", "Ruth", "Scott", "Brenda", "Frank", "Pamela", "Justin", "Virginia", "Brandon", "Katherine", "Raymond", "Nicole", "Gregory", "Catherine", "Samuel", "Christine", "Benjamin", "Samantha", "Patrick", "Debra", "Jack", "Janet", "Alexander", "Carolyn", "Dennis",
	"Rachel", "Jerry", "Heather", "Tyler", "Maria", "Aaron", "Diane", "Henry", "Emma", "Douglas", "Julie", "Peter", "Joyce", "Jose", "Frances", "Adam", "Evelyn", "Zachary", "Joan", "Walter", "Christina", "Nathan", "Kelly", "Harold", "Martha", "Kyle", "Lauren", "Carl", "Victoria", "Arthur", "Judith", "Gerald", "Cheryl", "Roger", "Megan",
	"Keith", "Alice", "Jeremy", "Ann", "Lawrence", "Jean", "Terry", "Doris", "Sean", "Andrea", "Albert", "Marie", "Joe", "Kathryn", "Christian", "Jacqueline", "Austin", "Gloria", "Willie", "Teresa", "Jesse", "Hannah", "Ethan", "Sara", "Billy", "Janice", "Bruce", "Julia", "Bryan", "Olivia", "Ralph", "Grace", "Roy", "Rose", "Jordan",
	"Theresa", "Eugene", "Judy", "Wayne", "Beverly", "Louis", "Denise", "Dylan", "Marilyn", "Alan", "Amber", "Juan", "Danielle", "Noah", "Brittany", "Russell", "Madison", "Harry", "Diana", "Randy", "Jane", "Philip", "Lori", "Vincent", "Mildred", "Gabriel", "Tiffany", "Bobby", "Natalie", "Johnny", "Abigail", "Howard", "Kathy",
}

// https://en.wikipedia.org/wiki/List_of_most_common_surnames_in_North_America
var lasts = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson", "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White", "Lopez", "Lee",
	"Gonzalez", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Perez", "Hall", "Young", "Allen", "Sanchez", "Wright", "King", "Scott", "Green", "Baker", "Adams", "Nelson", "Hill", "Ramirez", "Campbell", "Mitchell", "Roberts",
	"Carter", "Phillips", "Evans", "Turner", "Torres", "Parker", "Collins", "Edwards", "Stewart", "Flores", "Morris", "Nguyen", "Murphy", "Rivera", "Cook", "Rogers", "Morgan", "Peterson", "Cooper", "Reed", "Bailey", "Bell",
	"Gomez", "Kelly", "Howard", "Ward", "Cox", "Diaz", "Richardson", "Wood", "Watson", "Brooks", "Bennett", "Gray", "James", "Reyes", "Cruz", "Hughes", "Price", "Myers", "Long", "Foster", "Sanders", "Ross", "Morales", "Powell",
	"Sullivan", "Russell", "Ortiz", "Jenkins", "Gutierrez", "Perry", "Butler", "Barnes", "Fisher",
}

func randJSON(buf *bytes.Buffer) string {
	buf.Reset()
	buf.WriteString(`{"name":{"first":"`)
	buf.WriteString(firsts[rand.Int()%len(firsts)])
	buf.WriteString(`","last":"`)
	buf.WriteString(lasts[rand.Int()%len(lasts)])
	buf.WriteString(`"},"age":`)
	buf.WriteString(strconv.FormatUint(uint64((rand.Int()%70)+15), 10))
	buf.WriteString(`"student":`)
	if rand.Int()%2 == 0 {
		buf.WriteString("true")
	} else {
		buf.WriteString("false")
	}
	buf.WriteString(`}`)
	return string(append([]byte{}, buf.Bytes()...))
}

func case10e(n int) {
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("building database with %d keys and random json\n", n)
	defer fmt.Printf("\n")
	os.RemoveAll("data.db")
	defer os.RemoveAll("data.db")

	var start time.Time
	var secs float64

	fmt.Printf("- generating keys and vals              ")
	start = time.Now()
	keys := make([]string, 0, n)
	vals := make([]string, 0, n)
	buf := &bytes.Buffer{}
	for i := range rand.Perm(n) {
		keys = append(keys, fmt.Sprintf("key:%20d", i))
		vals = append(vals, randJSON(buf))
	}
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

	//
	fmt.Printf("- inserting into database (batch 100)   ")
	start = time.Now()
	db, err := buntdb.Open("data.db")
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	for i := 0; i < n; {
		if err := db.Update(func(tx *buntdb.Tx) error {
			for j := 0; j < 1000 && i < n; j, i = j+1, i+1 {
				_, _, err := tx.Set(keys[i], vals[i], nil)
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			fatal(err)
		}
	}
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

	if err := db.Close(); err != nil {
		fatal(err)
	}

	fi, err := os.Stat("data.db")
	if err != nil {
		fatal(err)
	}

	fmt.Printf("- loading database from disk (%4.2fGB)   ", float64(fi.Size())/1024.0/1024.0/1024.0)
	start = time.Now()
	db, err = buntdb.Open("data.db")
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

	fmt.Printf("- create index IndexString              ")
	start = time.Now()
	if err := db.CreateIndex("strings", "*", buntdb.IndexString); err != nil {
		fatal(err)
	}
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

	fmt.Printf("- create index IndexBinary              ")
	start = time.Now()
	if err := db.CreateIndex("binary", "*", buntdb.IndexBinary); err != nil {
		fatal(err)
	}
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

	fmt.Printf(`- create index IndexJSON("name.last")   `)
	start = time.Now()
	if err := db.CreateIndex("json", "*", buntdb.IndexJSON("name.last")); err != nil {
		fatal(err)
	}
	secs = float64(time.Now().Sub(start)) / float64(time.Second)
	fmt.Printf(" %0.2f secs %0.0f ops/sec\n", secs, float64(n)/secs)

}

package stackadler32

import (
	"fmt"
	"hash/adler32"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestChecksum(t *testing.T) {
	for i := 0; i < 10000; i++ {
		randBytes := generateRandomByte(i)
		if Checksum(randBytes) != adler32.Checksum(randBytes) {
			fmt.Println(len(randBytes))
			t.FailNow()
		}
	}
}

var cases = []string{
	"",
	"a",
	"ab",
	"abc",
	"abcd",
	"abcde",
	"abcdef",
	"abcdefg",
	"abcdefgh",
	"abcdefghi",
	"abcdefghij",
	"Discard medicine more than two years old.",
	"He who has a shady past knows that nice guys finish last.",
	"I wouldn't marry him with a ten foot pole.",
	"Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave",
	"The days of the digital watch are numbered.  -Tom Stoppard",
	"Nepal premier won't resign.",
	"For every action there is an equal and opposite government program.",
	"His money is twice tainted: 'taint yours and 'taint mine.",
	"There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977",
	"It's a tiny change to the code and not completely disgusting. - Bob Manchek",
	"size:  a.out:  bad magic",
	"The major problem is with sendmail.  -Mark Horton",
	"Give me a rock, paper and scissors and I will move the world.  CCFestoon",
	"If the enemy is within range, then so are you.",
	"It's well we cannot hear the screams/That we create in others' dreams.",
	"You remind me of a TV show, but that's all right: I watch it anyway.",
	"C is as portable as Stonehedge!!",
	"Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley",
	"The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule",
	"How can you write a big system without C++?  -Paul Glick",
	"'Invariant assertions' is the most elegant programming technique!  -Tom Szymanski",
	strings.Repeat("\xff", 5548) + "8",
	strings.Repeat("\xff", 5549) + "9",
	strings.Repeat("\xff", 5550) + "0",
	strings.Repeat("\xff", 5551) + "1",
	strings.Repeat("\xff", 5552) + "2",
	strings.Repeat("\xff", 5553) + "3",
	strings.Repeat("\xff", 5554) + "4",
	strings.Repeat("\xff", 5555) + "5",
	strings.Repeat("\x00", 1e5),
	strings.Repeat("a", 1e5),
	strings.Repeat("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 1e4),
}

func TestCases(t *testing.T) {
	for _, c := range cases {
		if Checksum([]byte(c)) != adler32.Checksum([]byte(c)) {
			fmt.Println("fail case: ", c)
			t.FailNow()
		}
	}
}

func TestUpdate(t *testing.T) {
	for _, c := range cases {
		if len(c) > 1024 {
			// Skip the very large tests, we are just ensuring that continuinity
			// between update calls is successive - the long tests run for minutes
			// considering how many small chunks they can be split up into.
			continue
		}

		for i := 1; i < len(c); i++ {
			data := []byte(c)
			digest := Digest{}.Update(data[:i])
			data = data[i:]
			for len(data) > 0 {
				size := i
				if size > len(data) {
					size = len(data)
				}
				digest = digest.Update(data[:size])
				data = data[size:]
			}
			if digest.Sum32() != adler32.Checksum([]byte(c)) {
				fmt.Println("fail case: ", c)
				t.FailNow()
			}
		}
	}
}

func BenchmarkThis(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Checksum(generateRandomByte(n))
	}
}

func BenchmarkStdLib(b *testing.B) {
	for n := 0; n < b.N; n++ {
		adler32.Checksum(generateRandomByte(n))
	}
}

func generateRandomByte(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < len(b); i++ {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

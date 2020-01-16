package value

import (
	"fmt"
	"strconv"
	"testing"
)

func Test_ValueStore(t *testing.T) {
	vs := OpenStore("abc")
	location := vs.Put([]byte("key1"), []byte("value1"))
	location = vs.Put([]byte("key2"), []byte("value1"))
	location = vs.Put([]byte("key3"), []byte("value7"))
	vs.Put([]byte("key5"), []byte("value1"))
	fmt.Println(string(vs.Get(location)))

	locations := make([][]byte, 1000 * 100)

	for i:= 0; i < 1000 * 100; i++ {
		key := "key" + strconv.Itoa(i)
		value := make([]byte, 4096)
		copy(value, []byte(key))
		locations[i] = vs.Put([]byte(key), value)
	}

	for i:= 1000 * 100 - 45; i < 1000 * 100; i++ {
		v := vs.Get(locations[i])
		fmt.Println(string(v[:30]))
	}
}

func Test_Manifest(t *testing.T) {
	vs := OpenStore("abc")
	vs.CurrentFileNumber = 5
	vs.Size = 4
	vs.Sequence = 3
	vs.Offset = 788
	vs.Level[6].Start = 3
	vs.Level[63].End = 89
	vs.Level[12].Offset = 56
	vs.Close()

	vs = OpenStore("abc")
	fmt.Println(vs.CurrentFileNumber)
	fmt.Println(vs.Size)
	fmt.Println(vs.Offset)
	fmt.Println(vs.Sequence)
	fmt.Println(vs.Level[6].Start)
	fmt.Println(vs.Level[7].Start)
	fmt.Println(vs.Level[8].Start)
	fmt.Println(vs.Level[63].End)
	fmt.Println(vs.Level[12].Offset)
	vs.Close()
}


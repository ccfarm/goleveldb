package value

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ccfarm/fasterleveldb/dbutil"
	"github.com/ccfarm/goleveldb/leveldb"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	Capacity    = 16 * 1024 * 1024
	MANIFEST = "MANIFEST"
	LEVEL = 64
	MANIFESTSIZE = LEVEL * 3 * 4 + 20
	MAXSIZE = 16 * 1024 * 1024 * 1024
	WARNINGLINE = MAXSIZE / 10 * 8
	SAFELINE = MAXSIZE / 10 * 6
)

type Level struct {
	Start int
	End int
	Offset int
}

type Store struct {
	Path              string
	Size              int64
	CurrentFileNumber int
	CurrentFile       *os.File
	Offset            int
	Sequence          int
	Mutex             *sync.Mutex
	Level             []Level
	Compacting        bool
	KeyStore          *leveldb.DB
}

func OpenStore(valuePath string) *Store {
	manifestPath := path.Join(valuePath, MANIFEST)
	if dbutil.FileExists(manifestPath) {
		return loadStore(valuePath)
	} else {
		return newStore(valuePath)
	}
}

func loadStore(valuePath string) *Store {
	manifestPath := path.Join(valuePath, MANIFEST)
	manifestFile, _ := os.OpenFile(manifestPath, os.O_RDONLY, os.ModePerm)
	bufferSize := MANIFESTSIZE
	buffer := make([]byte, bufferSize)
	manifestFile.Read(buffer)
	sequence := int(binary.BigEndian.Uint32(buffer[0:]))
	size := binary.BigEndian.Uint64(buffer[4:])
	currentFileNumber := int(binary.BigEndian.Uint32(buffer[12:]))
	offset := int(binary.BigEndian.Uint32(buffer[16:]))
	vs := &Store{
		Path:              valuePath,
		Size:              int64(size),
		CurrentFileNumber: currentFileNumber,
		Offset:            offset,
		Sequence:          sequence,
		Mutex:             &sync.Mutex{},
		Level:make([]Level, LEVEL),
	}
	for i := 0; i < LEVEL; i++ {
		vs.Level[i].Start = int(binary.BigEndian.Uint32(buffer[20 + i * 12:]))
		vs.Level[i].End = int(binary.BigEndian.Uint32(buffer[20 + i * 12 + 4:]))
		vs.Level[i].Offset = int(binary.BigEndian.Uint32(buffer[20 + i * 12 + 8:]))
	}
	return vs
}

func newStore(valuePath string) *Store {
	vs := &Store{
		Path:              valuePath,
		Size:              1,
		CurrentFileNumber: 0,
		Offset:            0,
		Sequence:          0,
		Mutex:             &sync.Mutex{},
		Level:make([]Level, LEVEL),
	}
	os.MkdirAll(valuePath, os.ModePerm)
	filename := vs.generateFilename(0, vs.CurrentFileNumber)
	vs.CurrentFile, _ = os.OpenFile(filename, os.O_CREATE | os.O_RDWR, os.ModePerm)
	fmt.Println(filename)
	return vs
}

func (vs *Store)Put(key []byte, value []byte) (location []byte) {
	l := len(key) + len(value) + 16
	buffer := make([]byte, l)
	binary.BigEndian.PutUint32(buffer[0: ], uint32(l))
	binary.BigEndian.PutUint32(buffer[4: ], uint32(len(key)))
	binary.BigEndian.PutUint32(buffer[8: ], uint32(len(value)))
	copy(buffer[16: ], key)
	copy(buffer[len(key) + 16: ], value)

	vs.Mutex.Lock()
	binary.BigEndian.PutUint32(buffer[12: ], uint32(vs.Sequence))
	vs.CurrentFile.WriteAt(buffer, int64(vs.Offset))
	location = generateLocation(l, vs.CurrentFileNumber, vs.Offset, int(vs.Sequence), 0)
	vs.Offset += l
	vs.Sequence += 1
	if vs.Offset >= Capacity {
		vs.Offset = 0
		vs.CurrentFileNumber += 1
		vs.CurrentFile.Close()
		filename := vs.generateFilename(0, vs.CurrentFileNumber)
		vs.CurrentFile, _ = os.OpenFile(filename, os.O_CREATE | os.O_RDWR, os.ModePerm)
		vs.Level[0].End = vs.CurrentFileNumber
	}
	vs.Mutex.Unlock()

	size := atomic.AddInt64(&vs.Size, int64(l))
	if !vs.Compacting {
		if size > WARNINGLINE {
			vs.Compacting = true
			go vs.compact()
		}
	}

	return location
}

func (vs *Store)Get(location []byte) (value []byte) {
	length, fileNumber, offset, _ , level:= parseLocation(location)
	//fmt.Println(length, fileNumber, offset, seq, level)
	buffer := make([]byte, length)
	fileName := vs.generateFilename(level, fileNumber)
	file, _ := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	file.ReadAt(buffer, int64(offset))

	keySize := binary.BigEndian.Uint32(buffer[4:])
	valueSize := binary.BigEndian.Uint32(buffer[8:])
	//fmt.Println(keySize)
	//fmt.Println(valueSize)
	return buffer[16 + keySize: 16 + keySize + valueSize]
}

func generateLocation(length int, fileNumber int, offset int, seq int, level int) []byte{
	buffer := make([]byte, 20)
	binary.BigEndian.PutUint32(buffer[0:], uint32(length))
	binary.BigEndian.PutUint32(buffer[4:], uint32(fileNumber))
	binary.BigEndian.PutUint32(buffer[8:], uint32(offset))
	binary.BigEndian.PutUint32(buffer[12:], uint32(seq))
	binary.BigEndian.PutUint32(buffer[16:], uint32(level))
	return buffer
}

func parseLocation(location []byte) (length int, fileNumber int, offset int, seq int, level int) {
	length = int(binary.BigEndian.Uint32(location[0:]))
	fileNumber = int(binary.BigEndian.Uint32(location[4:]))
	offset = int(binary.BigEndian.Uint32(location[8:]))
	seq = int(binary.BigEndian.Uint32(location[12:]))
	level = int(binary.BigEndian.Uint32(location[16:]))
	return
}

func (vs *Store)generateFilename(level int, fileNumber int) string{
	filename := path.Join(vs.Path, "level_" + strconv.Itoa(level) + "_number_" + strconv.Itoa(fileNumber))
	return filename
}

func (vs *Store)Close() () {
	vs.CurrentFile.Close()
	manifestPath := path.Join(vs.Path, MANIFEST)
	manifestFile, _ := os.OpenFile(manifestPath, os.O_CREATE | os.O_RDWR, os.ModePerm)
	bufferSize := MANIFESTSIZE
	buffer := make([]byte, bufferSize)
	binary.BigEndian.PutUint32(buffer[0:], uint32(vs.Sequence))
	binary.BigEndian.PutUint64(buffer[4:], uint64(vs.Size))
	binary.BigEndian.PutUint32(buffer[12:], uint32(vs.CurrentFileNumber))
	binary.BigEndian.PutUint32(buffer[16:], uint32(vs.Offset))
	for i := 0; i < LEVEL; i++ {
		binary.BigEndian.PutUint32(buffer[20 + i * 12:], uint32(vs.Level[i].Start))
		binary.BigEndian.PutUint32(buffer[20 + i * 12 + 4:], uint32(vs.Level[i].End))
		binary.BigEndian.PutUint32(buffer[20 + i * 12 + 8:], uint32(vs.Level[i].Offset))
	}
	manifestFile.Write(buffer)
	manifestFile.Close()
	return
}

func (vs *Store)compact() {

	lengthBytes := make([]byte, 4)
	for {
		for i := 0; i < LEVEL - 1; i++ {
			//fmt.Printf("compacting level %d size %d\n", i, vs.Size)
			wFilename := vs.generateFilename(i + 1, vs.Level[i + 1].End)
			wFile, err := os.OpenFile(wFilename, os.O_CREATE | os.O_RDWR, os.ModePerm)
			if err != nil {
				fmt.Println(err)
			}
			for vs.Level[i].Start < vs.Level[i].End {
				rFilename := vs.generateFilename(i, vs.Level[i].Start)
				rFile, err := os.OpenFile(rFilename, os.O_RDWR, os.ModePerm)
				if err != nil {
					fmt.Println(err)
				}
				reader := bufio.NewReader(rFile)
				for {
					//n, e := reader.Read(lengthBytes)
					n, e := io.ReadFull(reader, lengthBytes)
					if n <= 0 || e != nil {
						break
					}
					length := binary.BigEndian.Uint32(lengthBytes)
					//fmt.Println(length)
					buffer := make([]byte, length)

					//reader.Read(buffer[4:])
					io.ReadFull(reader, buffer[4:])
					keySize := binary.BigEndian.Uint32(buffer[4:])
					//valueSize := binary.BigEndian.Uint32(buffer[8:])
					seq := binary.BigEndian.Uint32(buffer[12:])
					key := buffer[16: 16 + keySize]
					//fmt.Println(string(key))
					//value := buffer[16 + keySize: 16 + keySize + valueSize]
					//fmt.Println(string(key))
					//fmt.Println(string(value))
					location, err:= vs.KeyStore.Get(key, nil)
					if err != nil {
						vs.Mutex.Lock()
						vs.Size -= int64(length)
						vs.Mutex.Unlock()
						//atomic.AddInt64(&vs.Size, int64(-length))
					} else {
						_, _, _, seqOld, _ := parseLocation(location)
						if seqOld != int(seq) {
							vs.Mutex.Lock()
							vs.Size -= int64(length)
							vs.Mutex.Unlock()
							//atomic.AddInt64(&vs.Size, int64(-length))
						} else {
							binary.BigEndian.PutUint32(buffer[0:], uint32(length))
							binary.BigEndian.PutUint32(buffer[12:], uint32(seq))
							wFile.WriteAt(buffer, int64(vs.Level[i + 1].Offset))
							vs.Mutex.Lock()
							seq := vs.Sequence
							vs.Sequence += 1
							vs.Mutex.Unlock()
							location := generateLocation(int(length), vs.Level[i + 1].End, vs.Level[i + 1].Offset, seq, i + 1)
							//fmt.Println(length, vs.Level[i + 1].End, vs.Level[i + 1].Offset, seq, i + 1)
							vs.KeyStore.Put(key, location, nil)
							vs.Level[i + 1].Offset += int(length)
							if vs.Level[i + 1].Offset >= Capacity {
								vs.Level[i + 1].Offset = 0
								vs.Level[i + 1].End += 1
								wFile.Close()
								wFilename = vs.generateFilename(i + 1, vs.Level[i + 1].End)
								wFile, err = os.OpenFile(wFilename, os.O_CREATE | os.O_RDWR, os.ModePerm)
								if err != nil {
									fmt.Println(err)
								}
							}
						}
					}
				}

				rFile.Close()
				err = os.Remove(rFilename)
				if err != nil {
					fmt.Println(err)
				}
				vs.Level[i].Start += 1
				if vs.Size < SAFELINE {
					vs.Compacting = false
					return
				}
			}
			wFile.Close()
		}
	}

}

func (vs *Store)SetKeyStore(keyStore *leveldb.DB) {
	vs.KeyStore = keyStore
}
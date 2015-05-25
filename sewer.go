package main

import (
	"log"
    "sync"
	"reflect"
)

type T interface{}

func makeChannel(t reflect.Type, chanDir reflect.ChanDir, buffer int) reflect.Value {
	ctype := reflect.ChanOf(chanDir, t)
	return reflect.MakeChan(ctype, buffer)
}

type TChan chan T
type TOutChan <- chan T

func chanFromValues(data ...T) TOutChan {
	out := make(chan T)

	go func() {
        for _, d := range data {
            out <- d
        }
        close(out)
    }()

    return out
}

func chanFromSlice(array T) TOutChan {
	value := reflect.ValueOf(array)
	if value.Kind() != reflect.Slice {
		panic("Parameter must be a slice")
	}

	out := make(chan T)

	go func() {
		for i := 0; i != value.Len(); i++ {
			out <- value.Index(i).Interface()
		}
		close(out)
	}()

	return out
}

func chanFromInts(nums ...int) <-chan int {
	out := make(chan int)

	go func() {
        for _, n := range nums {
            out <- n
        }
        close(out)
    }()

    return out
}

func chanFromFloats(nums ...float64) <-chan float64 {
	out := make(chan float64)

	go func() {
        for _, n := range nums {
            out <- n
        }
        close(out)
    }()

    return out
}

func chanFromStrings(stringi ...string) <-chan string {
	out := make(chan string)

	go func() {
        for _, n := range stringi {
            out <- n
        }
        close(out)
    }()

    return out
}

type Tuple struct {
	Key interface{}
	Value interface{}
	Meta interface{}
}

func checkRunFunction(f T) (vf reflect.Value) {
	vf = reflect.ValueOf(f)
	ftype := vf.Type()

	if ftype.Kind() != reflect.Func {
		log.Panicf("`f` should be %s but got %s", reflect.Func, ftype.Kind())
	}
	if ftype.NumIn() != 1 {
		log.Panicf("`f` should have 1 parameter but it has %d parameters", ftype.NumIn())
	}
	if ftype.NumOut() != 1 {
		log.Panicf("`f` should return 1 value but it returns %d values", ftype.NumOut())
	}

	return
}

func checkRunTuple(xs TupleOutChan) {
	vxs := reflect.ValueOf(xs)
	xstype := vxs.Type()

	if xstype.Kind() != reflect.Chan {
		log.Panicf("`f` should be %s but got %s", reflect.Chan, xstype.Kind())
	}

	return
}

func runSyncAsyncTupleSingle(xs TupleOutChan, async bool, f T) TupleOutChan {
	vf := checkRunFunction(f)
	checkRunTuple(xs)

	out := make(chan Tuple)

	go func() {
		var wg sync.WaitGroup

		for tup := range xs {
			wg.Add(1)
			fx := func(tup Tuple) {
				valRes := vf.Call([]reflect.Value{ reflect.ValueOf(tup.Value) })[0].Interface()
				out <- NewTupleVal(tup, valRes)
				wg.Done()
			}
			if(async) {
				go fx(tup)
			} else {
				fx(tup)
			}
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func filterSyncAsyncTupleSingle(xs TupleOutChan, async bool, f T) TupleOutChan {
	vf := checkRunFunction(f)
	checkRunTuple(xs)

	out := make(chan Tuple)

	go func() {
		var wg sync.WaitGroup

		for tup := range xs {
			wg.Add(1)
			fx := func(tup Tuple) {
				valRes := vf.Call([]reflect.Value{ reflect.ValueOf(tup.Value) })[0].Interface()
				if valRes.(bool) {
					out <- tup
				}
				wg.Done()
			}
			if(async) {
				go fx(tup)
			} else {
				fx(tup)
			}
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func runSyncAsyncTuple(xs TupleOutChan, async bool, f ...T) TupleOutChan {
	var ch TupleOutChan
	ch = xs
	for m := range f {
		ch = runSyncAsyncTupleSingle(ch, async, f[m])
	}
	return ch
}

func filterSyncAsyncTuple(xs TupleOutChan, async bool, f ...T) TupleOutChan {
	var ch TupleOutChan
	ch = xs
	for m := range f {
		ch = filterSyncAsyncTupleSingle(ch, async, f[m])
	}
	return ch
}

func runSyncAsyncSingle(xs T, f T, async bool) TOutChan {
	vf := reflect.ValueOf(f)
	ftype := vf.Type()
	if ftype.Kind() != reflect.Func {
		log.Panicf("`f` should be %s but got %s", reflect.Func, ftype.Kind())
	}
	if ftype.NumIn() != 1 {
		log.Panicf("`f` should have 1 parameter but it has %d parameters", ftype.NumIn())
	}
	if ftype.NumOut() != 1 {
		log.Panicf("`f` should return 1 value but it returns %d values", ftype.NumOut())
	}

	vxs := reflect.ValueOf(xs)
	xstype := vxs.Type()

	if xstype.Kind() != reflect.Chan {
		log.Panicf("`f` should be %s but got %s", reflect.Chan, xstype.Kind())
	}

	// elIn := vf.Type().In(0)
	// // elOut := vf.Type().Out(0)

	// elemType := xstype.Elem()

	// if elemType != elIn {
	// 	log.Panicf("`f` parameter must be %s but got %s", elemType, elIn)
	// }

	out := make(chan T)

	go func() {
		var wg sync.WaitGroup

		for ok := true; ok; {
			var elem reflect.Value
			if elem, ok = vxs.Recv(); ok {
				wg.Add(1)
				fx := func(elem reflect.Value) {
					// rx := vf.Call([]reflect.Value{ elem })[0].Interface()
					// elem.Interface()
					out <- vf.Call([]reflect.Value{ reflect.ValueOf(elem.Interface()) })[0].Interface()
					wg.Done()
				}
				if(async) {
					go fx(elem)
				} else {
					fx(elem)
				}
			}
		}

		wg.Wait()
		close(out)
	}()

	return out
}

func runSyncAsync(xs T, async bool, f ...T) TOutChan {
	var ch interface{}
	ch = xs
	for m := range f {
		ch = runSyncAsyncSingle(ch, f[m], async)
	}
	return ch.(TOutChan)
}

type TupleChan chan Tuple
type TupleOutChan <- chan Tuple

func NewTuple(k interface{}, v interface{}) Tuple {
	return Tuple{k, v, nil}
}

func NewTupleVal(t Tuple, v interface{}) Tuple {
	return Tuple{t.Key, v, t.Meta}
}

func MakeTupleChan(data ...T) TupleOutChan {
	out := make(chan Tuple)

	go func() {
		i := 0

		for _, d := range data {
			out <- NewTuple(i, d)
			i++
		}

		close(out)
	}()

	return out
}

func MakeTupleChanFromChan(data interface{}) TupleOutChan {
	out := make(chan Tuple)

	dataVal := reflect.ValueOf(data)
	if dataVal.Kind() != reflect.Chan {
		panic("data parameter must be a chan")
	}

	go func() {
		i := 0

		var elem reflect.Value
		for ok := true; ok; {
			if elem, ok = dataVal.Recv(); ok {
				out <- NewTuple(i, elem.Interface())
				i++
			}
		}

		close(out)
	}()

	return out
}

func MakeTupleChanFromSlice(array T) TupleOutChan {
	value := reflect.ValueOf(array)
	if value.Kind() != reflect.Slice {
		panic("Parameter must be a slice")
	}

	out := make(chan Tuple)

	go func() {
		for i := 0; i != value.Len(); i++ {
			out <- NewTuple(i, value.Index(i).Interface())
		}
		close(out)
	}()

	return out
}

func MakeTupleFloat64Chan(data ...float64) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleFloat32Chan(data ...float32) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleInt64Chan(data ...int64) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleInt32Chan(data ...int32) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleIntChan(data ...int) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleStringChan(data ...string) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func MakeTupleBoolChan(data ...bool) TupleOutChan {
	return MakeTupleChanFromSlice(data)
}

func (ch TupleOutChan) Apply(f T) TupleOutChan {
	return runSyncAsyncTuple(ch, true, f)
}

func (ch TupleOutChan) ApplyAll(f ...T) TupleOutChan {
	return runSyncAsyncTuple(ch, true, f...)
}

func (ch TupleOutChan) Filter(f T) TupleOutChan {
	return filterSyncAsyncTuple(ch, true, f)
}

func (ch TupleOutChan) FilterAll(f ...T) TupleOutChan {
	return filterSyncAsyncTuple(ch, true, f...)
}

func ApplyStream(s StreamI) StreamI {
	out := make(chan Tuple)

	go func() {
		s.Run()
		close(out)
	}()

	<- out

	return s
}

func RunStreams(data ...StreamI) <- chan StreamI {
	dataStream := make(chan StreamI)
	out := make(chan StreamI)

	go func() {
        for _, d := range data {
            dataStream <- d
        }
        close(dataStream)
    }()

    go func() {
    	for s := range runSyncAsync(dataStream, true, ApplyStream) {
    		out <- s.(StreamI)
    	}
    	close(out)
    }()

    return out
}
 
func RunStreamsAndWait(data ...StreamI) {
	for _ = range RunStreams(data...) {}
}

type StreamFunction func ()
type StreamProcessFunction interface{}

type Stream struct {
	id T
	function StreamFunction
	in 	*TupleOutChan
	out TupleChan
}

func (s Stream) Id() interface{} {
	return s.id
}

func (s Stream) SetData(data *TupleOutChan) {
	s.in = data
}

func (s Stream) Items() <- chan Tuple {
	return s.out
}

func (s Stream) Run() {
	s.function()
}

func NewStream(id T, f StreamFunction) Stream {
	return Stream{ id, f, nil, make(TupleChan) }
}

type StreamProcess struct {
	Stream
	function StreamProcessFunction
}

func (s StreamProcess) Run() {
	for x := range runSyncAsyncTuple(*s.in, true, s.function) {
		s.out <- x
	}
}

func NewStreamProcess(id T, data TupleOutChan, f StreamProcessFunction) StreamProcess {
	s := StreamProcess{}
	s.id = id
	s.in = &data
	s.out = make(TupleChan)
	s.function = f
	return s
}

type StreamReaderFunction func(t Tuple)

type StreamReader struct {
	stream StreamI
	function StreamReaderFunction
}

func (sr StreamReader) ReadStream(s StreamI) {
	for t := range s.Items() {
		sr.OnRead(t)
	}
}

func (sr StreamReader) OnRead(t Tuple) {
	sr.function(t)
}

func NewStreamReader(f StreamReaderFunction) StreamReader {
	return StreamReader{nil, f}
}

func ReadStream(s StreamI, f StreamReaderFunction) {
	NewStreamReader(f).ReadStream(s)
}

type StreamReaderI interface {
	ReadStream(s StreamI)
	OnRead(t Tuple)
}

type StreamI interface {
	Id() interface{}
	Items() <- chan Tuple
	Run()
}
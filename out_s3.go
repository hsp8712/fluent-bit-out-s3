package main

import (
	"C"
	"fmt"
	"log"
	"strconv"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

type Params struct {
	bucket         string
	region         string
	multipart_size int
	s3_key_format  string
}

var parameters *Params

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "out_s3", "Out S3 GO!")
}

//export FLBPluginInit
// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	bucket := output.FLBPluginConfigKey(plugin, "bucket")
	region := output.FLBPluginConfigKey(plugin, "region")
	multipartSize := output.FLBPluginConfigKey(plugin, "multipart_size")
	s3KeyFormat := output.FLBPluginConfigKey(plugin, "s3_key_format")

	parameters.bucket = bucket
	parameters.region = region
	multipartSizeInt, err := strconv.Atoi(multipartSize)
	if err != nil {
		log.Printf("[out_s3] multipart_size invalid")
		return output.FLB_ERROR
	}
	parameters.multipart_size = multipartSizeInt
	parameters.s3_key_format = s3KeyFormat
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	var count int
	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	count = 0
	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			fmt.Println("time provided invalid, defaulting to now.")
			timestamp = time.Now()
		}

		// Print record keys and values
		fmt.Printf("[%d] %s: [%s, {", count, C.GoString(tag),
			timestamp.String())
		for k, v := range record {
			fmt.Printf("\"%s\": %v, ", k, v)
		}
		fmt.Printf("}\n")
		count++
	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}

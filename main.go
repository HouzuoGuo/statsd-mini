package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jbuchbinder/go-gmetric/gmetric"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	TCP = "tcp"
	UDP = "udp"
)

type Packet struct {
	Bucket   string
	Value    string
	Modifier string
	Sampling float32
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	gangliaAddress   = flag.String("ganglia", "", "Ganglia gmond servers, comma separated")
	gangliaPort      = flag.Int("ganglia-port", 8649, "Ganglia gmond service port")
	gangliaSpoofHost = flag.String("ganglia-spoof-host", "", "Ganglia gmond spoof host string")
	gangliaGroup     = flag.String("ganglia-group", "statsd", "Ganglia metric group name")
	calculationInterval    = flag.Int64("calculation-interval", 10, "Calculation interval")
	debug            = flag.Bool("debug", false, "Debug mode")
)

var (
	In       = make(chan Packet, 10000)
	counters = make(map[string]int)
	timers   = make(map[string][]float64)
)

func monitor() {
	var err error
	if err != nil {
		log.Println(err)
	}
	t := time.NewTicker(time.Duration(*calculationInterval) * time.Second)
	for {
		select {
		case <-t.C:
			submit()
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t []float64
					timers[s.Bucket] = t
				}
				//intValue, _ := strconv.Atoi(s.Value)
				floatValue, _ := strconv.ParseFloat(s.Value, 64)
				timers[s.Bucket] = append(timers[s.Bucket], floatValue)
			} else if s.Modifier == "g" {
				log.Println("Guage data is ignored")
			} else {
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				floatValue, _ := strconv.ParseFloat(s.Value, 32)
				counters[s.Bucket] += int(float32(floatValue) * (1 / s.Sampling))
			}
		}
	}
}

func submit() {
	var useGanglia bool
	var gm gmetric.Gmetric
	gmSubmitFloat := func(name, unit string, value float64) {
		if useGanglia {
			if *debug {
				log.Println("Ganglia send float metric %s value %f\n", name, value)
			}
			go gm.SendMetric(name, fmt.Sprint(value), uint32(gmetric.VALUE_DOUBLE), unit, uint32(gmetric.SLOPE_BOTH), uint32(14400), uint32(14400), *gangliaGroup)
		}
	}
	if *gangliaAddress != "" {
		gm = gmetric.Gmetric{
			Host:  *gangliaSpoofHost,
			Spoof: *gangliaSpoofHost,
		}
		gm.SetVerbose(false)

		if strings.Contains(*gangliaAddress, ",") {
			segs := strings.Split(*gangliaAddress, ",")
			for i := 0; i < len(segs); i++ {
				gIP, err := net.ResolveIPAddr("ip4", segs[i])
				if err != nil {
					panic(err.Error())
				}
				gm.AddServer(gmetric.GmetricServer{gIP.IP, *gangliaPort})
			}
		} else {
			gIP, err := net.ResolveIPAddr("ip4", *gangliaAddress)
			if err != nil {
				panic(err.Error())
			}
			gm.AddServer(gmetric.GmetricServer{gIP.IP, *gangliaPort})
		}
		useGanglia = true
	} else {
		useGanglia = false
	}
	// Submit all counter data to Ganglia
	for name, value := range counters {
		averageOverInterval := float64(value) / float64(*calculationInterval)
		gmSubmitFloat(fmt.Sprintf("count_%s", name), "count", averageOverInterval)
		counters[name] = 0
	}
	// Calculate timer average and send to Ganglia
	for name, timerValues := range timers {
		if len(timerValues) == 0 {
			// Submit timer data 0 (no data)
			gmSubmitFloat(fmt.Sprintf("avg_%s", name), "ms", 0)
			continue
		}
		// Calculate min/average/max response time
		var sum float64
		for i := 0; i < len(timerValues); i++ {
			sum += timerValues[i]
		}
		avg := sum / float64(len(timerValues))
		// Send timer average to Ganglia
		gmSubmitFloat(fmt.Sprintf("avg_%s", name), "ms", avg)
	}
}

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var value string
	var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
	var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_\\.]+):(\\-?[0-9\\.]+)\\|(c|ms|g)(\\|@([0-9\\.]+))?")
	s := sanitizeRegexp.ReplaceAllString(buf.String(), "")
	for _, item := range packetRegexp.FindAllStringSubmatch(s, -1) {
		value = item[2]
		if item[3] == "ms" {
			_, err := strconv.ParseFloat(item[2], 32)
			if err != nil {
				value = "0"
			}
		}

		sampleRate, err := strconv.ParseFloat(item[5], 32)
		if err != nil {
			sampleRate = 1
		}

		packet.Bucket = item[1]
		packet.Value = value
		packet.Modifier = item[3]
		packet.Sampling = float32(sampleRate)

		if *debug {
			log.Println(
				fmt.Sprintf("Packet: bucket = %s, value = %s, modifier = %s, sampling = %f\n",
					packet.Bucket, packet.Value, packet.Modifier, packet.Sampling))
		}

		In <- packet
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr(UDP, *serviceAddress)
	listener, err := net.ListenUDP(UDP, address)
	defer listener.Close()
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}
	for {
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		if *debug {
			log.Println("Packet received: " + string(message[0:n]) + "\n")
		}
		go handleMessage(listener, remaddr, buf)
	}
}

func main() {
	flag.Parse()
	go udpListener()
	monitor()
}

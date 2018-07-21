package main

import (
	"flag"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/j-vizcaino/goteleinfo"
	log "github.com/sirupsen/logrus"
)

const (
	BufferedFramesCount = 100
)

func ensureFrameReader(serialDevice string) teleinfo.Reader {
	port, err := teleinfo.OpenPort(serialDevice)
	if err != nil {
		log.WithFields(log.Fields{"device": serialDevice, "error": err}).Fatal("Cannot open teleinfo port")
	}

	return teleinfo.NewReader(port)
}

func readFrames(reader teleinfo.Reader, framesChan chan teleinfo.Frame) {
	for {
		f, err := reader.ReadFrame()
		if err != nil {
			log.WithField("error", err).Warn("Teleinfo frame read failed")
		} else {
			framesChan <- f
		}
	}
}

func ensureStatsdClient(metricsNamespace string) *statsd.Client {
	// TODO: provide a way to change this
	url := "localhost:8125"
	dsd, err := statsd.New(url)
	if err != nil {
		log.WithFields(log.Fields{"statsd_url": url, "error": err}).Fatal("statsd client setup failed")
	}

	dsd.Namespace = metricsNamespace
	return dsd
}

func buildFrameTags(f teleinfo.Frame) []string {
	if f.Type() != "HC.." {
		log.WithField("frame_type", f.Type()).Warn("Skipped unsupported teleinfo frame")
		return nil
	}
	rate, _ := f.GetStringField("PTEC")
	var rateTag string
	switch rate {
	case "HP..":
		rateTag = "heures pleines"
	case "HC..":
		rateTag = "heures creuses"
	default:
		rateTag = "unknown"
	}

	return []string{"price:" + rateTag}
}

func exportFrames(dsd *statsd.Client, framesChan chan teleinfo.Frame) {
	counters := map[string]string{
		"HCHC": "counter_low_price_kwh",
		"HCHP": "counter_full_price_kwh",
	}
	gauges := map[string]string{
		"PAPP":  "power_va",
		"IINST": "current_amps",
	}

	for f := range framesChan {
		tags := buildFrameTags(f)
		if tags == nil {
			continue
		}

		for key, metric := range counters {
			value, ok := f.GetUIntField(key)
			if !ok {
				s, _ := f.GetStringField(key)
				log.WithFields(log.Fields{"field_name": key, "field_value": s, "metric": metric}).Warn("Cannot extract metric value from frame")
				continue
			}
			log.WithFields(log.Fields{"value": value, "metric": metric, "namespace": dsd.Namespace, "tags": tags}).Debug("Sending dogstatsd counter")
			dsd.Count(metric, int64(value), tags, 1)
		}

		for key, metric := range gauges {
			value, ok := f.GetUIntField(key)
			if !ok {
				s, _ := f.GetStringField(key)
				log.WithFields(log.Fields{"field_name": key, "field_value": s, "metric": metric}).Warn("Cannot extract metric value from frame")
				continue
			}
			log.WithFields(log.Fields{"value": value, "metric": metric, "namespace": dsd.Namespace, "tags": tags}).Debug("Sending dogstatsd gauge")
			dsd.Gauge(metric, float64(value), tags, 1)
		}
	}
}

func main() {
	var serialDevice string
	var logDebug bool
	var metricsNamespace string
	flag.StringVar(&serialDevice, "device", "/dev/ttyUSB0", "Serial port to read frames from")
	flag.BoolVar(&logDebug, "debug", false, "Enable debug log messages")
	flag.StringVar(&metricsNamespace, "metrics-namespace", "electrical_energy.", "Namespace for statsd metrics")
	flag.Parse()

	if logDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.Info("Starting dd-teleinfo...")

	reader := ensureFrameReader(serialDevice)
	dsd := ensureStatsdClient(metricsNamespace)

	framesChan := make(chan teleinfo.Frame, BufferedFramesCount)
	go readFrames(reader, framesChan)

	exportFrames(dsd, framesChan)
}

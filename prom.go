package main

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	filesCopied = promauto.NewCounter(prometheus.CounterOpts{
		Name: "photoimportd_files_copied",
		Help: "The total number of files copied to the dstPath",
	})

	filesScanned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "photoimportd_files_scanned",
		Help: "The total number of files scanned by the storageWorker",
	})
)

func exitProgram(w http.ResponseWriter, r *http.Request) {
	os.Exit(0)
}

func prometheusMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/exit", exitProgram)
	http.ListenAndServe(":2112", nil)
}
